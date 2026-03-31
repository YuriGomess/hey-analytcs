import json
import logging
import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request

from app.dashboard.router import router as dashboard_router
from app.database import close_db_pool, get_db_cursor, init_db
from app.integrations.monday import MondayClient
from app.integrations.typeform import parse_response, validate_signature
from app.scheduler import start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield
    stop_scheduler()
    close_db_pool()


app = FastAPI(lifespan=lifespan, title="Hey Analytics")
app.include_router(dashboard_router)


@app.post("/webhook/typeform")
async def typeform_webhook(
    request: Request,
    typeform_signature: str | None = Header(default=None, alias="Typeform-Signature"),
):
    payload = await request.body()
    secret = os.getenv("TYPEFORM_SECRET", "")
    if not secret:
        raise HTTPException(status_code=500, detail="TYPEFORM_SECRET not configured")

    if not validate_signature(payload, typeform_signature or "", secret):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        body = json.loads(payload.decode("utf-8"))
        parsed = parse_response(body)

        campaign_id = None
        utm_campaign = parsed.get("utm_campaign")

        with get_db_cursor() as (_, cur):
            if utm_campaign:
                cur.execute(
                    """
                    SELECT campaign_id
                    FROM campaigns
                    WHERE campaign_name ILIKE %s OR campaign_id = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (utm_campaign, utm_campaign),
                )
                row = cur.fetchone()
                campaign_id = row[0] if row else None

            cur.execute(
                """
                INSERT INTO leads (
                    typeform_response_id, email, phone, name, utm_campaign, utm_adset, utm_ad,
                    utm_source, utm_medium, lp_url, form_completed_at, campaign_id, raw_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (typeform_response_id) DO UPDATE SET
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    name = EXCLUDED.name,
                    utm_campaign = EXCLUDED.utm_campaign,
                    utm_adset = EXCLUDED.utm_adset,
                    utm_ad = EXCLUDED.utm_ad,
                    utm_source = EXCLUDED.utm_source,
                    utm_medium = EXCLUDED.utm_medium,
                    lp_url = EXCLUDED.lp_url,
                    form_completed_at = EXCLUDED.form_completed_at,
                    campaign_id = EXCLUDED.campaign_id,
                    raw_data = EXCLUDED.raw_data
                RETURNING id
                """,
                (
                    parsed.get("response_id"),
                    parsed.get("email"),
                    parsed.get("phone"),
                    parsed.get("name"),
                    parsed.get("utm_campaign"),
                    parsed.get("utm_adset"),
                    parsed.get("utm_ad"),
                    parsed.get("utm_source"),
                    parsed.get("utm_medium"),
                    parsed.get("lp_url"),
                    parsed.get("submitted_at"),
                    campaign_id,
                    json.dumps(parsed.get("raw_data", {})),
                ),
            )
            lead_id = cur.fetchone()[0]

        board_id = os.getenv("MONDAY_BOARD_ID", "")
        if board_id:
            monday = MondayClient()
            item_name = parsed.get("name") or parsed.get("email") or f"Lead {lead_id}"
            monday_item_id = monday.create_item(
                int(board_id),
                item_name,
                {
                    "email": {"email": parsed.get("email"), "text": parsed.get("email")},
                    "phone": parsed.get("phone"),
                    "text": parsed.get("utm_campaign") or "",
                },
            )
            if monday_item_id:
                with get_db_cursor() as (_, cur):
                    cur.execute(
                        """
                        INSERT INTO lead_status (lead_id, monday_item_id, responded, meeting_scheduled, meeting_done, sale)
                        VALUES (%s, %s, FALSE, FALSE, FALSE, FALSE)
                        ON CONFLICT DO NOTHING
                        """,
                        (lead_id, monday_item_id),
                    )

        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("typeform_webhook_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=500, detail="internal_error")


@app.get("/health")
def health():
    try:
        with get_db_cursor() as (_, cur):
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        logger.exception("health_check_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=500, detail="database_disconnected")
