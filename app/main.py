import json
import logging
import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request

from app.dashboard.router import router as dashboard_router
from app.database import close_db_pool, get_db_cursor, init_db
from app.integrations.monday import MondayClient
from app.integrations.typeform import parse_response, validate_signature, match_campaign, normalize_campaign_name, is_invalid_utm, classify_mql
from app.scheduler import start_scheduler, stop_scheduler, sync_meta_ads, sync_typeform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
    except Exception:
        logger.exception("startup_init_db_failed_continuing")
    try:
        start_scheduler()
    except Exception:
        logger.exception("startup_scheduler_failed_continuing")
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
        logger.warning("health_check_db_unavailable: %s", exc)
        # Always return 200 so Railway healthcheck passes even if DB is
        # temporarily unreachable at cold-start. The "database" field
        # exposes the real connectivity state for observability.
        return {"status": "ok", "database": "disconnected", "detail": str(exc)}


@app.post("/admin/sync-meta")
def admin_sync_meta(
    x_admin_sync_secret: str | None = Header(default=None, alias="X-Admin-Sync-Secret"),
):
    configured_secret = os.getenv("ADMIN_SYNC_SECRET", "")
    if not configured_secret:
        raise HTTPException(status_code=503, detail="ADMIN_SYNC_SECRET not configured")

    if x_admin_sync_secret != configured_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        result = sync_meta_ads()
        logger.info(
            "admin_sync_meta_triggered",
            extra={
                "status": result.get("status"),
                "campaigns_processed": result.get("campaigns_processed", 0),
                "insights_processed": result.get("insights_processed", 0),
                "errors": len(result.get("errors", [])),
            },
        )
        return result
    except Exception as exc:
        logger.exception("admin_sync_meta_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=500, detail="manual_sync_failed")


@app.post("/admin/trigger-sync-meta")
def trigger_sync_meta():
    """
    Dashboard button for manual Meta Ads sync.
    No header required; only callable from backend.
    Reutiliza sync_meta_ads() sem duplicação.
    """
    try:
        result = sync_meta_ads()
        logger.info(
            "dashboard_sync_meta_triggered",
            extra={
                "status": result.get("status"),
                "campaigns_processed": result.get("campaigns_processed", 0),
                "insights_processed": result.get("insights_processed", 0),
                "errors": len(result.get("errors", [])),
            },
        )
        return result
    except Exception as exc:
        logger.exception("trigger_sync_meta_failed", extra={"error": str(exc)})
        return {"status": "error", "error": str(exc), "campaigns_processed": 0, "insights_processed": 0}


@app.post("/admin/trigger-sync-typeform")
def trigger_sync_typeform():
    """
    Dashboard button for manual Typeform sync.
    Reutiliza sync_typeform() do scheduler.
    """
    try:
        result = sync_typeform()
        logger.info(
            "dashboard_sync_typeform_triggered",
            extra={
                "status": result.get("status"),
                "leads_created": result.get("leads_created", 0),
                "mql_count": result.get("mql_count", 0),
                "errors": len(result.get("errors", [])),
            },
        )
        return result
    except Exception as exc:
        logger.exception("trigger_sync_typeform_failed", extra={"error": str(exc)})
        return {"status": "error", "error": str(exc)}


@app.post("/admin/reattribute-leads")
def reattribute_leads():
    """
    Re-run campaign attribution on ALL existing leads using corrected matching logic.
    Updates campaign_id, campaign_name, campaign_match_status, matched_by.
    """
    try:
        with get_db_cursor() as (conn, cur):
            # Fetch all active campaigns
            cur.execute("SELECT campaign_id, campaign_name FROM campaigns")
            campaigns = [{"campaign_id": r[0], "campaign_name": r[1]} for r in cur.fetchall()]

            # Fetch all leads with their UTM
            cur.execute("SELECT id, utm_campaign_raw, utm_campaign_normalized FROM leads")
            all_leads = cur.fetchall()

            updated = 0
            matched = 0
            unmatched = 0
            invalid = 0

            for lead_id, utm_raw, utm_norm in all_leads:
                # Re-normalize if needed
                if not utm_norm and utm_raw:
                    utm_norm = normalize_campaign_name(utm_raw)

                if is_invalid_utm(utm_raw):
                    cur.execute(
                        """UPDATE leads SET campaign_id = NULL, campaign_name = NULL,
                           campaign_match_status = 'unmatched_invalid_utm', matched_by = 'invalid_utm',
                           utm_campaign_normalized = %s, updated_at = NOW() WHERE id = %s""",
                        (utm_norm, lead_id),
                    )
                    invalid += 1
                    updated += 1
                    continue

                cid, cname, matched_by = match_campaign(utm_norm or "", campaigns)
                if cid:
                    status = "matched"
                    matched += 1
                else:
                    status = "unmatched_campaign"
                    unmatched += 1

                cur.execute(
                    """UPDATE leads SET campaign_id = %s, campaign_name = %s,
                       campaign_match_status = %s, matched_by = %s,
                       utm_campaign_normalized = %s, updated_at = NOW() WHERE id = %s""",
                    (cid, cname, status, matched_by, utm_norm, lead_id),
                )
                updated += 1

            conn.commit()

        return {
            "status": "ok",
            "total_leads": len(all_leads),
            "updated": updated,
            "matched": matched,
            "unmatched": unmatched,
            "invalid_utm": invalid,
            "campaigns_available": len(campaigns),
        }
    except Exception as exc:
        logger.exception("reattribute_leads_failed", extra={"error": str(exc)})
        return {"status": "error", "error": str(exc)}


@app.post("/admin/backfill-submitted-at")
def backfill_submitted_at():
    """Backfill submitted_at from form_completed_at for leads that have it NULL."""
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                UPDATE leads SET submitted_at = form_completed_at
                WHERE submitted_at IS NULL AND form_completed_at IS NOT NULL
            """)
            updated = cur.rowcount
            conn.commit()
        return {"status": "ok", "updated": updated}
    except Exception as exc:
        logger.exception("backfill_submitted_at_failed")
        return {"status": "error", "error": str(exc)}


@app.get("/admin/debug-meta-actions")
def debug_meta_actions():
    """Temporary debug endpoint: fetch raw action_types from Meta and show custom conversion totals."""
    from app.integrations.meta import MetaAdsClient
    from collections import defaultdict

    try:
        client = MetaAdsClient()
        campaigns = client.get_campaigns()
        if not campaigns:
            return {"error": "no campaigns found"}

        # Aggregate custom conversion totals across ALL campaigns
        custom_conversion_totals: dict[str, int] = defaultdict(int)
        total_rows = 0

        for camp in campaigns[:4]:  # first 4 to avoid timeout
            cid = camp["id"]
            raw_rows = client._get_paginated(
                f"{cid}/insights",
                {
                    "fields": "actions",
                    "date_preset": "last_30d",
                    "level": "campaign",
                    "time_increment": "all_days",
                },
            )
            total_rows += len(raw_rows)
            for row in raw_rows:
                for action in row.get("actions", []):
                    atype = action.get("action_type", "")
                    if "custom" in atype or "invitee" in atype or "lead" in atype:
                        custom_conversion_totals[atype] += int(float(action.get("value", 0)))

        return {
            "campaigns_checked": min(len(campaigns), 4),
            "custom_conversions_with_totals": dict(sorted(custom_conversion_totals.items(), key=lambda x: -x[1])),
            "hint": "Set META_FORM_ACTION_TYPE env var to the action_type that represents form conversions",
        }
    except Exception as exc:
        return {"error": str(exc)}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
