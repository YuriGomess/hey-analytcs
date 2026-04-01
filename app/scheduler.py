import json
import logging
import os
from datetime import date, datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

from app.database import get_db_cursor
from app.integrations.meta import MetaAdsClient
from app.integrations.monday import MondayClient
from app.integrations.typeform import (
    TypeformClient,
    classify_mql,
    is_invalid_utm,
    match_campaign,
    normalize_campaign_name,
    parse_api_response,
)

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")


def sync_meta_ads() -> dict:
    logger.info("sync_meta_ads_started")
    client = MetaAdsClient()

    report = {
        "status": "ok",
        "campaigns_found": 0,
        "campaigns_processed": 0,
        "adsets_processed": 0,
        "ads_processed": 0,
        "insights_processed": 0,
        "errors": [],
    }

    try:
        campaigns = client.get_campaigns()
    except Exception as exc:
        logger.exception("sync_meta_ads_fetch_campaigns_failed", extra={"error": str(exc)})
        report["status"] = "error"
        report["errors"].append({"stage": "get_campaigns", "error": str(exc)})
        return report

    report["campaigns_found"] = len(campaigns)
    if not campaigns:
        logger.warning(
            "sync_meta_ads_no_campaigns",
            extra={"ad_account_id": client.ad_account_id},
        )

    processed_campaigns = 0
    processed_adsets = 0
    processed_ads = 0
    processed_insight_rows = 0

    for campaign in campaigns:
        campaign_id = campaign.get("id")
        campaign_name = campaign.get("name")
        if not campaign_id:
            continue

        try:
            processed_campaigns += 1
            campaign_adsets = 0
            campaign_ads = 0
            adsets = client.get_adsets(campaign_id)
            if not adsets:
                adsets = [{"id": None, "name": None}]

            for adset in adsets:
                if adset.get("id"):
                    processed_adsets += 1
                    campaign_adsets += 1
                ads = client.get_ads(adset.get("id")) if adset.get("id") else []
                if not ads:
                    ads = [{"id": None, "name": None}]

                for ad in ads:
                    if ad.get("id"):
                        processed_ads += 1
                        campaign_ads += 1
                    with get_db_cursor() as (_, cur):
                        cur.execute(
                            """
                            INSERT INTO campaigns (campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (campaign_id) DO UPDATE SET
                                campaign_name = EXCLUDED.campaign_name,
                                adset_id = EXCLUDED.adset_id,
                                adset_name = EXCLUDED.adset_name,
                                ad_id = EXCLUDED.ad_id,
                                ad_name = EXCLUDED.ad_name
                            """,
                            (
                                campaign_id,
                                campaign_name,
                                adset.get("id"),
                                adset.get("name"),
                                ad.get("id"),
                                ad.get("name"),
                            ),
                        )

            date_end = date.today()
            date_start = date_end - timedelta(days=29)
            insights = client.get_insights(campaign_id, date_start, date_end)
            processed_insight_rows += len(insights)

            for row in insights:
                with get_db_cursor() as (_, cur):
                    cur.execute(
                        """
                        INSERT INTO campaign_metrics (
                            campaign_id, date, impressions, cpm, clicks, link_clicks, ctr,
                            cpc, page_views, meta_forms, cost_per_page_view, spend, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (campaign_id, date) DO UPDATE SET
                            impressions = EXCLUDED.impressions,
                            cpm = EXCLUDED.cpm,
                            clicks = EXCLUDED.clicks,
                            link_clicks = EXCLUDED.link_clicks,
                            ctr = EXCLUDED.ctr,
                            cpc = EXCLUDED.cpc,
                            page_views = EXCLUDED.page_views,
                            meta_forms = EXCLUDED.meta_forms,
                            cost_per_page_view = EXCLUDED.cost_per_page_view,
                            spend = EXCLUDED.spend,
                            updated_at = NOW()
                        """,
                        (
                            campaign_id,
                            row.get("date"),
                            row.get("impressions", 0),
                            row.get("cpm", 0),
                            row.get("clicks", 0),
                            row.get("link_clicks", 0),
                            row.get("ctr", 0),
                            row.get("cpc", 0),
                            row.get("page_views", 0),
                            row.get("meta_forms", 0),
                            row.get("cost_per_page_view", 0),
                            row.get("spend", 0),
                        ),
                    )

            logger.info(
                "sync_meta_ads_campaign_ok",
                extra={
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "adsets": campaign_adsets,
                    "ads": campaign_ads,
                    "insights_rows": len(insights),
                },
            )
        except Exception as exc:
            logger.exception(
                "sync_meta_ads_campaign_failed",
                extra={"campaign_id": campaign_id, "error": str(exc)},
            )
            report["errors"].append(
                {
                    "stage": "campaign",
                    "campaign_id": campaign_id,
                    "error": str(exc),
                }
            )

    report["campaigns_processed"] = processed_campaigns
    report["adsets_processed"] = processed_adsets
    report["ads_processed"] = processed_ads
    report["insights_processed"] = processed_insight_rows
    if report["errors"]:
        report["status"] = "partial_success"

    logger.info(
        "sync_meta_ads_finished",
        extra={
            "campaigns_found": len(campaigns),
            "campaigns_processed": processed_campaigns,
            "adsets_processed": processed_adsets,
            "ads_processed": processed_ads,
            "insight_rows_processed": processed_insight_rows,
            "errors": len(report["errors"]),
        },
    )
    return report


def sync_monday() -> None:
    logger.info("sync_monday_started")
    board_id = os.getenv("MONDAY_BOARD_ID", "")
    if not board_id:
        logger.warning("sync_monday_skipped_missing_board_id")
        return

    client = MondayClient()
    items = client.get_items(int(board_id))

    for item in items:
        try:
            email = item.get("email")
            if not email:
                continue

            with get_db_cursor() as (_, cur):
                cur.execute(
                    "SELECT id FROM leads WHERE LOWER(email) = LOWER(%s) ORDER BY id DESC LIMIT 1",
                    (email,),
                )
                lead_row = cur.fetchone()
                if not lead_row:
                    continue

                lead_id = lead_row[0]

                cur.execute("SELECT id FROM lead_status WHERE lead_id = %s", (lead_id,))
                status_row = cur.fetchone()

                if status_row:
                    cur.execute(
                        """
                        UPDATE lead_status
                        SET monday_item_id = %s,
                            responded = %s,
                            meeting_scheduled = %s,
                            meeting_scheduled_at = %s,
                            meeting_done = %s,
                            meeting_done_at = %s,
                            sale = %s,
                            sale_at = %s,
                            updated_at = NOW()
                        WHERE lead_id = %s
                        """,
                        (
                            item.get("id"),
                            item.get("responded", False),
                            item.get("meeting_scheduled", False),
                            item.get("meeting_scheduled_at"),
                            item.get("meeting_done", False),
                            item.get("meeting_done_at"),
                            item.get("sale", False),
                            item.get("sale_at"),
                            lead_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO lead_status (
                            lead_id, monday_item_id, responded, meeting_scheduled,
                            meeting_scheduled_at, meeting_done, meeting_done_at, sale, sale_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            lead_id,
                            item.get("id"),
                            item.get("responded", False),
                            item.get("meeting_scheduled", False),
                            item.get("meeting_scheduled_at"),
                            item.get("meeting_done", False),
                            item.get("meeting_done_at"),
                            item.get("sale", False),
                            item.get("sale_at"),
                        ),
                    )

            logger.info("sync_monday_item_ok", extra={"item_id": item.get("id"), "email": email})
        except Exception as exc:
            logger.exception(
                "sync_monday_item_failed",
                extra={"item_id": item.get("id"), "error": str(exc)},
            )

    logger.info("sync_monday_finished", extra={"items": len(items)})


def sync_typeform() -> dict:
    """Sync leads from Typeform API, match to campaigns, classify MQL."""
    logger.info("typeform_sync_started")

    report = {
        "status": "ok",
        "responses_found": 0,
        "leads_created": 0,
        "leads_updated": 0,
        "campaigns_matched": 0,
        "campaigns_unmatched": 0,
        "invalid_utm": 0,
        "mql_count": 0,
        "errors": [],
    }

    client = TypeformClient()
    if not client.api_token or not client.form_id:
        report["status"] = "skipped"
        report["errors"].append({"stage": "config", "error": "TYPEFORM_API_TOKEN or TYPEFORM_FORM_ID not configured"})
        logger.warning("typeform_sync_skipped_missing_config")
        return report

    try:
        # Determine 'since' cursor from last synced response (with 2h buffer)
        since: str | None = None
        with get_db_cursor() as (_, cur):
            cur.execute("""
                SELECT MAX(form_completed_at) FROM leads
                WHERE source IN ('typeform_api', 'webhook')
            """)
            row = cur.fetchone()
            if row and row[0]:
                buffer_time = row[0] - timedelta(hours=2)
                since_str = buffer_time.isoformat()
                if "Z" not in since_str and "+" not in since_str:
                    since_str += "Z"
                since = since_str

        form_title = client.get_form_title()
        responses = client.fetch_responses(since=since)
        report["responses_found"] = len(responses)

        logger.info("typeform_sync_responses_found", extra={
            "responses_found": len(responses),
            "completed_responses_found": len(responses),
            "since": since,
        })

        if not responses:
            logger.info("typeform_sync_no_new_responses")
            return report

        # Load campaigns for matching
        campaigns: list[dict[str, str]] = []
        with get_db_cursor() as (_, cur):
            cur.execute("SELECT campaign_id, campaign_name FROM campaigns")
            campaigns = [{"campaign_id": r[0], "campaign_name": r[1]} for r in cur.fetchall()]

        for item in responses:
            try:
                parsed = parse_api_response(item, form_id=client.form_id, form_name=form_title)

                # Campaign attribution
                utm_raw = parsed["utm_campaign_raw"]
                utm_norm = parsed["utm_campaign_normalized"]
                campaign_id: str | None = None
                campaign_name_matched: str | None = None
                match_status = "no_match"
                matched_by = "no_match"

                if is_invalid_utm(utm_raw):
                    match_status = "unmatched_invalid_utm"
                    matched_by = "invalid_utm"
                    report["invalid_utm"] += 1
                elif utm_norm:
                    campaign_id, campaign_name_matched, matched_by = match_campaign(utm_norm, campaigns)
                    if campaign_id:
                        match_status = "matched"
                        report["campaigns_matched"] += 1
                    else:
                        match_status = "unmatched_campaign"
                        report["campaigns_unmatched"] += 1

                # MQL classification
                is_mql, mql_reason = classify_mql(parsed)
                if is_mql:
                    report["mql_count"] += 1

                # Upsert lead
                with get_db_cursor() as (_, cur):
                    cur.execute("""
                        INSERT INTO leads (
                            typeform_response_id, source, form_id, form_name,
                            response_token, response_type,
                            email, phone, name, instagram,
                            business_area, revenue_range, paid_traffic_fit,
                            already_runs_paid_traffic, sales_challenge,
                            urgency_stage, best_contact_time,
                            utm_source, utm_medium, utm_campaign,
                            utm_campaign_raw, utm_campaign_normalized, utm_term,
                            form_completed_at, landed_at,
                            campaign_id, campaign_name, campaign_match_status, matched_by,
                            is_mql, mql_reason,
                            raw_data, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s, %s,
                            %s, %s,
                            %s::jsonb, NOW()
                        )
                        ON CONFLICT (typeform_response_id) DO UPDATE SET
                            source = EXCLUDED.source,
                            form_id = EXCLUDED.form_id,
                            form_name = EXCLUDED.form_name,
                            response_token = EXCLUDED.response_token,
                            email = COALESCE(EXCLUDED.email, leads.email),
                            phone = COALESCE(EXCLUDED.phone, leads.phone),
                            name = COALESCE(EXCLUDED.name, leads.name),
                            instagram = COALESCE(EXCLUDED.instagram, leads.instagram),
                            business_area = COALESCE(EXCLUDED.business_area, leads.business_area),
                            revenue_range = COALESCE(EXCLUDED.revenue_range, leads.revenue_range),
                            paid_traffic_fit = COALESCE(EXCLUDED.paid_traffic_fit, leads.paid_traffic_fit),
                            already_runs_paid_traffic = COALESCE(EXCLUDED.already_runs_paid_traffic, leads.already_runs_paid_traffic),
                            sales_challenge = COALESCE(EXCLUDED.sales_challenge, leads.sales_challenge),
                            urgency_stage = COALESCE(EXCLUDED.urgency_stage, leads.urgency_stage),
                            best_contact_time = COALESCE(EXCLUDED.best_contact_time, leads.best_contact_time),
                            utm_campaign_raw = EXCLUDED.utm_campaign_raw,
                            utm_campaign_normalized = EXCLUDED.utm_campaign_normalized,
                            utm_term = EXCLUDED.utm_term,
                            campaign_id = EXCLUDED.campaign_id,
                            campaign_name = EXCLUDED.campaign_name,
                            campaign_match_status = EXCLUDED.campaign_match_status,
                            matched_by = EXCLUDED.matched_by,
                            is_mql = EXCLUDED.is_mql,
                            mql_reason = EXCLUDED.mql_reason,
                            raw_data = EXCLUDED.raw_data,
                            updated_at = NOW()
                        RETURNING id, (xmax = 0) AS is_new
                    """, (
                        parsed["response_token"] or parsed["response_id"],
                        parsed["source"],
                        parsed["form_id"],
                        parsed["form_name"],
                        parsed["response_token"],
                        parsed["response_type"],
                        parsed.get("email"),
                        parsed["phone"],
                        parsed["name"],
                        parsed["instagram"],
                        parsed["business_area"],
                        parsed["revenue_range"],
                        parsed["paid_traffic_fit"],
                        parsed["already_runs_paid_traffic"],
                        parsed["sales_challenge"],
                        parsed["urgency_stage"],
                        parsed["best_contact_time"],
                        parsed["utm_source"],
                        parsed["utm_medium"],
                        parsed["utm_campaign_raw"],
                        parsed["utm_campaign_raw"],
                        parsed["utm_campaign_normalized"],
                        parsed["utm_term"],
                        parsed["submitted_at"],
                        parsed["landed_at"],
                        campaign_id,
                        campaign_name_matched,
                        match_status,
                        matched_by,
                        is_mql,
                        mql_reason,
                        json.dumps(parsed["raw_payload"]),
                    ))
                    result_row = cur.fetchone()
                    if result_row and result_row[1]:  # is_new = True (INSERT)
                        report["leads_created"] += 1
                    else:
                        report["leads_updated"] += 1

            except Exception as exc:
                logger.exception("typeform_sync_response_failed", extra={
                    "token": item.get("token", "?"),
                    "error": str(exc),
                })
                report["errors"].append({
                    "stage": "process_response",
                    "token": item.get("token", "?"),
                    "error": str(exc),
                })

    except Exception as exc:
        logger.exception("typeform_sync_failed", extra={"error": str(exc)})
        report["status"] = "error"
        report["errors"].append({"stage": "fetch_responses", "error": str(exc)})

    if report["errors"] and report["status"] == "ok":
        report["status"] = "partial_success"

    logger.info("typeform_sync_finished", extra={
        "responses_found": report["responses_found"],
        "leads_created": report["leads_created"],
        "leads_updated": report["leads_updated"],
        "campaigns_matched": report["campaigns_matched"],
        "campaigns_unmatched": report["campaigns_unmatched"],
        "invalid_utm": report["invalid_utm"],
        "mql_count": report["mql_count"],
        "errors_count": len(report["errors"]),
    })

    return report


def start_scheduler() -> None:
    if scheduler.running:
        return
    scheduler.add_job(sync_meta_ads, "interval", hours=1, id="sync_meta_ads", replace_existing=True)
    scheduler.add_job(sync_typeform, "interval", minutes=15, id="sync_typeform", replace_existing=True)
    scheduler.add_job(sync_monday, "interval", minutes=30, id="sync_monday", replace_existing=True)
    scheduler.start()
    logger.info("scheduler_started")


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
