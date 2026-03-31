import logging
import os
from datetime import date, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

from app.database import get_db_cursor
from app.integrations.meta import MetaAdsClient
from app.integrations.monday import MondayClient

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")


def sync_meta_ads() -> None:
    logger.info("sync_meta_ads_started")
    client = MetaAdsClient()
    campaigns = client.get_campaigns()
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
                            cpc, page_views, cost_per_page_view, spend, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (campaign_id, date) DO UPDATE SET
                            impressions = EXCLUDED.impressions,
                            cpm = EXCLUDED.cpm,
                            clicks = EXCLUDED.clicks,
                            link_clicks = EXCLUDED.link_clicks,
                            ctr = EXCLUDED.ctr,
                            cpc = EXCLUDED.cpc,
                            page_views = EXCLUDED.page_views,
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

    logger.info(
        "sync_meta_ads_finished",
        extra={
            "campaigns_found": len(campaigns),
            "campaigns_processed": processed_campaigns,
            "adsets_processed": processed_adsets,
            "ads_processed": processed_ads,
            "insight_rows_processed": processed_insight_rows,
        },
    )


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


def start_scheduler() -> None:
    if scheduler.running:
        return
    scheduler.add_job(sync_meta_ads, "interval", hours=1, id="sync_meta_ads", replace_existing=True)
    scheduler.add_job(sync_monday, "interval", minutes=30, id="sync_monday", replace_existing=True)
    scheduler.start()
    logger.info("scheduler_started")


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")
