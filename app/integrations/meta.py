import logging
import os
from json import dumps
from datetime import date
from collections import defaultdict
from typing import Any

import requests

logger = logging.getLogger(__name__)


class MetaAdsClient:
    base_url = "https://graph.facebook.com/v18.0"

    def __init__(self, access_token: str | None = None, ad_account_id: str | None = None) -> None:
        self.access_token = access_token or os.getenv("META_ACCESS_TOKEN", "")
        raw_account_id = (ad_account_id or os.getenv("META_AD_ACCOUNT_ID", "")).strip()
        self.ad_account_id = self._normalize_ad_account_id(raw_account_id)

    @staticmethod
    def _normalize_ad_account_id(ad_account_id: str) -> str:
        if not ad_account_id:
            return ""
        if ad_account_id.startswith("act_"):
            return ad_account_id
        if ad_account_id.isdigit():
            return f"act_{ad_account_id}"
        return ad_account_id

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        full_params = {**params, "access_token": self.access_token}
        response = requests.get(url, params=full_params, timeout=30)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            payload: dict[str, Any] = {}
            try:
                payload = response.json()
            except Exception:
                payload = {}
            error_data = payload.get("error", {}) if isinstance(payload, dict) else {}
            status_code = response.status_code
            code = error_data.get("code")
            subcode = error_data.get("error_subcode")
            message = error_data.get("message")
            if status_code in (401, 403):
                logger.error(
                    "meta_auth_or_permission_error",
                    extra={
                        "status_code": status_code,
                        "code": code,
                        "subcode": subcode,
                        "message": message,
                        "response_text": response.text,
                        "endpoint": endpoint,
                    },
                )
            else:
                logger.error(
                    "meta_api_error",
                    extra={
                        "status_code": status_code,
                        "code": code,
                        "subcode": subcode,
                        "message": message,
                        "response_text": response.text,
                        "endpoint": endpoint,
                    },
                )
            raise exc
        return response.json()

    def _get_paginated(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_url = f"{self.base_url}/{endpoint}"
        next_params: dict[str, Any] | None = {**params, "access_token": self.access_token}

        while next_url:
            response = requests.get(next_url, params=next_params, timeout=30)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                payload: dict[str, Any] = {}
                try:
                    payload = response.json()
                except Exception:
                    payload = {}
                error_data = payload.get("error", {}) if isinstance(payload, dict) else {}
                status_code = response.status_code
                if status_code in (401, 403):
                    logger.error(
                        "meta_auth_or_permission_error",
                        extra={
                            "status_code": status_code,
                            "code": error_data.get("code"),
                            "subcode": error_data.get("error_subcode"),
                            "message": error_data.get("message"),
                            "response_text": response.text,
                            "endpoint": endpoint,
                        },
                    )
                else:
                    logger.error(
                        "meta_api_error",
                        extra={
                            "status_code": status_code,
                            "code": error_data.get("code"),
                            "subcode": error_data.get("error_subcode"),
                            "message": error_data.get("message"),
                            "response_text": response.text,
                            "endpoint": endpoint,
                        },
                    )
                raise exc

            payload = response.json()
            items.extend(payload.get("data", []))
            next_url = payload.get("paging", {}).get("next")
            next_params = None

        return items

    def get_campaigns(self) -> list[dict[str, Any]]:
        if not self.ad_account_id:
            logger.warning("meta_missing_ad_account_id")
            return []
        try:
            campaigns = self._get_paginated(
                f"{self.ad_account_id}/campaigns",
                {
                    "fields": "id,name,status",
                    "effective_status": dumps(["ACTIVE"]),
                },
            )
            logger.info("meta_get_campaigns_ok", extra={"count": len(campaigns), "ad_account_id": self.ad_account_id})
            return campaigns
        except Exception as exc:
            logger.exception("meta_get_campaigns_failed", extra={"error": str(exc)})
            return []

    def get_adsets(self, campaign_id: str) -> list[dict[str, Any]]:
        try:
            adsets = self._get_paginated(f"{campaign_id}/adsets", {"fields": "id,name"})
            return adsets
        except Exception as exc:
            logger.exception(
                "meta_get_adsets_failed",
                extra={"campaign_id": campaign_id, "error": str(exc)},
            )
            return []

    def get_ads(self, adset_id: str) -> list[dict[str, Any]]:
        try:
            ads = self._get_paginated(f"{adset_id}/ads", {"fields": "id,name"})
            return ads
        except Exception as exc:
            logger.exception(
                "meta_get_ads_failed",
                extra={"adset_id": adset_id, "error": str(exc)},
            )
            return []

    def get_insights(self, campaign_id: str, date_start: date, date_end: date) -> list[dict[str, Any]]:
        try:
            raw_rows = self._get_paginated(
                f"{campaign_id}/insights",
                {
                    "fields": "impressions,clicks,inline_link_clicks,ctr,cpc,cpm,spend,actions",
                    "date_preset": "last_30d",
                    "level": "ad",
                    "time_increment": 1,
                },
            )

            logger.info(
                "meta_get_insights_response",
                extra={
                    "campaign_id": campaign_id,
                    "date_start": date_start.isoformat(),
                    "date_end": date_end.isoformat(),
                    "insights_count": len(raw_rows),
                },
            )

            if not raw_rows:
                logger.warning(
                    "Meta returned 0 insights",
                    extra={
                        "campaign_id": campaign_id,
                        "date_start": date_start.isoformat(),
                        "date_end": date_end.isoformat(),
                    },
                )
                return []

            aggregated_by_day: dict[str, dict[str, float]] = defaultdict(
                lambda: {
                    "impressions": 0.0,
                    "clicks": 0.0,
                    "link_clicks": 0.0,
                    "page_views": 0.0,
                    "meta_forms": 0.0,
                    "spend": 0.0,
                }
            )

            rows_without_form_conversion = 0

            for item in raw_rows:
                page_views = 0
                meta_forms = 0
                for action in item.get("actions", []):
                    if action.get("action_type") == "landing_page_view":
                        page_views = int(float(action.get("value", 0)))
                    if action.get("action_type") == "invitee_event_type_page":
                        meta_forms = int(float(action.get("value", 0)))

                if meta_forms == 0:
                    rows_without_form_conversion += 1

                day = item.get("date_start") or item.get("date_stop")
                if not day:
                    continue

                link_clicks = int(item.get("inline_link_clicks", 0) or 0)
                spend = float(item.get("spend", 0) or 0)
                aggregated = aggregated_by_day[day]
                aggregated["impressions"] += float(item.get("impressions", 0) or 0)
                aggregated["clicks"] += float(item.get("clicks", 0) or 0)
                aggregated["link_clicks"] += float(link_clicks)
                aggregated["page_views"] += float(page_views)
                aggregated["meta_forms"] += float(meta_forms)
                aggregated["spend"] += spend

            rows = []
            for day, totals in sorted(aggregated_by_day.items()):
                impressions = int(totals["impressions"])
                clicks = int(totals["clicks"])
                link_clicks = int(totals["link_clicks"])
                page_views = int(totals["page_views"])
                meta_forms = int(totals["meta_forms"])
                spend = float(totals["spend"])

                rows.append(
                    {
                        "date": day,
                        "impressions": impressions,
                        "cpm": ((spend * 1000) / impressions) if impressions else 0.0,
                        "clicks": clicks,
                        "link_clicks": link_clicks,
                        "ctr": ((clicks * 100) / impressions) if impressions else 0.0,
                        "cpc": (spend / clicks) if clicks else 0.0,
                        "page_views": page_views,
                        "meta_forms": meta_forms,
                        "cost_per_page_view": (spend / page_views) if page_views else 0.0,
                        "spend": spend,
                    }
                )

            total_meta_forms = sum(int(row.get("meta_forms", 0) or 0) for row in rows)
            logger.info(
                "meta_get_insights_forms_extracted",
                extra={
                    "campaign_id": campaign_id,
                    "total_meta_forms": total_meta_forms,
                    "rows_without_form_conversion": rows_without_form_conversion,
                },
            )
            if total_meta_forms == 0:
                logger.warning(
                    "meta_form_conversion_missing",
                    extra={
                        "campaign_id": campaign_id,
                        "conversion_action_type": "invitee_event_type_page",
                        "message": "Meta returned no invitee_event_type_page conversion values",
                    },
                )

            logger.info(
                "meta_get_insights_ok",
                extra={
                    "campaign_id": campaign_id,
                    "date_start": date_start.isoformat(),
                    "date_end": date_end.isoformat(),
                    "rows": len(rows),
                    "raw_rows": len(raw_rows),
                },
            )
            return rows
        except Exception as exc:
            logger.exception(
                "meta_get_insights_failed",
                extra={"campaign_id": campaign_id, "error": str(exc)},
            )
            return []
