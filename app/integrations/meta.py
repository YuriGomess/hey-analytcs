import logging
import os
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)


class MetaAdsClient:
    base_url = "https://graph.facebook.com/v18.0"

    def __init__(self, access_token: str | None = None, ad_account_id: str | None = None) -> None:
        self.access_token = access_token or os.getenv("META_ACCESS_TOKEN", "")
        self.ad_account_id = ad_account_id or os.getenv("META_AD_ACCOUNT_ID", "")

    def _get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        full_params = {**params, "access_token": self.access_token}
        response = requests.get(url, params=full_params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_campaigns(self) -> list[dict[str, Any]]:
        if not self.ad_account_id:
            logger.warning("meta_missing_ad_account_id")
            return []
        try:
            data = self._get(
                f"{self.ad_account_id}/campaigns",
                {
                    "fields": "id,name,status",
                    "status": ["ACTIVE"],
                },
            )
            return data.get("data", [])
        except Exception as exc:
            logger.exception("meta_get_campaigns_failed", extra={"error": str(exc)})
            return []

    def get_adsets(self, campaign_id: str) -> list[dict[str, Any]]:
        try:
            data = self._get(f"{campaign_id}/adsets", {"fields": "id,name"})
            return data.get("data", [])
        except Exception as exc:
            logger.exception(
                "meta_get_adsets_failed",
                extra={"campaign_id": campaign_id, "error": str(exc)},
            )
            return []

    def get_ads(self, adset_id: str) -> list[dict[str, Any]]:
        try:
            data = self._get(f"{adset_id}/ads", {"fields": "id,name"})
            return data.get("data", [])
        except Exception as exc:
            logger.exception(
                "meta_get_ads_failed",
                extra={"adset_id": adset_id, "error": str(exc)},
            )
            return []

    def get_insights(self, campaign_id: str, date_start: date, date_end: date) -> list[dict[str, Any]]:
        try:
            data = self._get(
                f"{campaign_id}/insights",
                {
                    "fields": "impressions,cpm,clicks,inline_link_clicks,ctr,cpc,actions,spend",
                    "time_range": {"since": date_start.isoformat(), "until": date_end.isoformat()},
                    "time_increment": 1,
                },
            )
            rows = []
            for item in data.get("data", []):
                page_views = 0
                for action in item.get("actions", []):
                    if action.get("action_type") == "landing_page_view":
                        page_views = int(float(action.get("value", 0)))
                        break
                link_clicks = int(item.get("inline_link_clicks", 0) or 0)
                spend = float(item.get("spend", 0) or 0)
                rows.append(
                    {
                        "date": item.get("date_start"),
                        "impressions": int(item.get("impressions", 0) or 0),
                        "cpm": float(item.get("cpm", 0) or 0),
                        "clicks": int(item.get("clicks", 0) or 0),
                        "link_clicks": link_clicks,
                        "ctr": float(item.get("ctr", 0) or 0),
                        "cpc": float(item.get("cpc", 0) or 0),
                        "page_views": page_views,
                        "cost_per_page_view": (spend / page_views) if page_views else 0.0,
                        "spend": spend,
                    }
                )
            return rows
        except Exception as exc:
            logger.exception(
                "meta_get_insights_failed",
                extra={"campaign_id": campaign_id, "error": str(exc)},
            )
            return []
