from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class ParsedLead:
    response_id: str
    submitted_at: datetime | None
    email: str | None
    phone: str | None
    name: str | None
    utm_campaign: str | None
    utm_adset: str | None
    utm_ad: str | None
    utm_source: str | None
    utm_medium: str | None
    lp_url: str | None
    raw_data: dict[str, Any]
