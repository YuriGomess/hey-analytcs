"""
Typeform integration: webhook validation, API sync client, campaign attribution, MQL classification.

Field matching uses keyword patterns against field.ref + field.title so the parser
adapts to different Typeform form configurations without hardcoding field IDs.
If the form structure changes, update _FIELD_PATTERNS below.
"""

import hashlib
import hmac
import logging
import os
import unicodedata
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)


# ─── UTM validation ──────────────────────────────────────────────────────────

INVALID_UTM_VALUES = frozenset(
    {"", "undefined", "null", "nan", "none", "test", "xxxxx",
     "{{campaign.name}}", "{{ad.name}}", "{{adset.name}}"}
)


def is_invalid_utm(value: str | None) -> bool:
    """Check if a UTM value is a placeholder or garbage."""
    if not value:
        return True
    normalized = value.strip().lower()
    if normalized in INVALID_UTM_VALUES:
        return True
    if "{{" in normalized and "}}" in normalized:
        return True
    return False


# ─── Campaign name normalization ─────────────────────────────────────────────

def normalize_campaign_name(value: str | None) -> str:
    """
    Normalize a campaign name for fuzzy matching:
    lowercase, strip accents, collapse whitespace.
    """
    if not value or not isinstance(value, str):
        return ""
    text = value.strip().lower()
    nfkd = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in nfkd if not unicodedata.category(c).startswith("M"))
    text = " ".join(text.split())
    return text


# ─── Campaign matching ───────────────────────────────────────────────────────

def match_campaign(
    utm_campaign_normalized: str,
    campaigns: list[dict[str, str]],
) -> tuple[str | None, str | None, str]:
    """
    Match a lead's utm_campaign to a Meta campaign.
    Returns (campaign_id, campaign_name, matched_by).
    Priority: exact → contains → token overlap (>50%).
    """
    if not utm_campaign_normalized:
        return None, None, "no_match"

    # 1. Exact match
    for c in campaigns:
        c_norm = normalize_campaign_name(c.get("campaign_name", ""))
        if c_norm and c_norm == utm_campaign_normalized:
            return c["campaign_id"], c["campaign_name"], "exact_utm_campaign"

    # 2. Contains (utm inside campaign name or vice-versa)
    for c in campaigns:
        c_norm = normalize_campaign_name(c.get("campaign_name", ""))
        if not c_norm:
            continue
        if utm_campaign_normalized in c_norm or c_norm in utm_campaign_normalized:
            return c["campaign_id"], c["campaign_name"], "fuzzy_utm_campaign"

    # 3. Token overlap >50%
    utm_tokens = set(utm_campaign_normalized.split())
    if len(utm_tokens) >= 2:
        best_match = None
        best_overlap = 0
        for c in campaigns:
            c_norm = normalize_campaign_name(c.get("campaign_name", ""))
            c_tokens = set(c_norm.split())
            if not c_tokens:
                continue
            overlap = len(utm_tokens & c_tokens)
            ratio = overlap / max(len(utm_tokens), len(c_tokens))
            if ratio > 0.5 and overlap > best_overlap:
                best_overlap = overlap
                best_match = c
        if best_match:
            return best_match["campaign_id"], best_match["campaign_name"], "fuzzy_utm_campaign"

    return None, None, "no_match"


# ─── MQL Classification ─────────────────────────────────────────────────────

def classify_mql(lead_data: dict[str, Any]) -> tuple[bool, str | None]:
    """
    Classify a lead as MQL based on Typeform answers.
    MQL = at least 1 strong qualifying signal.
    Returns (is_mql, mql_reason).
    """
    reasons: list[str] = []

    # 1. Already runs paid traffic
    already_runs = (lead_data.get("already_runs_paid_traffic") or "").strip().lower()
    positive_already = {"sim", "yes", "ja invisto", "já invisto", "ja", "já"}
    if already_runs and already_runs in positive_already:
        reasons.append("already_runs_paid_traffic")

    # 2. Revenue range qualifies
    revenue = (lead_data.get("revenue_range") or "").strip().lower()
    revenue_qualifiers = [
        "100", "200", "500", "1m", "milhao", "milhão",
        "acima", "mais de", "50k", "100k", "200k",
    ]
    if revenue and any(q in revenue for q in revenue_qualifiers):
        reasons.append("revenue_qualifies")

    # 3. Paid traffic fit / interest positive
    fit = (lead_data.get("paid_traffic_fit") or "").strip().lower()
    negative_fit = {"nao", "não", "no", "nunca", "nenhum", ""}
    if fit and fit not in negative_fit:
        reasons.append("paid_traffic_fit")

    # 4. Urgency
    urgency = (lead_data.get("urgency_stage") or "").strip().lower()
    urgency_qualifiers = [
        "imediato", "urgente", "agora", "esta semana", "este mes",
        "este mês", "proximo mes", "próximo mês", "1 mes", "já",
    ]
    if urgency and any(q in urgency for q in urgency_qualifiers):
        reasons.append("urgency_high")

    # 5. Sales challenge relevance (non-trivial answer)
    challenge = (lead_data.get("sales_challenge") or "").strip()
    if challenge and len(challenge) > 10:
        reasons.append("has_sales_challenge")

    is_mql = len(reasons) >= 1
    return is_mql, "; ".join(reasons) if reasons else None


# ─── Webhook signature validation (existing) ─────────────────────────────────

def validate_signature(payload: bytes, signature: str, secret: str) -> bool:
    if not signature or not signature.startswith("sha256="):
        return False
    provided_hash = signature.split("=", 1)[1]
    expected_hash = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(provided_hash, expected_hash)


# ─── Answer value extraction ─────────────────────────────────────────────────

def _extract_answer_value(answer: dict[str, Any]) -> str | None:
    """Extract the display value from any Typeform answer type."""
    atype = answer.get("type", "")
    if atype == "text":
        return answer.get("text")
    elif atype == "email":
        return answer.get("email")
    elif atype == "phone_number":
        return answer.get("phone_number")
    elif atype == "number":
        val = answer.get("number")
        return str(val) if val is not None else None
    elif atype == "boolean":
        val = answer.get("boolean")
        return "Sim" if val else ("Não" if val is not None else None)
    elif atype == "choice":
        choice = answer.get("choice", {})
        return choice.get("label") or choice.get("other")
    elif atype == "choices":
        choices = answer.get("choices", {})
        labels = choices.get("labels", [])
        other = choices.get("other")
        parts = labels + ([other] if other else [])
        return "; ".join(parts) if parts else None
    elif atype == "date":
        return answer.get("date")
    elif atype == "url":
        return answer.get("url")
    elif atype == "file_url":
        return answer.get("file_url")
    for key in ("text", "email", "phone_number", "number", "url"):
        if key in answer:
            return str(answer[key])
    return None


# ─── Field matching by keyword patterns ──────────────────────────────────────
# Maps lead field names to keyword patterns for matching against field ref + title.
# To adapt to a new Typeform form, update the patterns below.

_FIELD_PATTERNS: list[tuple[str, list[str]]] = [
    ("name", ["nome", "name", "seu nome"]),
    ("phone", ["telefone", "phone", "celular", "whatsapp", "whats"]),
    ("email", ["email", "e-mail"]),
    ("instagram", ["instagram", "insta"]),
    ("business_area", ["area", "segmento", "nicho", "ramo", "setor"]),
    ("revenue_range", ["faturamento", "receita", "revenue", "fatura"]),
    ("paid_traffic_fit", ["interesse", "fit", "encaixa", "perfil"]),
    ("already_runs_paid_traffic", ["trafego", "traffic", "ja investe", "investe em", "anuncio"]),
    ("sales_challenge", ["desafio", "challenge", "dificuldade", "problema", "dor"]),
    ("urgency_stage", ["urgencia", "urgência", "quando", "prazo", "tempo"]),
    ("best_contact_time", ["horario", "horário", "contato", "melhor hora", "ligar"]),
]


def _match_field_to_lead_key(field_ref: str, field_title: str, field_type: str) -> str | None:
    """Map a Typeform field to a lead data key by type or keyword match."""
    if field_type == "email":
        return "email"
    if field_type == "phone_number":
        return "phone"

    searchable = f"{field_ref} {field_title}".lower()
    nfkd = unicodedata.normalize("NFKD", searchable)
    searchable = "".join(c for c in nfkd if not unicodedata.category(c).startswith("M"))

    for lead_key, patterns in _FIELD_PATTERNS:
        for pattern in patterns:
            if pattern in searchable:
                return lead_key
    return None


# ─── Parse webhook payload (backward-compatible) ─────────────────────────────

def parse_response(data: dict[str, Any]) -> dict[str, Any]:
    """Parse a Typeform webhook payload. Kept for backward compatibility."""
    form_response = data.get("form_response", {})
    response_id = form_response.get("token") or form_response.get("response_id")
    submitted_at_raw = form_response.get("submitted_at")

    email = None
    phone = None
    name = None

    for answer in form_response.get("answers", []):
        answer_type = answer.get("type")
        if answer_type == "email" and not email:
            email = answer.get("email")
        elif answer_type == "phone_number" and not phone:
            phone = answer.get("phone_number")
        elif answer_type == "text" and not name:
            name = answer.get("text")

    hidden = form_response.get("hidden", {}) or {}

    submitted_at = None
    if submitted_at_raw:
        try:
            submitted_at = datetime.fromisoformat(submitted_at_raw.replace("Z", "+00:00"))
        except ValueError:
            submitted_at = None

    return {
        "response_id": response_id,
        "submitted_at": submitted_at,
        "email": email,
        "phone": phone,
        "name": name,
        "utm_campaign": hidden.get("utm_campaign"),
        "utm_adset": hidden.get("utm_adset"),
        "utm_ad": hidden.get("utm_ad"),
        "utm_source": hidden.get("utm_source"),
        "utm_medium": hidden.get("utm_medium"),
        "lp_url": hidden.get("lp_url"),
        "raw_data": data,
    }


# ─── Parse API response item ─────────────────────────────────────────────────

def parse_api_response(
    item: dict[str, Any],
    form_id: str = "",
    form_name: str = "",
) -> dict[str, Any]:
    """
    Parse a single Typeform API response item into a lead dict.
    Extracts all fields via keyword matching and hidden fields for UTMs.
    """
    response_id = item.get("response_id") or item.get("token", "")
    response_token = item.get("token", "")
    landed_at_raw = item.get("landed_at")
    submitted_at_raw = item.get("submitted_at")

    landed_at = None
    if landed_at_raw:
        try:
            landed_at = datetime.fromisoformat(landed_at_raw.replace("Z", "+00:00"))
        except ValueError:
            pass

    submitted_at = None
    if submitted_at_raw:
        try:
            submitted_at = datetime.fromisoformat(submitted_at_raw.replace("Z", "+00:00"))
        except ValueError:
            pass

    hidden = item.get("hidden", {}) or {}
    utm_source = hidden.get("utm_source") or hidden.get("utmsource")
    utm_medium = hidden.get("utm_medium") or hidden.get("utmmedium")
    utm_campaign_raw = hidden.get("utm_campaign") or hidden.get("utmcampaign")
    utm_term = hidden.get("utm_term") or hidden.get("utmterm")

    utm_campaign_normalized = ""
    if not is_invalid_utm(utm_campaign_raw):
        utm_campaign_normalized = normalize_campaign_name(utm_campaign_raw)

    lead_fields: dict[str, str | None] = {
        "name": None,
        "phone": None,
        "email": None,
        "instagram": None,
        "business_area": None,
        "revenue_range": None,
        "paid_traffic_fit": None,
        "already_runs_paid_traffic": None,
        "sales_challenge": None,
        "urgency_stage": None,
        "best_contact_time": None,
    }
    matched_keys: set[str] = set()

    for answer in item.get("answers", []):
        field = answer.get("field", {})
        field_ref = field.get("ref", "")
        field_title = field.get("title", "")
        field_type = field.get("type", "")
        value = _extract_answer_value(answer)

        if not value:
            continue

        lead_key = _match_field_to_lead_key(field_ref, field_title, field_type)
        if lead_key and lead_key not in matched_keys:
            lead_fields[lead_key] = value
            matched_keys.add(lead_key)

    return {
        "source": "typeform_api",
        "form_id": form_id,
        "form_name": form_name,
        "response_id": response_id,
        "response_token": response_token,
        "response_type": "completed",
        "submitted_at": submitted_at,
        "landed_at": landed_at,
        **lead_fields,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign_raw": utm_campaign_raw,
        "utm_campaign_normalized": utm_campaign_normalized,
        "utm_term": utm_term,
        "raw_payload": item,
    }


# ─── Typeform API Client ─────────────────────────────────────────────────────

class TypeformClient:
    """Client for the Typeform Responses API with cursor-based pagination."""

    base_url = "https://api.typeform.com"

    def __init__(
        self,
        api_token: str | None = None,
        form_id: str | None = None,
    ) -> None:
        self.api_token = api_token or os.getenv("TYPEFORM_API_TOKEN", "")
        self.form_id = form_id or os.getenv("TYPEFORM_FORM_ID", "")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    def fetch_responses(
        self,
        since: str | None = None,
        completed: bool = True,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch all responses from the form with cursor-based pagination."""
        if not self.api_token:
            logger.warning("typeform_api_token_not_configured")
            return []
        if not self.form_id:
            logger.warning("typeform_form_id_not_configured")
            return []

        all_items: list[dict[str, Any]] = []
        url = f"{self.base_url}/forms/{self.form_id}/responses"
        after: str | None = None

        while True:
            params: dict[str, Any] = {
                "page_size": page_size,
                "completed": str(completed).lower(),
                "sort": "submitted_at,asc",
            }
            if since:
                params["since"] = since
            if after:
                params["after"] = after

            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
                resp.raise_for_status()
            except requests.HTTPError:
                logger.error(
                    "typeform_api_error",
                    extra={
                        "status_code": resp.status_code,
                        "response_text": resp.text[:500],
                        "form_id": self.form_id,
                    },
                )
                raise
            except requests.RequestException as exc:
                logger.error("typeform_api_connection_error", extra={"error": str(exc)})
                raise

            data = resp.json()
            items = data.get("items", [])
            all_items.extend(items)

            logger.info(
                "typeform_api_page_fetched",
                extra={
                    "items_in_page": len(items),
                    "total_fetched": len(all_items),
                    "total_items": data.get("total_items", 0),
                },
            )

            if len(items) < page_size:
                break

            if items:
                after = items[-1].get("token")
            else:
                break

        return all_items

    def get_form_title(self) -> str:
        """Fetch the form title for metadata."""
        if not self.api_token or not self.form_id:
            return ""
        try:
            resp = requests.get(
                f"{self.base_url}/forms/{self.form_id}",
                headers=self._headers(),
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json().get("title", "")
        except Exception:
            return ""
