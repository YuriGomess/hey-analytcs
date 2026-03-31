import hashlib
import hmac
from datetime import datetime
from typing import Any


def validate_signature(payload: bytes, signature: str, secret: str) -> bool:
    if not signature or not signature.startswith("sha256="):
        return False
    provided_hash = signature.split("=", 1)[1]
    expected_hash = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(provided_hash, expected_hash)


def parse_response(data: dict[str, Any]) -> dict[str, Any]:
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
