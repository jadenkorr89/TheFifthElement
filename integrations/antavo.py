"""Antavo API signing plus reward and event clients."""
import hashlib
import hmac
import json
import math
import os
import re
from datetime import datetime, timezone

import requests


class AntavoError(Exception):
    pass


class AntavoAlreadyApplied(AntavoError):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"Antavo event already applied ({code}): {message}")


def _antavo_date():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _sha256_hex(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hmac_sha256(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _config():
    stack = os.environ.get("ANTAVO_STACK", "")
    api_key = os.environ.get("ANTAVO_API_KEY", "")
    secret = os.environ.get("ANTAVO_API_SECRET", "")
    if not re.fullmatch(r"[a-z0-9-]+", stack) or not api_key or not secret:
        raise AntavoError("Set ANTAVO_STACK, ANTAVO_API_KEY and ANTAVO_API_SECRET.")
    return stack, api_key, secret


def sign_antavo_request(method, stack, uri, parameters, api_key, api_secret, payload=None):
    host = f"api.{stack}.antavo.com"
    date = _antavo_date()
    date_part = date.split("T")[0]
    is_body_method = method.lower() in ("post", "put", "patch")
    body_str = json.dumps(payload) if is_body_method and payload is not None else ""
    sorted_parameters = "&".join(sorted(parameters.split("&"))) if parameters else ""
    canonical_headers = f"date:{date}\nhost:{host}\n"
    canonical_request = (
        f"{method.upper()}\n{uri}\n{sorted_parameters}\n"
        f"{canonical_headers}\ndate;host\n{_sha256_hex(body_str)}"
    )
    scope = f"{date_part}/{stack}/api/antavo_request"
    string_to_sign = (
        f"ANTAVO-HMAC-SHA256\n{date}\n{scope}\n{_sha256_hex(canonical_request)}"
    )
    key = _hmac_sha256(("ANTAVO" + api_secret).encode("utf-8"), date_part)
    for component in (stack, "api", "antavo_request"):
        key = _hmac_sha256(key, component)
    signature = hmac.new(
        key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    headers = {
        "date": date,
        "Authorization": (
            "ANTAVO-HMAC-SHA256 "
            f"Credential={api_key}/{scope}, SignedHeaders=date;host, Signature={signature}"
        ),
    }
    if is_body_method:
        headers["Content-Type"] = "application/json"
    url = f"https://{host}{uri}"
    if sorted_parameters:
        url += "?" + sorted_parameters
    return url, headers, body_str


def _request(method, uri, payload=None):
    stack, api_key, secret = _config()
    url, headers, body = sign_antavo_request(
        method, stack, uri, "", api_key, secret, payload=payload
    )
    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            data=body if payload is not None else None,
            timeout=(5, 20),
            allow_redirects=False,
        )
    except requests.RequestException:
        raise AntavoError("Antavo connection failed or timed out.") from None

    if not 200 <= response.status_code < 300:
        error_code = None
        error_message = ""
        try:
            error_payload = response.json()
            error = error_payload.get("error", {}) if isinstance(error_payload, dict) else {}
            error_code = error.get("code")
            error_message = str(error.get("message") or "")
        except ValueError:
            pass

        if error_code in {5003, 112101}:
            raise AntavoAlreadyApplied(error_code, error_message)

        safe_detail = response.text.strip().replace("\n", " ")[:800]
        detail = f" Response: {safe_detail}" if safe_detail else ""
        raise AntavoError(
            f"Antavo returned HTTP {response.status_code}.{detail}"
        )
    if not response.content:
        return {}
    try:
        data = response.json()
    except ValueError:
        raise AntavoError("Antavo returned a non-JSON response.") from None
    if not isinstance(data, (dict, list)):
        raise AntavoError("Antavo returned an unexpected JSON value.")
    return data


def fetch_rewards():
    # Preserve the actual envelope: the docs show an object despite describing a list.
    return _request("GET", "/entities/rewards/reward")


def _customer_id(value):
    if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value or "")):
        raise AntavoError("customer_id must be a Shopify numeric customer ID.")
    return str(value)


def _reward_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._~-]+", value):
        raise AntavoError("reward_id contains unsupported characters.")
    return value


def customer_get(customer_id):
    return _request("GET", f"/customers/{_customer_id(customer_id)}")


def customer_give_reward(customer_id, reward_id, points=None):
    uri = f"/customers/{_customer_id(customer_id)}/activities/rewards/{_reward_id(reward_id)}/claim"
    body = {}
    if points is not None:
        if isinstance(points, bool) or not isinstance(points, (int, float)) or not math.isfinite(points) or points < 0:
            raise AntavoError("points must be a nonnegative finite number.")
        body["points"] = points
    return _request("POST", uri, body)


AI_ACTIONS = frozenset({"prime_message", "give_points", "double_points"})


def customer_custom_action(customer_id, ai_action, ai_message="", ai_points=0):
    customer = _customer_id(customer_id)
    if ai_action not in AI_ACTIONS:
        raise AntavoError("ai_action must be prime_message, give_points or double_points.")
    if not isinstance(ai_message, str) or len(ai_message) > 4000:
        raise AntavoError("ai_message must be text of at most 4000 characters.")
    if ai_action == "prime_message" and not ai_message.strip():
        raise AntavoError("prime_message requires a nonempty ai_message.")
    if isinstance(ai_points, bool) or not isinstance(ai_points, int) or ai_points < 0:
        raise AntavoError("ai_points must be a nonnegative integer.")
    if ai_action == "give_points" and ai_points == 0:
        raise AntavoError("give_points requires a positive ai_points value.")
    return _request(
        "POST", "/events",
        {"customer": customer, "action": "ai_action", "data": {
            "ai_action": ai_action, "ai_message": ai_message, "ai_points": ai_points,
        }},
    )


def send_event(customer, action, data):
    if customer is None or str(customer).strip() == "":
        raise AntavoError("Antavo event customer is required.")
    if not action:
        raise AntavoError("Antavo event action is required.")
    return _request(
        "POST",
        "/events",
        {
            "customer": str(customer),
            "action": action,
            "data": data or {},
        },
    )


def send_opt_in(customer_id, email="", first_name="", last_name=""):
    return send_event(
        customer_id,
        "opt_in",
        {
            "email": email or "",
            "first_name": first_name or "",
            "last_name": last_name or "",
        },
    )


def send_checkout(customer_id, transaction_id, total, items, currency=None):
    data = {
        "transaction_id": str(transaction_id),
        "total": float(total),
        "items": items,
    }
    if currency:
        data["currency"] = str(currency)
    return send_event(customer_id, "checkout", data)
