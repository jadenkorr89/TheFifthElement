"""Typed runtime settings backed by Databricks."""

from dataclasses import dataclass

from integrations.databricks import load_settings


class SettingsError(Exception):
    pass


DEFAULT_CART_RECOVERY_MAIN_PROMPT = """Evaluate one abandoned ecommerce checkout for a dry-run PoC.
Return only a JSON object with four string fields:
recommended_action, reason, message_subject, message_body.

recommended_action must be exactly one of NO_ACTION, REMINDER, INCENTIVE,
or HUMAN_REVIEW.

Do not invent discounts, coupon codes, reward values, stock, urgency, or
customer facts. INCENTIVE only means a verified incentive should be considered
later. Prefer REMINDER for an ordinary cart. Use HUMAN_REVIEW for malformed or
contradictory input. Use {{ recovery_url }} as the link placeholder. Nothing is
being sent. Keep reason under 300 characters, subject under 120, body under 800."""


DEFAULT_SETTINGS = {
    "cart_recovery_budget": {
        "type": "float",
        "value": "2000",
    },
    "cart_recovery_main_prompt": {
        "type": "string",
        "value": DEFAULT_CART_RECOVERY_MAIN_PROMPT,
    },
}


@dataclass(frozen=True)
class RecoverySettings:
    cart_recovery_budget: float
    cart_recovery_main_prompt: str


def _get_typed_value(rows, key, expected_type):
    row = rows.get(key)
    if not row:
        raise SettingsError(f"Missing runtime setting: {key}.")
    if row.get("type") != expected_type:
        raise SettingsError(
            f"Runtime setting {key} must have type {expected_type}, "
            f"got {row.get('type') or 'empty'}."
        )

    value = row.get("value", "")
    if expected_type == "float":
        try:
            return float(value)
        except ValueError as exc:
            raise SettingsError(
                f"Runtime setting {key} must contain a valid float."
            ) from exc
    if expected_type == "string":
        return value
    raise SettingsError(f"Unsupported runtime setting type: {expected_type}.")


def get_recovery_settings():
    rows = load_settings(DEFAULT_SETTINGS)
    return RecoverySettings(
        cart_recovery_budget=_get_typed_value(
            rows, "cart_recovery_budget", "float"
        ),
        cart_recovery_main_prompt=_get_typed_value(
            rows, "cart_recovery_main_prompt", "string"
        ),
    )
