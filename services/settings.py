"""Typed runtime settings backed by Databricks."""

from dataclasses import dataclass

from integrations.databricks import load_settings


class SettingsError(Exception):
    pass


DEFAULT_CART_RECOVERY_MAIN_PROMPT = """Evaluate an abandoned checkout and choose a single recovery action.
Use the live Antavo customer and rewards catalog supplied with the cart.
Prefer a useful prime_message for an ordinary cart. Offer a reward or points
only when warranted and verified. Never invent reward IDs, balances, prices,
discounts, urgency, or customer facts. Keep the reason short. Antavo handles
email delivery after the prime_message event; its API acceptance is not proof
of delivery. Choose HUMAN_REVIEW if the input is contradictory or unsafe."""


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
