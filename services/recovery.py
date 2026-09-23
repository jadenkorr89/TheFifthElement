"""Scheduled dry-run recovery decision worker."""
import hashlib
import json
import logging
import os

from google.genai import types
from integrations.gemini import create_client
from integrations.databricks import (
    list_good_recovery_examples,
    list_unprocessed_recovery_candidates,
    store_recovery_decision,
    update_recovery_decision_slack_message,
)
from integrations.slack import post_recovery_decision, post_recovery_failure
from services.settings import get_recovery_settings


class RecoveryError(Exception):
    pass


ALLOWED_ACTIONS = {"NO_ACTION", "REMINDER", "INCENTIVE", "HUMAN_REVIEW"}


def _sanitized_snapshot(candidate):
    try:
        payload = json.loads(candidate.get("latest_payload_json") or "{}")
    except json.JSONDecodeError:
        payload = {}
    lines = payload.get("line_items") or payload.get("items") or []
    items = []
    for line in lines[:20]:
        if isinstance(line, dict):
            items.append(
                {
                    "title": line.get("title") or line.get("presentment_title"),
                    "variant_title": line.get("variant_title")
                    or line.get("presentment_variant_title"),
                    "quantity": line.get("quantity"),
                    "price": line.get("price") or line.get("variant_price"),
                    "sku": line.get("sku"),
                }
            )
    return {
        "currency": candidate.get("currency"),
        "total_price": candidate.get("total_price"),
        "item_count": candidate.get("item_count"),
        "first_seen_at": candidate.get("first_seen_at"),
        "last_seen_at": candidate.get("last_seen_at"),
        "latest_topic": candidate.get("latest_topic"),
        "items": items,
    }


def _decide(client, model, snapshot, settings, good_examples):
    prompt = (
        f"{settings.cart_recovery_main_prompt}\n"
        f"Remaining budget: {settings.cart_recovery_budget}"
    )
    if good_examples:
        prompt += (
            "\n\nPrevious recovery decisions that received GOOD human feedback. "
            "Use these as guidance, not hard rules; evaluate the current cart independently:\n"
            + json.dumps(good_examples, ensure_ascii=False)
        )
    prompt += "\n\nSanitized cart:\n" + json.dumps(snapshot, ensure_ascii=False)
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )
    except Exception as exc:
        raise RecoveryError(
            f"Gemini request failed ({type(exc).__name__}): {str(exc)[:800]}"
        ) from exc
    try:
        result = json.loads(response.text or "")
    except json.JSONDecodeError as exc:
        raise RecoveryError("Gemini returned invalid decision JSON.") from exc
    if not isinstance(result, dict):
        raise RecoveryError("Gemini returned a non-object decision.")
    action = str(result.get("recommended_action", ""))
    if action not in ALLOWED_ACTIONS:
        raise RecoveryError("Gemini returned an unsupported recovery action.")
    reason = str(result.get("reason", "")).strip()[:300]
    if not reason:
        raise RecoveryError("Gemini returned an empty decision reason.")
    return {
        "recommended_action": action,
        "reason": reason,
        "message_subject": str(result.get("message_subject", "")).strip()[:120],
        "message_body": str(result.get("message_body", "")).strip()[:800],
    }


def run_recovery_worker():
    model = os.environ.get("GEMINI_MODEL")
    if not model:
        raise RecoveryError("GEMINI_MODEL is not configured.")
    settings = get_recovery_settings()
    good_examples = list_good_recovery_examples()
    candidates = list_unprocessed_recovery_candidates(
        limit=int(os.environ.get("RECOVERY_BATCH_SIZE", "5"))
    )
    processed = []
    with create_client() as client:
        for candidate in candidates:
            snapshot = _sanitized_snapshot(candidate)
            try:
                result = _decide(client, model, snapshot, settings, good_examples)
            except Exception as exc:
                context = {
                    "model": model,
                    "shop_domain": candidate.get("shop_domain"),
                    "state_token": candidate.get("state_token"),
                }
                logging.exception(
                    "Recovery decision failed for shop=%s state_token=%s model=%s",
                    context["shop_domain"],
                    context["state_token"],
                    model,
                )
                try:
                    post_recovery_failure(str(exc), context)
                    setattr(exc, "slack_notified", True)
                except Exception:
                    logging.exception(
                        "Could not send recovery failure notification to Slack"
                    )
                if isinstance(exc, RecoveryError):
                    raise
                raise RecoveryError(
                    f"Recovery decision failed ({type(exc).__name__}): {str(exc)[:800]}"
                ) from exc

            decision_id = hashlib.sha256(
                f"{candidate['shop_domain']}:{candidate['state_token']}".encode()
            ).hexdigest()
            decision = {
                **result,
                "decision_id": decision_id,
                "shop_domain": candidate["shop_domain"],
                "state_token": candidate["state_token"],
                "model": model,
                "input_snapshot_json": json.dumps(
                    snapshot, ensure_ascii=False, separators=(",", ":")
                ),
            }
            store_recovery_decision(decision)
            slack_message = post_recovery_decision(decision)
            update_recovery_decision_slack_message(
                decision_id,
                slack_message.get("channel"),
                slack_message.get("ts"),
            )
            processed.append(
                {
                    "decision_id": decision_id,
                    "recommended_action": result["recommended_action"],
                }
            )
    return {"ok": True, "dry_run": True, "count": len(processed), "processed": processed}
