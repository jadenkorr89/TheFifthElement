"""Scheduled dry-run recovery decision worker."""
import hashlib
import json
import logging
import os
import asyncio

from google.genai import types
from integrations.gemini import create_client
from integrations.antavo import AntavoError, customer_custom_action, customer_get, customer_give_reward
from integrations.antavo_mcp import session
from integrations.databricks import (
    list_good_recovery_examples,
    list_unprocessed_recovery_candidates,
    store_recovery_decision,
    update_recovery_action_status,
    update_recovery_decision_slack_message,
)
from integrations.slack import post_recovery_decision, post_recovery_failure
from services.settings import get_recovery_settings


class RecoveryError(Exception):
    pass


ALLOWED_ACTIONS = {"NO_ACTION", "PRIME_MESSAGE", "GIVE_REWARD", "GIVE_POINTS", "HUMAN_REVIEW"}


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


async def _available_rewards():
    async with session() as mcp:
        listing = await mcp.list_tools()
        tools = [
            tool for tool in listing.tools
            if "reward" in tool.name.lower()
            and any(word in tool.name.lower() for word in ("list", "search", "get"))
            and tool.annotations and tool.annotations.readOnlyHint is True
            and not (tool.inputSchema or {}).get("required")
        ]
        if not tools:
            raise RecoveryError("No zero-argument read-only rewards tool is available on Antavo MCP.")
        tool = next((t for t in tools if "list" in t.name.lower()), tools[0])
        result = await mcp.call_tool(tool.name, arguments={})
        if result.isError:
            raise RecoveryError("Antavo MCP rewards lookup failed.")
        value = result.model_dump(mode="json", exclude_none=True)
        serialized = json.dumps(value, ensure_ascii=False)
        if len(serialized) > 30000:
            raise RecoveryError("Antavo rewards catalog is too large.")
        return value


def _reward_ids(value):
    if isinstance(value, dict):
        ids = {str(value[key]) for key in ("id", "reward_id") if value.get(key) is not None}
        return ids | set().union(*(_reward_ids(v) for v in value.values()))
    if isinstance(value, list):
        return set().union(*(_reward_ids(v) for v in value))
    if isinstance(value, str) and value[:1] in ("{", "["):
        try:
            return _reward_ids(json.loads(value))
        except (ValueError, TypeError):
            pass
    return set()


def _decide(client, model, snapshot, settings, good_examples, rewards, customer, remaining_budget):
    prompt = (
        f"{settings.cart_recovery_main_prompt}\n"
        f"Remaining budget: {remaining_budget}"
    )
    if good_examples:
        prompt += (
            "\n\nPrevious recovery decisions that received GOOD human feedback. "
            "Use these as guidance, not hard rules; evaluate the current cart independently:\n"
            + json.dumps(good_examples, ensure_ascii=False)
        )
    prompt += (
        "\n\nThis run is LIVE. Earlier dry-run wording in the stored setting is obsolete. "
        "Choose exactly one action: NO_ACTION, PRIME_MESSAGE, GIVE_REWARD, "
        "GIVE_POINTS, HUMAN_REVIEW. Return JSON with recommended_action, reason, "
        "message_subject, message_body, reward_id, points. For GIVE_REWARD use "
        "only a reward ID in the Antavo catalog. For GIVE_POINTS use a positive "
        "integer within the remaining budget. PRIME_MESSAGE must contain a "
        "useful message. Use the checkout link placeholder {{ recovery_url }}. "
        "Antavo sends the email when prime_message is recorded; do not invent "
        "delivery confirmation. Do not promise an incentive unless it is actually "
        "available. If customer details are insufficient, choose HUMAN_REVIEW."
        "\n\nAntavo rewards:\n" + json.dumps(rewards, ensure_ascii=False)
        + "\n\nCustomer:\n" + json.dumps(customer, ensure_ascii=False)
        + "\n\nSanitized cart:\n" + json.dumps(snapshot, ensure_ascii=False)
    )
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
        "reward_id": str(result.get("reward_id") or "").strip(),
        "points": result.get("points", 0),
    }


def _execute_action(result, customer_id, rewards, remaining_budget):
    action = result["recommended_action"]
    if action in {"NO_ACTION", "HUMAN_REVIEW"}:
        return {"status": "NO_ACTION", "steps": []}, 0
    if not customer_id or not str(customer_id).isdigit():
        return {"status": "FAILED", "steps": [{"action": action, "ok": False,
                "detail": "Checkout has no numeric customer ID."}]}, 0

    message = result["message_body"]
    if not message or "{{ recovery_url }}" not in message:
        return {"status": "FAILED", "steps": [{"action": action, "ok": False,
                "detail": "Message is empty or missing the recovery URL placeholder."}]}, 0
    points = result["points"]
    if action == "GIVE_POINTS" and (
        isinstance(points, bool) or not isinstance(points, int)
        or points <= 0 or points > remaining_budget
    ):
        return {"status": "FAILED", "steps": [{"action": action, "ok": False,
                "detail": "Points exceed the available budget or are invalid."}]}, 0
    if action == "GIVE_REWARD" and result["reward_id"] not in _reward_ids(rewards):
        return {"status": "FAILED", "steps": [{"action": action, "ok": False,
                "detail": "Reward ID was not verified in the MCP catalog."}]}, 0

    steps = []
    try:
        customer_custom_action(customer_id, "prime_message", message)
        steps.append({"action": "prime_message", "ok": True,
                      "detail": "Antavo accepted the event; delivery is unconfirmed."})
    except AntavoError as exc:
        return {"status": "FAILED", "steps": [{"action": "prime_message", "ok": False,
                "detail": str(exc)[:300]}]}, 0

    try:
        if action == "GIVE_REWARD":
            customer_give_reward(customer_id, result["reward_id"])
            steps.append({"action": "give_reward", "ok": True,
                          "detail": result["reward_id"]})
        elif action == "GIVE_POINTS":
            customer_custom_action(customer_id, "give_points", "", points)
            steps.append({"action": "give_points", "ok": True,
                          "detail": str(points)})
    except AntavoError as exc:
        steps.append({"action": action.lower(), "ok": False, "detail": str(exc)[:300]})
        return {"status": "PARTIAL", "steps": steps}, 0
    return {"status": "EXECUTED", "steps": steps}, points if action == "GIVE_POINTS" else 0


def run_recovery_worker():
    model = os.environ.get("GEMINI_MODEL")
    if not model:
        raise RecoveryError("GEMINI_MODEL is not configured.")
    settings = get_recovery_settings()
    good_examples = list_good_recovery_examples()
    candidates = list_unprocessed_recovery_candidates(
        limit=int(os.environ.get("RECOVERY_BATCH_SIZE", "5"))
    )
    if not candidates:
        return {"ok": True, "count": 0, "processed": []}
    rewards = asyncio.run(_available_rewards())
    processed = []
    remaining_budget = int(settings.cart_recovery_budget)
    with create_client() as client:
        for candidate in candidates:
            snapshot = _sanitized_snapshot(candidate)
            customer_id = candidate.get("customer_id")
            customer = {}
            if customer_id:
                try:
                    customer = customer_get(customer_id)
                except AntavoError:
                    logging.exception("Could not fetch recovery customer state_token=%s",
                                      candidate["state_token"])
            try:
                result = _decide(client, model, snapshot, settings, good_examples,
                                 rewards, customer, remaining_budget)
            except Exception as exc:
                context = {"model": model, "shop_domain": candidate.get("shop_domain"),
                           "state_token": candidate.get("state_token")}
                logging.exception("Recovery decision failed for %s", context)
                try:
                    post_recovery_failure(str(exc), context)
                    setattr(exc, "slack_notified", True)
                except Exception:
                    logging.exception("Could not send recovery failure notification")
                raise

            decision_id = hashlib.sha256(
                f"{candidate['shop_domain']}:{candidate['state_token']}".encode()
            ).hexdigest()
            decision = {
                **result, "decision_id": decision_id,
                "shop_domain": candidate["shop_domain"],
                "state_token": candidate["state_token"], "model": model,
                "input_snapshot_json": json.dumps(snapshot, ensure_ascii=False,
                                                  separators=(",", ":")),
            }
            # Durable record before any side effect; scheduler retries cannot replay it.
            store_recovery_decision(decision)
            execution, spent = _execute_action(result, customer_id, rewards,
                                               remaining_budget)
            remaining_budget -= spent
            decision["execution"] = execution
            try:
                update_recovery_action_status(decision_id, execution["status"])
            except Exception:
                logging.exception("Action result could not be recorded; do not replay %s",
                                  decision_id)
            try:
                slack_message = post_recovery_decision(decision)
                update_recovery_decision_slack_message(
                    decision_id, slack_message.get("channel"), slack_message.get("ts")
                )
            except Exception:
                logging.exception("Action completed but Slack reporting failed for %s",
                                  decision_id)
                raise
            processed.append({"decision_id": decision_id,
                              "recommended_action": result["recommended_action"],
                              "execution": execution})
    return {"ok": True, "count": len(processed), "processed": processed}
