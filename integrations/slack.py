"""Slack decision cards, signed interactions, and feedback transport."""
import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import parse_qs

import requests
from google.cloud import pubsub_v1


class SlackError(Exception):
    pass


def _decision_blocks(decision, feedback=None):
    action = decision["recommended_action"]
    reason = decision["reason"]
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Recovery action: {action}"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Reasoning*\n{reason}"}},
    ]
    if decision.get("message_body"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": "*Message passed to Antavo*\\n" + str(decision["message_body"])[:800]}})
    execution = decision.get("execution")
    if execution:
        steps = execution.get("steps", [])
        summary = "\n".join(
            f"• {step['action']}: {'accepted' if step['ok'] else 'failed'}"
            + (f" — {str(step.get('detail', ''))[:300]}" if step.get("detail") else "")
            for step in steps
        ) or "No Antavo action taken."
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": f"*Antavo outcome: {execution['status']}*\n{summary}"}})
    if feedback:
        rating = feedback.get("rating", "")
        text = feedback.get("text", "")
        user_name = feedback.get("user_name", "")
        summary = f"*Feedback:* {rating}"
        if user_name:
            summary += f" — {user_name}"
        if text:
            summary += f"\n{text}"
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": summary}}
        )
    else:
        button_value = json.dumps(
            {
                "decision_id": decision["decision_id"],
                "recommended_action": action,
                "reason": reason,
                "execution": execution,
            },
            separators=(",", ":"),
        )
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "action_id": "recovery_feedback_open",
                        "text": {"type": "plain_text", "text": "Give feedback"},
                        "style": "primary",
                        "value": button_value,
                    }
                ],
            }
        )
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Decision `{decision['decision_id'][:12]}`",
                }
            ],
        }
    )
    return blocks


def post_recovery_decision(decision):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL_ID", "")
    if not token or not channel:
        raise SlackError("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required.")
    action = decision["recommended_action"]
    blocks = _decision_blocks(decision)
    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": channel, "text": f"Recovery proposal: {action}", "blocks": blocks},
            timeout=(5, 15),
        )
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise SlackError("Slack message request failed.") from exc
    if response.status_code != 200 or not data.get("ok"):
        raise SlackError(f"Slack rejected the message: {data.get('error', response.status_code)}")
    return {"channel": data.get("channel"), "ts": data.get("ts")}


def verify_slack_request(raw_body, timestamp, signature):
    secret = os.environ.get("SLACK_SIGNING_SECRET", "")
    if not secret or not timestamp or not signature:
        return False
    try:
        if abs(time.time() - int(timestamp)) > 300:
            return False
    except ValueError:
        return False
    base = b"v0:" + timestamp.encode() + b":" + raw_body
    expected = "v0=" + hmac.new(
        secret.encode(), base, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def parse_slack_payload(raw_body):
    try:
        form = parse_qs(raw_body.decode("utf-8"), strict_parsing=True)
        payload = json.loads(form["payload"][0])
    except (UnicodeDecodeError, ValueError, KeyError, IndexError, TypeError) as exc:
        raise SlackError("Invalid Slack interaction payload.") from exc
    if not isinstance(payload, dict):
        raise SlackError("Invalid Slack interaction payload.")
    return payload


def open_recovery_feedback_modal(payload):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        raise SlackError("SLACK_BOT_TOKEN is required.")
    try:
        action = payload["actions"][0]
        if action.get("action_id") != "recovery_feedback_open":
            raise SlackError("Unsupported Slack action.")
        decision = json.loads(action.get("value") or "{}")
        decision_id = decision["decision_id"]
        recommended_action = decision["recommended_action"]
        reason = decision["reason"]
        execution = decision.get("execution")
        trigger_id = payload["trigger_id"]
        channel_id = payload["channel"]["id"]
        message_ts = payload["message"]["ts"]
    except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SlackError("Invalid recovery feedback action.") from exc

    private_metadata = json.dumps(
        {
            "decision_id": decision_id,
            "recommended_action": recommended_action,
            "reason": reason,
            "execution": execution,
            "channel_id": channel_id,
            "message_ts": message_ts,
        },
        separators=(",", ":"),
    )
    view = {
        "type": "modal",
        "callback_id": "recovery_feedback_submit",
        "private_metadata": private_metadata,
        "title": {"type": "plain_text", "text": "Recovery feedback"},
        "submit": {"type": "plain_text", "text": "Submit"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*AI choice:* {recommended_action}\n"
                        f"*Reasoning:* {reason}"
                    ),
                },
            },
            {
                "type": "input",
                "block_id": "feedback_rating",
                "label": {"type": "plain_text", "text": "Decision quality"},
                "element": {
                    "type": "radio_buttons",
                    "action_id": "rating",
                    "options": [
                        {
                            "text": {"type": "plain_text", "text": "GOOD"},
                            "value": "GOOD",
                        },
                        {
                            "text": {"type": "plain_text", "text": "BAD"},
                            "value": "BAD",
                        },
                    ],
                },
            },
            {
                "type": "input",
                "block_id": "feedback_text",
                "optional": True,
                "label": {"type": "plain_text", "text": "Further feedback"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "text",
                    "multiline": True,
                    "max_length": 1500,
                },
            },
        ],
    }
    try:
        response = requests.post(
            "https://slack.com/api/views.open",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"trigger_id": trigger_id, "view": view},
            timeout=(5, 15),
        )
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise SlackError("Slack modal request failed.") from exc
    if response.status_code != 200 or not data.get("ok"):
        raise SlackError(
            f"Slack rejected the modal: {data.get('error', response.status_code)}"
        )
    return True


def parse_recovery_feedback_submission(payload):
    try:
        if payload.get("type") != "view_submission":
            raise SlackError("Unsupported Slack interaction type.")
        view = payload["view"]
        if view.get("callback_id") != "recovery_feedback_submit":
            raise SlackError("Unsupported Slack modal submission.")
        metadata = json.loads(view.get("private_metadata") or "{}")
        values = view["state"]["values"]
        rating = values["feedback_rating"]["rating"]["selected_option"]["value"]
        feedback_text = values["feedback_text"]["text"].get("value") or ""
        user = payload.get("user", {})
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SlackError("Invalid recovery feedback submission.") from exc

    if rating not in {"GOOD", "BAD"}:
        raise SlackError("Feedback rating must be GOOD or BAD.")
    return {
        "decision_id": metadata.get("decision_id", ""),
        "recommended_action": metadata.get("recommended_action", ""),
        "reason": metadata.get("reason", ""),
        "execution": metadata.get("execution"),
        "rating": rating,
        "feedback_text": feedback_text.strip()[:1500],
        "user_id": user.get("id", ""),
        "user_name": user.get("username") or user.get("name", ""),
        "channel_id": metadata.get("channel_id", ""),
        "message_ts": metadata.get("message_ts", ""),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def update_recovery_decision_message(feedback):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    if not token:
        raise SlackError("SLACK_BOT_TOKEN is required.")
    channel = feedback.get("channel_id", "")
    ts = feedback.get("message_ts", "")
    if not channel or not ts:
        raise SlackError("Slack message location is missing from feedback.")

    decision = {
        "decision_id": feedback["decision_id"],
        "recommended_action": feedback.get("recommended_action", ""),
        "reason": feedback.get("reason", ""),
        "execution": feedback.get("execution"),
    }
    blocks = _decision_blocks(
        decision,
        {
            "rating": feedback.get("rating", ""),
            "text": feedback.get("feedback_text", ""),
            "user_name": feedback.get("user_name", ""),
        },
    )
    try:
        response = requests.post(
            "https://slack.com/api/chat.update",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "channel": channel,
                "ts": ts,
                "text": f"Recovery proposal: {decision['recommended_action']}",
                "blocks": blocks,
            },
            timeout=(5, 15),
        )
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise SlackError("Slack message update failed.") from exc
    if response.status_code != 200 or not data.get("ok"):
        raise SlackError(
            f"Slack rejected the message update: "
            f"{data.get('error', response.status_code)}"
        )
    return True


def publish_slack_feedback(feedback):
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    topic_id = os.environ.get("PUBSUB_SLACK_FEEDBACK_TOPIC_ID", "slack-recovery-feedback")
    if not project:
        raise SlackError("GOOGLE_CLOUD_PROJECT is required.")
    publisher = pubsub_v1.PublisherClient()
    future = publisher.publish(
        publisher.topic_path(project, topic_id),
        json.dumps(feedback, separators=(",", ":")).encode(),
        decision_id=feedback["decision_id"],
        feedback_rating=feedback["rating"],
    )
    return future.result(timeout=3)


def decode_slack_feedback_push(body):
    try:
        message = body["message"]
        feedback = json.loads(base64.b64decode(message["data"], validate=True))
        feedback["pubsub_message_id"] = message.get("messageId", "")
        return feedback
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SlackError("Invalid Slack feedback Pub/Sub message.") from exc


def post_recovery_failure(message, context=None):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL_ID", "")
    if not token or not channel:
        raise SlackError("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required.")

    context = context or {}
    details = []
    for key in ("model", "shop_domain", "state_token"):
        value = context.get(key)
        if value:
            details.append(f"*{key}:* `{str(value)[:160]}`")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Recovery worker failed"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error*\n```{str(message)[:1200]}```",
            },
        },
    ]
    if details:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(details)},
            }
        )

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "channel": channel,
                "text": f"Recovery worker failed: {str(message)[:300]}",
                "blocks": blocks,
            },
            timeout=(5, 15),
        )
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise SlackError("Slack failure notification request failed.") from exc
    if response.status_code != 200 or not data.get("ok"):
        raise SlackError(
            f"Slack rejected the failure notification: "
            f"{data.get('error', response.status_code)}"
        )
    return {"channel": data.get("channel"), "ts": data.get("ts")}
