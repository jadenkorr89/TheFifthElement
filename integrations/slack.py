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


def post_recovery_decision(decision):
    token = os.environ.get("SLACK_BOT_TOKEN", "")
    channel = os.environ.get("SLACK_CHANNEL_ID", "")
    if not token or not channel:
        raise SlackError("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID are required.")
    action = decision["recommended_action"]
    reason = decision["reason"]
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Recovery proposal: {action}"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Reasoning*\n{reason}"}},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "action_id": "recovery_approve",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "style": "primary",
                    "value": decision["decision_id"],
                },
                {
                    "type": "button",
                    "action_id": "recovery_reject",
                    "text": {"type": "plain_text", "text": "Reject"},
                    "style": "danger",
                    "value": decision["decision_id"],
                },
                {
                    "type": "button",
                    "action_id": "recovery_needs_work",
                    "text": {"type": "plain_text", "text": "Needs work"},
                    "value": decision["decision_id"],
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Dry run • Decision `{decision['decision_id'][:12]}`"}
            ],
        },
    ]
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


def parse_slack_interaction(raw_body):
    try:
        form = parse_qs(raw_body.decode("utf-8"), strict_parsing=True)
        payload = json.loads(form["payload"][0])
        action = payload["actions"][0]
    except (UnicodeDecodeError, ValueError, KeyError, IndexError, TypeError) as exc:
        raise SlackError("Invalid Slack interaction payload.") from exc
    mapping = {
        "recovery_approve": "APPROVED",
        "recovery_reject": "REJECTED",
        "recovery_needs_work": "NEEDS_WORK",
    }
    feedback = mapping.get(action.get("action_id"))
    if not feedback:
        raise SlackError("Unsupported Slack action.")
    return {
        "decision_id": action.get("value", ""),
        "feedback": feedback,
        "user_id": payload.get("user", {}).get("id", ""),
        "user_name": payload.get("user", {}).get("username")
        or payload.get("user", {}).get("name", ""),
        "channel_id": payload.get("channel", {}).get("id", ""),
        "message_ts": payload.get("message", {}).get("ts", ""),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


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
        feedback=feedback["feedback"],
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
