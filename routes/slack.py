"""Slack interaction and internal feedback HTTP handlers."""

import logging

from integrations.databricks import DatabricksError, store_recovery_feedback
from integrations.google_cloud import (
    GoogleCloudConfigurationError,
    verify_pubsub_push,
)
from integrations.slack import (
    SlackError,
    decode_slack_feedback_push,
    parse_slack_interaction,
    publish_slack_feedback,
    verify_slack_request,
)


def receive_slack_interaction(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    raw_body = request.get_data(cache=True)
    if not verify_slack_request(
        raw_body,
        request.headers.get("X-Slack-Request-Timestamp"),
        request.headers.get("X-Slack-Signature"),
    ):
        return {"error": "Invalid Slack signature."}, 401
    try:
        feedback = parse_slack_interaction(raw_body)
        publish_slack_feedback(feedback)
    except SlackError as exc:
        return {"error": str(exc)}, 400
    except Exception:
        logging.exception("Could not queue Slack feedback")
        return {"error": "Could not queue feedback."}, 503
    return {
        "response_type": "ephemeral",
        "text": f"Feedback recorded: {feedback['feedback']}",
    }, 200


def process_slack_feedback(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    try:
        if not verify_pubsub_push(request):
            return {"error": "Invalid Pub/Sub identity."}, 401
        feedback = decode_slack_feedback_push(request.get_json(silent=True))
        store_recovery_feedback(feedback)
    except SlackError as exc:
        return {"error": str(exc)}, 400
    except GoogleCloudConfigurationError as exc:
        logging.exception("Pub/Sub identity configuration is invalid")
        return {"error": str(exc)}, 503
    except DatabricksError:
        logging.exception("Could not store Slack feedback")
        return {"error": "Could not store feedback."}, 503
    return "", 204
