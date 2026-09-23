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
    open_recovery_feedback_modal,
    parse_recovery_feedback_submission,
    parse_slack_payload,
    publish_slack_feedback,
    update_recovery_decision_message,
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
        payload = parse_slack_payload(raw_body)
        payload_type = payload.get("type")
        if payload_type == "block_actions":
            open_recovery_feedback_modal(payload)
            return "", 200
        if payload_type == "view_submission":
            feedback = parse_recovery_feedback_submission(payload)
            publish_slack_feedback(feedback)
            return "", 200
        return {"error": "Unsupported Slack interaction."}, 400
    except SlackError as exc:
        logging.exception("Slack interaction failed")
        return {"error": str(exc)}, 400
    except Exception:
        logging.exception("Could not handle Slack interaction")
        return {"error": "Could not handle Slack interaction."}, 503


def process_slack_feedback(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    try:
        if not verify_pubsub_push(request):
            return {"error": "Invalid Pub/Sub identity."}, 401
        feedback = decode_slack_feedback_push(request.get_json(silent=True))
        store_recovery_feedback(feedback)
        update_recovery_decision_message(feedback)
    except SlackError as exc:
        logging.exception("Could not apply Slack feedback")
        return {"error": str(exc)}, 503
    except GoogleCloudConfigurationError as exc:
        logging.exception("Pub/Sub identity configuration is invalid")
        return {"error": str(exc)}, 503
    except DatabricksError:
        logging.exception("Could not store Slack feedback")
        return {"error": "Could not store feedback."}, 503
    return "", 204
