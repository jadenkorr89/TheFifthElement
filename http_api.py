"""Top-level HTTP router for the single Cloud Run service."""

from routes.recovery import run_recovery_job
from routes.root import handle_root
from routes.shopify import process_shopify_event, receive_shopify_webhook
from routes.slack import process_slack_feedback, receive_slack_interaction


ROUTES = {
    "/webhooks/shopify": receive_shopify_webhook,
    "/internal/shopify/process": process_shopify_event,
    "/slack/interactions": receive_slack_interaction,
    "/internal/slack/process": process_slack_feedback,
    "/jobs/recovery/run": run_recovery_job,
}


def dispatch_request(request):
    path = request.path.rstrip("/") or "/"
    handler = ROUTES.get(path)
    if handler:
        return handler(request)
    if path == "/":
        return handle_root(request)
    return {"error": "Not found."}, 404
