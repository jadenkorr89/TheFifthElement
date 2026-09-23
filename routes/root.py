"""Health, manual agent, and diagnostics routes."""

import logging

from integrations.databricks import DatabricksError, count_shopify_events
from integrations.gemini import GeminiConfigurationError
from services.rewards import DEFAULT_PROMPT, run_rewards_agent


def handle_root(request):
    if request.method == "GET":
        return {
            "ok": True,
            "service": "the-fifth-element",
            "source": "github",
            "deployment_marker": "slack-feedback-learning-1",
        }, 200
    if request.method != "POST":
        return {
            "error": "Use GET for health or POST with a JSON object containing prompt."
        }, 405, {"Allow": "GET, POST"}

    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return {"error": "Expected a JSON object."}, 400
    if body.get("action") == "test_databricks":
        return _test_databricks()

    prompt = body.get("prompt", DEFAULT_PROMPT)
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 10000:
        return {
            "error": "prompt must be a nonempty string of at most 10000 characters."
        }, 400
    try:
        return run_rewards_agent(prompt)
    except (ValueError, GeminiConfigurationError) as exc:
        return {"error": str(exc)}, 500
    except Exception:
        logging.exception("Agent request failed")
        return {"error": "Agent request failed; see logs."}, 502


def _test_databricks():
    try:
        return {
            "ok": True,
            "shopify_event_count": count_shopify_events(),
            "message": "Databricks connection and Shopify event table succeeded.",
        }, 200
    except DatabricksError as exc:
        return {"error": str(exc)}, 502
