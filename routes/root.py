"""Health, manual agent, and diagnostics routes."""

import logging
import uuid

from google.genai.errors import APIError

from integrations.databricks import DatabricksError, count_shopify_events
from integrations.gemini import GeminiConfigurationError
from services.leeloo import DEFAULT_PROMPT, LeelooError, run_leeloo


def _error_details(exc):
    """Summarize nested transport errors without logging request or response bodies."""
    if isinstance(exc, BaseExceptionGroup):
        return [detail for child in exc.exceptions for detail in _error_details(child)]
    detail = {"type": type(exc).__name__}
    if isinstance(exc, APIError):
        detail["upstream"] = "gemini"
        detail["status"] = exc.code
    return [detail]


def handle_root(request):
    if request.method == "GET":
        return {
            "ok": True,
            "service": "the-fifth-element",
            "source": "github",
            "deployment_marker": "live-recovery-actions-1",
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
        return run_leeloo(prompt), 200
    except (ValueError, GeminiConfigurationError, LeelooError) as exc:
        return {"error": str(exc)}, 500
    except Exception as exc:
        error_id = uuid.uuid4().hex[:12]
        logging.error(
            "Agent request failed error_id=%s details=%s",
            error_id, _error_details(exc), exc_info=True,
        )
        return {"error": "Agent request failed; see logs.", "error_id": error_id}, 502


def _test_databricks():
    try:
        return {
            "ok": True,
            "shopify_event_count": count_shopify_events(),
            "message": "Databricks connection and Shopify event table succeeded.",
        }, 200
    except DatabricksError as exc:
        return {"error": str(exc)}, 502
