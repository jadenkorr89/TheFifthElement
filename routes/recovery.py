"""Scheduled recovery worker HTTP handler."""

import logging

from integrations.databricks import DatabricksError
from integrations.google_cloud import (
    GoogleCloudConfigurationError,
    verify_scheduler_request,
)
from integrations.gemini import GeminiConfigurationError
from integrations.slack import SlackError, post_recovery_failure
from services.recovery import (
    RecoveryError,
    run_recovery_worker,
)
from services.settings import SettingsError


def _notify_failure_safely(exc):
    if getattr(exc, "slack_notified", False):
        return
    try:
        post_recovery_failure(
            f"{type(exc).__name__}: {str(exc)[:1000]}",
            {"scope": "recovery_job"},
        )
    except Exception:
        logging.exception("Could not send recovery job failure notification to Slack")


def run_recovery_job(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    try:
        if not verify_scheduler_request(request):
            return {"error": "Invalid scheduler identity."}, 401
        return run_recovery_worker(), 200
    except (
        RecoveryError,
        SettingsError,
        DatabricksError,
        SlackError,
        GoogleCloudConfigurationError,
        GeminiConfigurationError,
    ) as exc:
        logging.exception(
            "Recovery worker failed: %s: %s",
            type(exc).__name__,
            str(exc),
        )
        _notify_failure_safely(exc)
        return {
            "error": str(exc),
            "error_type": type(exc).__name__,
            "retryable": True,
        }, 503
    except Exception as exc:
        logging.exception(
            "Recovery worker failed unexpectedly: %s: %s",
            type(exc).__name__,
            str(exc),
        )
        _notify_failure_safely(exc)
        return {
            "error": "Recovery worker failed unexpectedly; see logs.",
            "error_type": type(exc).__name__,
            "retryable": True,
        }, 503
