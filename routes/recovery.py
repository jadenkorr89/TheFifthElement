"""Scheduled recovery worker HTTP handler."""

import logging

from integrations.databricks import DatabricksError
from integrations.google_cloud import (
    GoogleCloudConfigurationError,
    verify_scheduler_request,
)
from integrations.gemini import GeminiConfigurationError
from integrations.slack import SlackError
from services.recovery import (
    RecoveryError,
    run_recovery_worker,
)
from services.settings import SettingsError


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
        logging.exception("Recovery worker failed")
        return {"error": str(exc)}, 503
    except Exception:
        logging.exception("Recovery worker failed unexpectedly")
        return {"error": "Recovery worker failed; see logs."}, 503
