"""Shared Google Cloud identity checks for authenticated HTTP pushes."""

import os

from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token


class GoogleCloudConfigurationError(Exception):
    pass


def _verify_oidc_request(request, audience_env, service_account_env):
    audience = os.environ.get(audience_env, "")
    expected_email = os.environ.get(service_account_env, "")
    if not audience or not expected_email:
        raise GoogleCloudConfigurationError(
            f"{audience_env} and {service_account_env} must be configured."
        )
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    try:
        claims = id_token.verify_oauth2_token(
            auth[7:], google_auth_requests.Request(), audience=audience
        )
    except (ValueError, TypeError):
        return False
    return (
        claims.get("email") == expected_email
        and claims.get("email_verified") is True
    )


def verify_pubsub_push(request):
    return _verify_oidc_request(
        request, "PUBSUB_PUSH_AUDIENCE", "PUBSUB_PUSH_SERVICE_ACCOUNT"
    )


def verify_scheduler_request(request):
    return _verify_oidc_request(
        request, "SCHEDULER_AUDIENCE", "SCHEDULER_SERVICE_ACCOUNT"
    )
