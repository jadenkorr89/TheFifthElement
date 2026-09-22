"""Cloud Run entry point.

Google Cloud Build should load ``service`` from this module. The old
``rewards_agent`` name remains as a temporary deployment compatibility alias.
"""

import functions_framework

from http_api import dispatch_request


@functions_framework.http
def service(request):
    return dispatch_request(request)


# Remove after the Cloud Build _ENTRYPOINT substitution is changed to service.
rewards_agent = service
