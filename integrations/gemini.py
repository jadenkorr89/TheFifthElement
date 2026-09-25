"""Gemini client construction shared by application workflows."""

import os

from google import genai
from google.genai import types


class GeminiConfigurationError(Exception):
    pass


def create_client():
    backend = os.environ.get("GEMINI_BACKEND", "developer")
    options = types.HttpOptions(
        timeout=60000
        max_retries=4,      # Keep native exponential backoff active
        api_version="v1",
        headers={
            # Forces the request to process via standard paygo instead of demanding a PT subscription
            "X-Vertex-AI-LLM-Request-Type": "shared",
            # Signals the backend load-balancers to route this to the VIP priority queue
            "X-Vertex-AI-LLM-Shared-Request-Type": "priority"
        }
    if backend == "developer":
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            raise GeminiConfigurationError("GEMINI_API_KEY is not configured.")
        return genai.Client(vertexai=False, api_key=key, http_options=options)
    if backend == "vertex":
        project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not project:
            raise GeminiConfigurationError("GOOGLE_CLOUD_PROJECT is not configured.")
        return genai.Client(
            vertexai=True,
            project=project,
            location=os.environ.get("GOOGLE_CLOUD_LOCATION", "global"),
            http_options=options,
        )
    raise GeminiConfigurationError("GEMINI_BACKEND must be developer or vertex.")
