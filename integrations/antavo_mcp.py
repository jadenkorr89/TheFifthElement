"""Authenticated Antavo Management MCP transport and short-lived token cache."""

import asyncio
import os
import time
import threading
from contextlib import asynccontextmanager

import httpx
import requests
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


TOKEN_URL = "https://api.hackathon.antavo.com/v1/auth/token"
MCP_URL = "https://management-api-mcp.hackathon.antavo.com"
REFRESH_MARGIN_SECONDS = 300
_token = None
_expires_at = 0.0
_lock = threading.Lock()


class AntavoMCPError(Exception):
    pass


async def access_token(force=False):
    return await asyncio.to_thread(_cached_token, force)


def _cached_token(force=False):
    global _token, _expires_at
    if not force and _token and time.monotonic() < _expires_at - REFRESH_MARGIN_SECONDS:
        return _token
    with _lock:
        if not force and _token and time.monotonic() < _expires_at - REFRESH_MARGIN_SECONDS:
            return _token
        credential = os.environ.get("ANTAVO_MCP_BASIC_AUTH", "").strip()
        if not credential:
            raise AntavoMCPError("ANTAVO_MCP_BASIC_AUTH is not configured.")
        try:
            response = requests.post(
                TOKEN_URL,
                headers={"Authorization": f"Basic {credential}"},
                json={"grant_type": "client_credentials", "scope": "management_api_mcp.all"},
                timeout=(5, 20),
                allow_redirects=False,
            )
            response.raise_for_status()
            payload = response.json()
            token = payload["access_token"]
            lifetime = int(payload["expires_in"])
            if not isinstance(token, str) or not token or lifetime <= REFRESH_MARGIN_SECONDS:
                raise ValueError("Invalid token response")
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            raise AntavoMCPError(f"Antavo token request failed ({type(exc).__name__}).") from None
        _token = token
        _expires_at = time.monotonic() + lifetime
        return token


@asynccontextmanager
async def session(force_refresh=False):
    token = await access_token(force=force_refresh)
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx.Timeout(60, connect=10),
        follow_redirects=False,
    ) as client:
        async with streamable_http_client(MCP_URL, http_client=client) as (read, write, _):
            async with ClientSession(read, write) as mcp:
                await mcp.initialize()
                yield mcp


def is_unauthorized(exc):
    """The transport may wrap HTTP status errors in an exception group."""
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 401:
        return True
    return any(is_unauthorized(child) for child in getattr(exc, "exceptions", ()))
