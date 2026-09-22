"""Shopify webhook verification and Pub/Sub transport."""
import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

from google.auth.transport import requests as google_auth_requests
from google.cloud import pubsub_v1
from google.oauth2 import id_token


class ShopifyWebhookError(Exception):
    pass


def verify_shopify_webhook(raw_body, signature):
    secret = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
    if not secret:
        raise ShopifyWebhookError("SHOPIFY_WEBHOOK_SECRET is not configured.")
    if not signature:
        return False
    expected = base64.b64encode(
        hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    ).decode("ascii")
    return hmac.compare_digest(expected, signature)


def publish_shopify_webhook(raw_body, headers):
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    topic_id = os.environ.get("PUBSUB_TOPIC_ID", "shopify-webhooks")
    if not project:
        raise ShopifyWebhookError("GOOGLE_CLOUD_PROJECT is not configured.")

    try:
        payload = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShopifyWebhookError("Shopify sent invalid JSON.") from exc

    envelope = {
        "webhook_id": headers.get("X-Shopify-Webhook-Id", ""),
        "topic": headers.get("X-Shopify-Topic", ""),
        "shop_domain": headers.get("X-Shopify-Shop-Domain", ""),
        "api_version": headers.get("X-Shopify-Api-Version", ""),
        "triggered_at": headers.get("X-Shopify-Triggered-At", ""),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    if not envelope["webhook_id"] or not envelope["topic"] or not envelope["shop_domain"]:
        raise ShopifyWebhookError("Required Shopify webhook headers are missing.")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_id)
    future = publisher.publish(
        topic_path,
        json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        webhook_id=envelope["webhook_id"],
        shop_domain=envelope["shop_domain"],
        shopify_topic=envelope["topic"],
    )
    return future.result(timeout=5)


def verify_pubsub_push(request):
    audience = os.environ.get("PUBSUB_PUSH_AUDIENCE", "")
    expected_email = os.environ.get("PUBSUB_PUSH_SERVICE_ACCOUNT", "")
    auth = request.headers.get("Authorization", "")
    if not audience or not expected_email:
        raise ShopifyWebhookError(
            "PUBSUB_PUSH_AUDIENCE and PUBSUB_PUSH_SERVICE_ACCOUNT must be configured."
        )
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


def decode_pubsub_push(body):
    try:
        message = body["message"]
        data = base64.b64decode(message["data"], validate=True)
        envelope = json.loads(data)
        envelope["pubsub_message_id"] = message.get("messageId", "")
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ShopifyWebhookError("Invalid Pub/Sub push envelope.") from exc
    if not isinstance(envelope, dict):
        raise ShopifyWebhookError("Invalid Pub/Sub message data.")
    return envelope
