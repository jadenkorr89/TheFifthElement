"""Shopify webhook and internal Pub/Sub HTTP handlers."""

import logging

from integrations.antavo import AntavoError, send_opt_in
from integrations.databricks import DatabricksError, store_shopify_event
from integrations.google_cloud import (
    GoogleCloudConfigurationError,
    verify_pubsub_push,
)
from integrations.shopify import (
    ShopifyWebhookError,
    decode_pubsub_push,
    publish_shopify_webhook,
    verify_shopify_webhook,
)


def receive_shopify_webhook(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    raw_body = request.get_data(cache=True)
    try:
        if not verify_shopify_webhook(
            raw_body, request.headers.get("X-Shopify-Hmac-Sha256")
        ):
            return {"error": "Invalid Shopify signature."}, 401
        message_id = publish_shopify_webhook(raw_body, request.headers)
    except ShopifyWebhookError as exc:
        return {"error": str(exc)}, 400
    except Exception:
        logging.exception("Could not publish Shopify webhook")
        return {"error": "Could not queue webhook."}, 503
    return {"ok": True, "queued": True, "message_id": message_id}, 200


def _send_customer_created_opt_in(event):
    if event.get("topic") != "customers/create":
        return

    customer = event.get("payload")
    if not isinstance(customer, dict):
        raise ShopifyWebhookError("Shopify customer payload is invalid.")

    send_opt_in(
        customer.get("id"),
        email=customer.get("email", ""),
        first_name=customer.get("first_name", ""),
        last_name=customer.get("last_name", ""),
    )


def process_shopify_event(request):
    if request.method != "POST":
        return {"error": "Method not allowed."}, 405, {"Allow": "POST"}
    try:
        if not verify_pubsub_push(request):
            return {"error": "Invalid Pub/Sub identity."}, 401
        event = decode_pubsub_push(request.get_json(silent=True))
        store_shopify_event(event)
        _send_customer_created_opt_in(event)
    except ShopifyWebhookError as exc:
        return {"error": str(exc)}, 400
    except GoogleCloudConfigurationError as exc:
        logging.exception("Pub/Sub identity configuration is invalid")
        return {"error": str(exc)}, 503
    except DatabricksError:
        logging.exception("Could not store Shopify event")
        return {"error": "Could not store Shopify event."}, 503
    except AntavoError:
        logging.exception("Could not send Shopify customer opt-in to Antavo")
        return {"error": "Could not send Antavo opt-in event."}, 503
    return "", 204
