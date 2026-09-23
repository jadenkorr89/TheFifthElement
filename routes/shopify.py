"""Shopify webhook and internal Pub/Sub HTTP handlers."""

import logging
from decimal import Decimal, InvalidOperation

from integrations.antavo import AntavoError, send_checkout, send_opt_in
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



def _to_decimal(value, field_name):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ShopifyWebhookError(
            f"Shopify order {field_name} must be numeric."
        ) from exc


def _send_order_created_checkout(event):
    if event.get("topic") != "orders/create":
        return

    order = event.get("payload")
    if not isinstance(order, dict):
        raise ShopifyWebhookError("Shopify order payload is invalid.")

    customer = order.get("customer")
    if not isinstance(customer, dict):
        customer = {}
    customer_id = customer.get("id") or order.get("customer_id")
    if not customer_id:
        raise ShopifyWebhookError("Shopify order customer id is missing.")

    order_id = order.get("id")
    if not order_id:
        raise ShopifyWebhookError("Shopify order id is missing.")

    line_items = order.get("line_items")
    if not isinstance(line_items, list) or not line_items:
        raise ShopifyWebhookError("Shopify order line_items are missing.")

    items = []
    for line in line_items:
        if not isinstance(line, dict):
            continue

        product_id = line.get("product_id")
        if not product_id:
            raise ShopifyWebhookError("Shopify order line item product_id is missing.")

        quantity = int(line.get("quantity") or 0)
        if quantity <= 0:
            raise ShopifyWebhookError("Shopify order line item quantity is invalid.")

        unit_price = _to_decimal(line.get("price"), "line item price")
        subtotal = unit_price * quantity
        items.append(
            {
                "product_id": str(product_id),
                "product_name": line.get("name") or line.get("title") or "",
                "price": float(unit_price),
                "quantity": quantity,
                "subtotal": float(subtotal),
            }
        )

    if not items:
        raise ShopifyWebhookError("Shopify order contains no usable line items.")

    total = _to_decimal(order.get("total_price"), "total_price")
    send_checkout(
        customer_id=customer_id,
        transaction_id=order_id,
        total=total,
        items=items,
        currency=order.get("currency"),
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
        _send_order_created_checkout(event)
    except ShopifyWebhookError as exc:
        return {"error": str(exc)}, 400
    except GoogleCloudConfigurationError as exc:
        logging.exception("Pub/Sub identity configuration is invalid")
        return {"error": str(exc)}, 503
    except DatabricksError:
        logging.exception("Could not store Shopify event")
        return {"error": "Could not store Shopify event."}, 503
    except AntavoError:
        logging.exception("Could not forward Shopify event to Antavo")
        return {"error": "Could not forward Shopify event to Antavo."}, 503
    return "", 204
