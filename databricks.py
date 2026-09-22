"""Minimal Databricks Statement Execution API client."""
import json
import os
import re

import requests


class DatabricksError(Exception):
    pass


_TABLE_READY = False
_CART_STATE_READY = False
_DECISIONS_READY = False


def _config():
    required = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_WAREHOUSE_ID",
        "DATABRICKS_CATALOG",
        "DATABRICKS_SCHEMA",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise DatabricksError("Missing configuration: " + ", ".join(missing))

    catalog = os.environ["DATABRICKS_CATALOG"]
    schema = os.environ["DATABRICKS_SCHEMA"]
    table = os.environ.get("DATABRICKS_SHOPIFY_EVENTS_TABLE", "shopify_webhook_events")
    for label, value in (("catalog", catalog), ("schema", schema), ("table", table)):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise DatabricksError(f"Invalid Databricks {label} identifier.")
    return {
        "host": os.environ["DATABRICKS_HOST"].rstrip("/"),
        "token": os.environ["DATABRICKS_TOKEN"],
        "warehouse": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "catalog": catalog,
        "schema": schema,
        "fqn": f"`{catalog}`.`{schema}`.`{table}`",
    }



def _qualified_name(env_name, default):
    config = _config()
    name = os.environ.get(env_name, default)
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        raise DatabricksError(f"Invalid Databricks identifier in {env_name}.")
    return f"`{config['catalog']}`.`{config['schema']}`.`{name}`"


def _param(name, value):
    return {"name": name, "value": "" if value is None else str(value)}


def execute_statement(statement, parameters=None):
    config = _config()
    try:
        response = requests.post(
            f"{config['host']}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {config['token']}"},
            json={
                "warehouse_id": config["warehouse"],
                "statement": statement,
                "parameters": parameters or [],
                "wait_timeout": "50s",
                "on_wait_timeout": "CANCEL",
            },
            timeout=(10, 65),
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise DatabricksError("Could not reach Databricks.") from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise DatabricksError(
            f"Databricks returned non-JSON HTTP {response.status_code}."
        ) from exc
    if response.status_code != 200:
        raise DatabricksError(
            f"Databricks API returned HTTP {response.status_code}: "
            + json.dumps(data)[:1000]
        )
    status = data.get("status", {})
    if status.get("state") != "SUCCEEDED":
        raise DatabricksError(
            "Databricks statement failed: " + json.dumps(status)[:1000]
        )
    return data


def ensure_shopify_events_table():
    global _TABLE_READY
    if _TABLE_READY:
        return
    fqn = _config()["fqn"]
    execute_statement(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
          webhook_id STRING NOT NULL,
          topic STRING NOT NULL,
          shop_domain STRING NOT NULL,
          api_version STRING,
          triggered_at STRING,
          received_at TIMESTAMP NOT NULL,
          pubsub_message_id STRING,
          payload_json STRING NOT NULL
        ) USING DELTA
        """
    )
    _TABLE_READY = True



def ensure_cart_state_objects():
    global _CART_STATE_READY
    if _CART_STATE_READY:
        return
    state_fqn = _qualified_name(
        "DATABRICKS_SHOPIFY_CART_STATE_TABLE", "shopify_cart_state"
    )
    candidates_fqn = _qualified_name(
        "DATABRICKS_SHOPIFY_CANDIDATES_VIEW", "shopify_abandonment_candidates"
    )
    execute_statement(
        f"""
        CREATE TABLE IF NOT EXISTS {state_fqn} (
          shop_domain STRING NOT NULL,
          state_token STRING NOT NULL,
          cart_token STRING,
          checkout_token STRING,
          customer_id STRING,
          email STRING,
          currency STRING,
          total_price STRING,
          item_count BIGINT,
          first_seen_at TIMESTAMP NOT NULL,
          last_seen_at TIMESTAMP NOT NULL,
          converted_at TIMESTAMP,
          order_id STRING,
          status STRING NOT NULL,
          latest_topic STRING NOT NULL,
          latest_payload_json STRING NOT NULL
        ) USING DELTA
        """
    )
    execute_statement(
        f"""
        CREATE OR REPLACE VIEW {candidates_fqn} AS
        SELECT *
        FROM {state_fqn}
        WHERE status = 'ACTIVE'
          AND email IS NOT NULL
          AND email <> ''
          AND item_count > 0
          AND last_seen_at <= current_timestamp() - INTERVAL 30 MINUTES
        """
    )
    _CART_STATE_READY = True


def project_shopify_event(event):
    ensure_cart_state_objects()
    topic = str(event.get("topic", "")).lower()
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return

    state_fqn = _qualified_name(
        "DATABRICKS_SHOPIFY_CART_STATE_TABLE", "shopify_cart_state"
    )
    shop_domain = str(event.get("shop_domain", ""))
    received_at = str(event.get("received_at", ""))
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    if topic.startswith("orders/"):
        cart_token = payload.get("cart_token") or ""
        checkout_token = payload.get("checkout_token") or ""
        if not cart_token and not checkout_token:
            return
        execute_statement(
            f"""
            UPDATE {state_fqn}
            SET status = 'CONVERTED',
                converted_at = CAST(:converted_at AS TIMESTAMP),
                order_id = :order_id,
                latest_topic = :topic
            WHERE shop_domain = :shop_domain
              AND (
                (:cart_token <> '' AND cart_token = :cart_token)
                OR (:checkout_token <> '' AND checkout_token = :checkout_token)
              )
            """,
            [
                _param("converted_at", received_at),
                _param("order_id", payload.get("id")),
                _param("topic", topic),
                _param("shop_domain", shop_domain),
                _param("cart_token", cart_token),
                _param("checkout_token", checkout_token),
            ],
        )
        return

    if not (topic.startswith("carts/") or topic.startswith("checkouts/")):
        return

    is_checkout = topic.startswith("checkouts/")
    cart_token = payload.get("cart_token") or (
        payload.get("token") if not is_checkout else ""
    )
    checkout_token = payload.get("checkout_token") or (
        payload.get("token") if is_checkout else ""
    )
    state_token = checkout_token or cart_token or payload.get("id")
    if not state_token:
        return

    customer = payload.get("customer")
    if not isinstance(customer, dict):
        customer = {}
    email = payload.get("email") or customer.get("email") or ""
    customer_id = customer.get("id") or payload.get("customer_id") or ""
    items = payload.get("line_items") or payload.get("items") or []
    item_count = payload.get("item_count")
    if item_count is None:
        item_count = sum(
            int(item.get("quantity", 1))
            for item in items
            if isinstance(item, dict)
        )
    total_price = (
        payload.get("total_price")
        or payload.get("current_total_price")
        or payload.get("subtotal_price")
        or ""
    )
    converted = bool(payload.get("completed_at") or payload.get("order_id"))
    status = "CONVERTED" if converted else "ACTIVE"

    execute_statement(
        f"""
        MERGE INTO {state_fqn} AS target
        USING (
          SELECT
            :shop_domain AS shop_domain,
            :state_token AS state_token,
            NULLIF(:cart_token, '') AS cart_token,
            NULLIF(:checkout_token, '') AS checkout_token,
            NULLIF(:customer_id, '') AS customer_id,
            NULLIF(:email, '') AS email,
            NULLIF(:currency, '') AS currency,
            NULLIF(:total_price, '') AS total_price,
            CAST(:item_count AS BIGINT) AS item_count,
            CAST(:seen_at AS TIMESTAMP) AS first_seen_at,
            CAST(:seen_at AS TIMESTAMP) AS last_seen_at,
            CASE WHEN :status = 'CONVERTED'
              THEN CAST(:seen_at AS TIMESTAMP) ELSE NULL END AS converted_at,
            NULLIF(:order_id, '') AS order_id,
            :status AS status,
            :topic AS latest_topic,
            :payload_json AS latest_payload_json
        ) AS source
        ON target.shop_domain = source.shop_domain
          AND target.state_token = source.state_token
        WHEN MATCHED AND source.last_seen_at >= target.last_seen_at THEN UPDATE SET
          cart_token = COALESCE(source.cart_token, target.cart_token),
          checkout_token = COALESCE(source.checkout_token, target.checkout_token),
          customer_id = COALESCE(source.customer_id, target.customer_id),
          email = COALESCE(source.email, target.email),
          currency = COALESCE(source.currency, target.currency),
          total_price = COALESCE(source.total_price, target.total_price),
          item_count = source.item_count,
          last_seen_at = source.last_seen_at,
          converted_at = COALESCE(source.converted_at, target.converted_at),
          order_id = COALESCE(source.order_id, target.order_id),
          status = CASE
            WHEN target.status = 'CONVERTED' THEN target.status
            ELSE source.status
          END,
          latest_topic = source.latest_topic,
          latest_payload_json = source.latest_payload_json
        WHEN NOT MATCHED THEN INSERT *
        """,
        [
            _param("shop_domain", shop_domain),
            _param("state_token", state_token),
            _param("cart_token", cart_token),
            _param("checkout_token", checkout_token),
            _param("customer_id", customer_id),
            _param("email", email),
            _param("currency", payload.get("currency")),
            _param("total_price", total_price),
            _param("item_count", item_count),
            _param("seen_at", received_at),
            _param("status", status),
            _param("order_id", payload.get("order_id")),
            _param("topic", topic),
            _param("payload_json", payload_json),
        ],
    )


def store_shopify_event(event):
    ensure_shopify_events_table()
    fqn = _config()["fqn"]
    payload_json = json.dumps(
        event.get("payload"), ensure_ascii=False, separators=(",", ":")
    )
    execute_statement(
        f"""
        MERGE INTO {fqn} AS target
        USING (
          SELECT
            :webhook_id AS webhook_id,
            :topic AS topic,
            :shop_domain AS shop_domain,
            :api_version AS api_version,
            :triggered_at AS triggered_at,
            CAST(:received_at AS TIMESTAMP) AS received_at,
            :pubsub_message_id AS pubsub_message_id,
            :payload_json AS payload_json
        ) AS source
        ON target.webhook_id = source.webhook_id
        WHEN NOT MATCHED THEN INSERT *
        """,
        [
            {"name": "webhook_id", "value": str(event.get("webhook_id", ""))},
            {"name": "topic", "value": str(event.get("topic", ""))},
            {"name": "shop_domain", "value": str(event.get("shop_domain", ""))},
            {"name": "api_version", "value": str(event.get("api_version", ""))},
            {"name": "triggered_at", "value": str(event.get("triggered_at", ""))},
            {"name": "received_at", "value": str(event.get("received_at", ""))},
            {
                "name": "pubsub_message_id",
                "value": str(event.get("pubsub_message_id", "")),
            },
            {"name": "payload_json", "value": payload_json},
        ],
    )
    project_shopify_event(event)

def count_shopify_events():
    ensure_shopify_events_table()
    fqn = _config()["fqn"]
    data = execute_statement(f"SELECT COUNT(*) FROM {fqn}")
    return int(data["result"]["data_array"][0][0])

def ensure_recovery_decisions_table():
    global _DECISIONS_READY
    if _DECISIONS_READY:
        return
    fqn = _qualified_name(
        "DATABRICKS_RECOVERY_DECISIONS_TABLE", "shopify_recovery_decisions"
    )
    execute_statement(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
          decision_id STRING NOT NULL,
          shop_domain STRING NOT NULL,
          state_token STRING NOT NULL,
          decided_at TIMESTAMP NOT NULL,
          decision_status STRING NOT NULL,
          recommended_action STRING NOT NULL,
          reason STRING NOT NULL,
          message_subject STRING,
          message_body STRING,
          model STRING NOT NULL,
          input_snapshot_json STRING NOT NULL,
          bloomreach_status STRING NOT NULL
        ) USING DELTA
        """
    )
    _DECISIONS_READY = True


def list_unprocessed_recovery_candidates(limit=5):
    ensure_cart_state_objects()
    ensure_recovery_decisions_table()
    candidates_fqn = _qualified_name(
        "DATABRICKS_SHOPIFY_CANDIDATES_VIEW", "shopify_abandonment_candidates"
    )
    decisions_fqn = _qualified_name(
        "DATABRICKS_RECOVERY_DECISIONS_TABLE", "shopify_recovery_decisions"
    )
    safe_limit = max(1, min(int(limit), 25))
    data = execute_statement(
        f"""
        SELECT
          c.shop_domain,
          c.state_token,
          c.cart_token,
          c.checkout_token,
          c.currency,
          c.total_price,
          c.item_count,
          CAST(c.first_seen_at AS STRING),
          CAST(c.last_seen_at AS STRING),
          c.latest_topic,
          c.latest_payload_json
        FROM {candidates_fqn} AS c
        LEFT ANTI JOIN {decisions_fqn} AS d
          ON d.shop_domain = c.shop_domain
         AND d.state_token = c.state_token
        ORDER BY c.last_seen_at
        LIMIT {safe_limit}
        """
    )
    rows = data.get("result", {}).get("data_array", [])
    keys = [
        "shop_domain",
        "state_token",
        "cart_token",
        "checkout_token",
        "currency",
        "total_price",
        "item_count",
        "first_seen_at",
        "last_seen_at",
        "latest_topic",
        "latest_payload_json",
    ]
    return [dict(zip(keys, row)) for row in rows]


def store_recovery_decision(decision):
    ensure_recovery_decisions_table()
    fqn = _qualified_name(
        "DATABRICKS_RECOVERY_DECISIONS_TABLE", "shopify_recovery_decisions"
    )
    execute_statement(
        f"""
        MERGE INTO {fqn} AS target
        USING (
          SELECT
            :decision_id AS decision_id,
            :shop_domain AS shop_domain,
            :state_token AS state_token,
            current_timestamp() AS decided_at,
            'PROPOSED' AS decision_status,
            :recommended_action AS recommended_action,
            :reason AS reason,
            NULLIF(:message_subject, '') AS message_subject,
            NULLIF(:message_body, '') AS message_body,
            :model AS model,
            :input_snapshot_json AS input_snapshot_json,
            'DISABLED' AS bloomreach_status
        ) AS source
        ON target.decision_id = source.decision_id
        WHEN NOT MATCHED THEN INSERT *
        """,
        [
            _param("decision_id", decision["decision_id"]),
            _param("shop_domain", decision["shop_domain"]),
            _param("state_token", decision["state_token"]),
            _param("recommended_action", decision["recommended_action"]),
            _param("reason", decision["reason"]),
            _param("message_subject", decision.get("message_subject")),
            _param("message_body", decision.get("message_body")),
            _param("model", decision["model"]),
            _param("input_snapshot_json", decision["input_snapshot_json"]),
        ],
    )

