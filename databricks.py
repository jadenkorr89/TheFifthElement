"""Minimal Databricks Statement Execution API client."""
import json
import os
import re

import requests


class DatabricksError(Exception):
    pass


_TABLE_READY = False


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
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise DatabricksError(f"Invalid Databricks {label} identifier.")
    return {
        "host": os.environ["DATABRICKS_HOST"].rstrip("/"),
        "token": os.environ["DATABRICKS_TOKEN"],
        "warehouse": os.environ["DATABRICKS_WAREHOUSE_ID"],
        "fqn": f"`{catalog}`.`{schema}`.`{table}`",
    }


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


def count_shopify_events():
    ensure_shopify_events_table()
    fqn = _config()["fqn"]
    data = execute_statement(f"SELECT COUNT(*) FROM {fqn}")
    return int(data["result"]["data_array"][0][0])
