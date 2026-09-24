# The Fifth Element

PoC service connecting Shopify webhooks, Databricks, Gemini, Antavo, and Slack.
It is deployed to Cloud Run from `main` through Cloud Build.

## Structure

```text
main.py             Cloud Run adapter; exports service
http_api.py         URL dispatch
routes/             HTTP validation and response handling
services/           Application workflows and AI decision logic
integrations/       External API, identity, Pub/Sub, and Databricks adapters
```

The preferred Cloud Run entry point is `service`. `rewards_agent` remains as a
temporary alias so existing Cloud Build configuration continues to deploy.

## Routes

| Route | Purpose |
| --- | --- |
| `GET /` | Health and deployment marker |
| `POST /` | Leeloo, the Gemini agent for Antavo Management MCP |
| `POST /webhooks/shopify` | Signed Shopify webhook receiver |
| `POST /internal/shopify/process` | Authenticated Shopify Pub/Sub consumer |
| `POST /jobs/recovery/run` | Authenticated scheduled recovery worker |
| `POST /slack/interactions` | Signed Slack feedback receiver |
| `POST /internal/slack/process` | Authenticated Slack feedback consumer |

Business workflows should stay in `services/`. Code that speaks directly to an
external system belongs in `integrations/`; HTTP-specific behavior belongs in
`routes/`.

## Leeloo

Set `ANTAVO_MCP_BASIC_AUTH` to the Base64 encoded client credentials, without
the `Basic ` prefix. Leeloo requests a token with the `management_api_mcp.all`
scope and refreshes it five minutes before expiry. The token stays in process
memory and is never stored in Databricks. Cloud Run instances cache independently.

`POST /` with `{"prompt":"..."}` asks Leeloo a question. She can use MCP tools
whose server annotations set `readOnlyHint: true`. To authorize additional tools,
set `ANTAVO_MCP_ALLOWED_TOOLS` to a comma-separated list of exact MCP tool names.
Those tools may change Antavo data, so only add names you intend Leeloo to call.

Leeloo also has `search_web` (Gemini Google Search grounding) and
`read_web_page` (Gemini URL Context). Both use the configured `GEMINI_MODEL`
and `GEMINI_BACKEND`. They provide sourced web research but do not guarantee
that a result is a direct, publicly fetchable image URL for `upload_image`.

Leeloo has three customer tools backed by the existing signed Antavo API
credentials (`ANTAVO_STACK`, `ANTAVO_API_KEY`, `ANTAVO_API_SECRET`):
`customer_get(customer_id)`, `customer_give_reward(customer_id, reward_id, points?)`,
and `customer_custom_action(customer_id, ai_action, ai_message?, ai_points?)`.
Customer IDs are numeric Shopify IDs. The custom action records an `ai_action`
event; acceptance of the event alone does not confirm any later email or points
processing.
