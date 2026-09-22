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
| `POST /` | Manual Antavo rewards agent |
| `POST /webhooks/shopify` | Signed Shopify webhook receiver |
| `POST /internal/shopify/process` | Authenticated Shopify Pub/Sub consumer |
| `POST /jobs/recovery/run` | Authenticated scheduled recovery worker |
| `POST /slack/interactions` | Signed Slack feedback receiver |
| `POST /internal/slack/process` | Authenticated Slack feedback consumer |

Business workflows should stay in `services/`. Code that speaks directly to an
external system belongs in `integrations/`; HTTP-specific behavior belongs in
`routes/`.
