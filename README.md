# Real-Time Event-Driven Notification System

A pnpm monorepo for a standalone notification service. Producer systems will publish addressed Events to the Fastify service; the service will create durable Notifications and deliver them to Recipients through the web app.

## Workspace

- `apps/service`: Fastify service scaffold with health endpoints.
- `apps/web`: Next.js notification center for Recipient sign-in, live Delivery, unread count, and read-state controls.
- `packages/contracts`: shared zod contracts package.
- `infra`: local Docker Compose provisioning for Kafka, Postgres, Keycloak, Prometheus, Grafana, and Kafka-UI.

## Local Setup

Install dependencies:

```sh
pnpm install
```

Run the root verification commands:

```sh
pnpm build
pnpm lint
pnpm typecheck
pnpm test
```

Run the Testcontainers Kafka/Postgres smoke test:

```sh
pnpm test:integration
```

The integration test requires a running Docker daemon.

Run only the retry/DLQ/metrics integration slice:

```sh
node --conditions=development --import tsx --test --test-concurrency=1 tests/integration/t6-retry-dlq-metrics.test.mjs
```

## Compose Stack

Start all infrastructure services and wait for healthchecks:

```sh
docker compose up -d --wait
```

Stop the stack and remove local volumes:

```sh
docker compose down -v
```

Local service endpoints:

- Postgres: `localhost:5432`, database/user/password `notifications`
- Kafka external listener: `localhost:9094`
- Keycloak: `http://localhost:8080`, admin `admin` / `admin`
- Keycloak realm: `notifications`
- Service audience: `notification-service`
- Producer client: `notification-producer` / `producer-secret`
- Web client: `notification-web`
- Demo Recipient: `recipient-demo` / `password`
- Demo operator: `demo-operator` / `password`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`, admin `admin` / `admin`
- Kafka-UI: `http://localhost:8081`

Prometheus scrapes the locally running service at `host.docker.internal:3001/metrics`.
Start the service on port `3001` when you want Grafana to show live service data.

Protected service surfaces require OIDC access tokens. Configure the service to validate compose Keycloak tokens:

```sh
OIDC_ISSUER=http://localhost:8080/realms/notifications
OIDC_AUDIENCE=notification-service
OIDC_PRODUCER_CLIENT_ID=notification-producer
OIDC_RECIPIENT_CLIENT_ID=notification-web
OIDC_RECIPIENT_ID_CLAIM=recipient_id
WEB_ORIGINS=http://localhost:3002
```

Configure the service pipeline against the compose infrastructure:

```sh
KAFKA_BROKERS=localhost:9094
KAFKA_TOPIC=notifications
KAFKA_DLQ_TOPIC=notifications.dlq
KAFKA_RETRY_ATTEMPTS=3
KAFKA_RETRY_BACKOFF_MS=250
POSTGRES_URL=postgresql://notifications:notifications@localhost:5432/notifications
```

Parked Events are written to the DLQ topic with the original Kafka payload, failure reason, failure kind, and attempt count. Inspect them in Kafka-UI under the configured `KAFKA_DLQ_TOPIC`.

Get a Producer token for `POST /events`:

```sh
curl -s http://localhost:8080/realms/notifications/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=notification-producer \
  -d client_secret=producer-secret
```

Get a demo Recipient token for Inbox REST, WS, or SSE:

```sh
curl -s http://localhost:8080/realms/notifications/protocol/openid-connect/token \
  -d grant_type=password \
  -d client_id=notification-web \
  -d username=demo-recipient \
  -d password=password
```

## App Commands

Run the service locally:

```sh
pnpm --filter @notification-system/service dev
```

The service exposes Prometheus metrics at `http://localhost:3001/metrics` by default.

Run the web app locally:

```sh
NEXT_PUBLIC_NOTIFICATION_API_BASE_URL=http://localhost:3001
NEXT_PUBLIC_KEYCLOAK_AUTHORIZATION_URL=http://localhost:8080/realms/notifications/protocol/openid-connect/auth
NEXT_PUBLIC_KEYCLOAK_TOKEN_URL=http://localhost:8080/realms/notifications/protocol/openid-connect/token
NEXT_PUBLIC_KEYCLOAK_CLIENT_ID=notification-web
pnpm --dir apps/web exec next dev --port 3002
```

Open `http://localhost:3002` and sign in as `recipient-demo` / `password`.
The app loads that Recipient's Inbox, opens a live Connection, shows a toast for new Notifications, and lets the Recipient mark one or all Notifications read.

Publish a demo Event after signing in:

```sh
PRODUCER_TOKEN=$(curl -s http://localhost:8080/realms/notifications/protocol/openid-connect/token \
  -d grant_type=client_credentials \
  -d client_id=notification-producer \
  -d client_secret=producer-secret | node -e "let body='';process.stdin.on('data',c=>body+=c);process.stdin.on('end',()=>console.log(JSON.parse(body).access_token))")

curl -i http://localhost:3001/events \
  -H "authorization: Bearer $PRODUCER_TOKEN" \
  -H "content-type: application/json" \
  -d '{
    "id": "evt_demo_order_shipped",
    "type": "order.shipped",
    "recipients": ["recipient-demo"],
    "title": "Order shipped",
    "body": "Your demo order is on the way.",
    "payload": { "orderId": "demo-order-1" },
    "occurredAt": "2026-07-20T10:00:00.000Z"
  }'
```

Use the `AUTO`, `WS`, and `SSE` transport control in the app to verify both live transports.
