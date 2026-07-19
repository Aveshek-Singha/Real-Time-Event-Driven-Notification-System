# Real-Time Event-Driven Notification System

A pnpm monorepo for a standalone notification service. Producer systems will publish addressed Events to the Fastify service; the service will create durable Notifications and deliver them to Recipients through the web app.

## Workspace

- `apps/service`: Fastify service scaffold with health endpoints.
- `apps/web`: Next.js notification center scaffold.
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
- Producer client: `notification-producer` / `producer-secret`
- Demo recipient: `demo-recipient` / `password`
- Demo operator: `demo-operator` / `password`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`, admin `admin` / `admin`
- Kafka-UI: `http://localhost:8081`

## App Commands

Run the service locally:

```sh
pnpm --filter @notification-system/service dev
```

Run the web app locally:

```sh
pnpm --filter @notification-system/web dev -- --port 3002
```
