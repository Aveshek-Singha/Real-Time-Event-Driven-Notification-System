import { runPnpmScript } from "./e2e-runner.mjs";

const env = {
  ...process.env,
  HOST: "0.0.0.0",
  KAFKA_BROKERS: "localhost:9094",
  KAFKA_DLQ_TOPIC: `notifications-e2e-dlq-${Date.now()}`,
  KAFKA_GROUP_ID: `notification-service-e2e-${Date.now()}`,
  KAFKA_RETRY_ATTEMPTS: "3",
  KAFKA_RETRY_BACKOFF_MS: "250",
  KAFKA_TOPIC: `notifications-e2e-${Date.now()}`,
  OIDC_AUDIENCE: "notification-service",
  OIDC_ISSUER: "http://localhost:8080/realms/notifications",
  OIDC_PRODUCER_CLIENT_ID: "notification-producer",
  OIDC_RECIPIENT_CLIENT_ID: "notification-web",
  OIDC_RECIPIENT_ID_CLAIM: "recipient_id",
  PORT: "3001",
  POSTGRES_URL: "postgresql://notifications:notifications@localhost:5432/notifications",
  WEB_ORIGINS: "http://localhost:3002",
};

runPnpmScript(
  ["--filter", "@notification-system/service", "dev"],
  env,
);
