import { runPnpmScript } from "./e2e-runner.mjs";

const env = {
  ...process.env,
  NEXT_PUBLIC_GRAFANA_DASHBOARD_URL: "http://localhost:3000/d/notification-system/notification-system",
  NEXT_PUBLIC_KAFKA_UI_URL: "http://localhost:8081",
  NEXT_PUBLIC_KEYCLOAK_AUTHORIZATION_URL:
    "http://localhost:8080/realms/notifications/protocol/openid-connect/auth",
  NEXT_PUBLIC_KEYCLOAK_CLIENT_ID: "notification-web",
  NEXT_PUBLIC_KEYCLOAK_TOKEN_URL: "http://localhost:8080/realms/notifications/protocol/openid-connect/token",
  NEXT_PUBLIC_NOTIFICATION_API_BASE_URL: "http://localhost:3001",
};

runPnpmScript(
  ["--dir", "apps/web", "exec", "next", "dev", "--hostname", "0.0.0.0", "--port", "3002"],
  env,
);
