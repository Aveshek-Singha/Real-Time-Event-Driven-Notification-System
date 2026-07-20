import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import OpsPage from "./ops/page";

describe("T8 ops page", () => {
  it("links operators to the configured Grafana dashboard and Kafka-UI", () => {
    const previousGrafanaUrl = process.env.NEXT_PUBLIC_GRAFANA_DASHBOARD_URL;
    const previousKafkaUiUrl = process.env.NEXT_PUBLIC_KAFKA_UI_URL;

    process.env.NEXT_PUBLIC_GRAFANA_DASHBOARD_URL = "http://grafana.example/d/notification-system";
    process.env.NEXT_PUBLIC_KAFKA_UI_URL = "http://kafka-ui.example/ui/clusters/local/all-topics";

    try {
      const html = renderToStaticMarkup(OpsPage());

      assert.match(html, /System health/);
      assert.match(html, /href="http:\/\/grafana\.example\/d\/notification-system"/);
      assert.match(html, /href="http:\/\/kafka-ui\.example\/ui\/clusters\/local\/all-topics"/);
    } finally {
      restoreEnv("NEXT_PUBLIC_GRAFANA_DASHBOARD_URL", previousGrafanaUrl);
      restoreEnv("NEXT_PUBLIC_KAFKA_UI_URL", previousKafkaUiUrl);
    }
  });
});

function restoreEnv(name: string, value: string | undefined) {
  if (value === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = value;
  }
}
