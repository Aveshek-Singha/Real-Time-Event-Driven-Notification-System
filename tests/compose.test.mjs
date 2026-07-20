import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { describe, it } from "node:test";

function composeConfig() {
  const result = spawnSync("docker", ["compose", "config", "--format", "json"], {
    cwd: process.cwd(),
    encoding: "utf8",
  });

  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

describe("T1 compose stack", () => {
  it("declares the required infrastructure services with healthchecks", () => {
    const config = composeConfig();
    const requiredServices = [
      "postgres",
      "kafka",
      "keycloak",
      "prometheus",
      "grafana",
      "kafka-ui",
    ];

    for (const serviceName of requiredServices) {
      assert.ok(config.services[serviceName], `${serviceName} should be declared`);
      assert.ok(config.services[serviceName].healthcheck, `${serviceName} should expose a healthcheck`);
    }
  });

  it("pre-provisions Keycloak and monitoring configuration", () => {
    const config = composeConfig();

    assert.match(JSON.stringify(config.services.keycloak.volumes), /realm-export\.json/);
    assert.match(JSON.stringify(config.services.prometheus.volumes), /prometheus\.yml/);
    assert.match(JSON.stringify(config.services.grafana.volumes), /provisioning/);
  });
});

describe("T6 observability config", () => {
  it("configures Prometheus to scrape service metrics", () => {
    const config = composeConfig();
    const prometheus = readFileSync(join(process.cwd(), "infra/prometheus/prometheus.yml"), "utf8");

    assert.match(prometheus, /job_name:\s+"notification-service"/);
    assert.match(prometheus, /host\.docker\.internal:3001/);
    assert.match(JSON.stringify(config.services.prometheus.extra_hosts ?? []), /host\.docker\.internal/);
  });

  it("provisions Grafana panels for the T6 metric families", () => {
    const dashboard = JSON.parse(
      readFileSync(join(process.cwd(), "infra/grafana/dashboards/notification-system.json"), "utf8"),
    );
    const expressions = JSON.stringify(
      dashboard.panels.flatMap((panel) => panel.targets?.map((target) => target.expr) ?? []),
    );

    for (const expected of [
      "notification_ingest_events_total",
      "notification_delivery_notifications_total",
      "notification_consumer_lag_messages",
      "notification_dlq_depth_messages",
      "notification_live_connections",
    ]) {
      assert.match(expressions, new RegExp(expected));
    }
  });
});
