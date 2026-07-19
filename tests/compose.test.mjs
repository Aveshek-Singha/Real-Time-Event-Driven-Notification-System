import assert from "node:assert/strict";
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
