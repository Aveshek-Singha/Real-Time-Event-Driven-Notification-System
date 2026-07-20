import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { KafkaContainer } from "@testcontainers/kafka";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import { Wait } from "testcontainers";
import { Kafka, logLevel } from "kafkajs";
import pg from "pg";

const { Client } = pg;

async function startKafkaContainer() {
  let lastError;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      return await new KafkaContainer("confluentinc/cp-kafka:7.4.0")
        .withKraft()
        .withWaitStrategy(Wait.forListeningPorts().withStartupTimeout(300_000))
        .withStartupTimeout(300_000)
        .start();
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError;
}

describe("T1 Testcontainers smoke harness", () => {
  it("starts Kafka and Postgres and connects to both", { timeout: 600_000 }, async () => {
    const kafkaContainer = await startKafkaContainer();
    const postgresContainer = await new PostgreSqlContainer("postgres:16-alpine").start();

    try {
      const postgres = new Client({
        host: postgresContainer.getHost(),
        port: postgresContainer.getPort(),
        database: postgresContainer.getDatabase(),
        user: postgresContainer.getUsername(),
        password: postgresContainer.getPassword(),
      });
      await postgres.connect();
      const postgresResult = await postgres.query("SELECT 1 AS live");
      await postgres.end();

      assert.deepEqual(postgresResult.rows, [{ live: 1 }]);

      const kafka = new Kafka({
        brokers: [`${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`],
        clientId: "t1-smoke",
        logLevel: logLevel.ERROR,
      });
      const admin = kafka.admin();
      await admin.connect();
      const topics = await admin.listTopics();
      await admin.disconnect();

      assert.ok(Array.isArray(topics));
    } finally {
      await Promise.allSettled([kafkaContainer.stop(), postgresContainer.stop()]);
    }
  });
});
