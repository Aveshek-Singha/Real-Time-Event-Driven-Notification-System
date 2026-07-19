import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer, Socket } from "node:net";
import { describe, it } from "node:test";
import { KafkaContainer } from "@testcontainers/kafka";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";

const { Client } = pg;

async function findOpenPort() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const port = 16_000 + Math.floor(Math.random() * 10_000);
    const server = createServer();

    try {
      server.listen(port, "127.0.0.1");
      await Promise.race([
        once(server, "listening"),
        once(server, "error").then(([error]) => {
          throw error;
        }),
      ]);
      server.close();
      await once(server, "close");

      return port;
    } catch {
      server.close();
    }
  }

  assert.fail("could not find an open high port");
}

async function runningComposeInfrastructure() {
  const kafkaBroker = "localhost:9094";
  const postgresUrl = "postgresql://notifications:notifications@localhost:5432/notifications";
  const postgres = new Client({ connectionString: postgresUrl });

  try {
    await probeTcpPort(9094);
    await postgres.connect();
    await postgres.query("SELECT 1");

    return {
      kafkaBroker,
      postgresUrl,
      stop: async () => {},
    };
  } catch {
    return undefined;
  } finally {
    await postgres.end().catch(() => {});
  }
}

async function probeTcpPort(port) {
  await new Promise((resolve, reject) => {
    const client = new Socket();
    const timeout = setTimeout(() => {
      client.destroy();
      reject(new Error(`port ${port} probe timed out`));
    }, 1_000);

    client.once("connect", () => {
      clearTimeout(timeout);
      client.end();
      resolve();
    });
    client.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    client.connect(port, "127.0.0.1");
  });
}

async function startKafkaContainer() {
  let lastError;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      return await new KafkaContainer("confluentinc/cp-kafka:7.4.0")
        .withKraft()
        .withStartupTimeout(300_000)
        .start();
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError;
}

async function testcontainersInfrastructure() {
  const kafkaContainer = await startKafkaContainer();
  const postgresContainer = await new PostgreSqlContainer("postgres:16-alpine").start();

  return {
    kafkaBroker: `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`,
    postgresUrl: `postgresql://${postgresContainer.getUsername()}:${postgresContainer.getPassword()}@${postgresContainer.getHost()}:${postgresContainer.getPort()}/${postgresContainer.getDatabase()}`,
    stop: async () => {
      await Promise.allSettled([kafkaContainer.stop(), postgresContainer.stop()]);
    },
  };
}

async function startInfrastructure() {
  return await runningComposeInfrastructure() ?? await testcontainersInfrastructure();
}

async function waitForRunner(runner, output) {
  let timeout;

  try {
    return await Promise.race([
      once(runner, "exit"),
      new Promise((_, reject) => {
        timeout = setTimeout(() => {
          runner.kill();
          reject(new Error(`T2 runner timed out\n${output.join("")}`));
        }, 420_000);
        timeout.unref();
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

describe("T2 Event in to Notification out over WebSocket", () => {
  it("accepts an Event, persists a Notification, and pushes it live", { timeout: 420_000 }, async () => {
    const infrastructure = await startInfrastructure();
    const port = await findOpenPort();
    const output = [];
    const runner = spawn(process.execPath, [
      "--conditions=development",
      "--import",
      "tsx",
      "tests/integration/t2-tracer-bullet-runner.mjs",
    ], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        KAFKA_BROKERS: infrastructure.kafkaBroker,
        KAFKA_GROUP_ID: `t2-${Date.now()}`,
        KAFKA_TOPIC: `notifications-t2-${Date.now()}`,
        PORT: String(port),
        POSTGRES_URL: infrastructure.postgresUrl,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    runner.stdout.on("data", (chunk) => output.push(chunk.toString()));
    runner.stderr.on("data", (chunk) => output.push(chunk.toString()));

    try {
      const [code, signal] = await waitForRunner(runner, output);

      assert.equal(code, 0, `runner exited with code ${code} signal ${signal}\n${output.join("")}`);
    } finally {
      await infrastructure.stop();
    }
  });
});
