import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { buildService } from "../../apps/service/src/server.ts";
import { startAuthHarness } from "./auth-harness.mjs";
import { Kafka, logLevel } from "kafkajs";
import pg from "pg";
import WebSocket from "ws";

const { Client } = pg;

function eventFixture(overrides) {
  return {
    id: `evt_t6_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    type: "order.shipped",
    recipients: ["recipient-a"],
    title: "Order shipped",
    body: "Your order is on the way.",
    payload: { orderId: "order-1" },
    occurredAt: "2026-07-19T10:00:00.000Z",
    ...overrides,
  };
}

async function postEvent(address, event, auth) {
  const response = await fetch(`${address}/events`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...await auth.producerHeaders(),
    },
    body: JSON.stringify(event),
  });
  const responseBody = await response.json();

  assert.equal(response.status, 202, JSON.stringify(responseBody));
}

async function produceRawMessage(kafka, topic, key, value) {
  const producer = kafka.producer();

  await producer.connect();
  try {
    await producer.send({
      topic,
      messages: [{ key, value }],
    });
  } finally {
    await producer.disconnect();
  }
}

async function waitForTopic(kafka, topic) {
  const admin = kafka.admin();

  await admin.connect();
  try {
    for (let attempt = 0; attempt < 80; attempt += 1) {
      try {
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] });

        if (metadata.topics.some((candidate) => candidate.name === topic && candidate.partitions.length > 0)) {
          return;
        }
      } catch {
        await delay(250);
      }
    }
  } finally {
    await admin.disconnect();
  }

  assert.fail(`Kafka topic ${topic} was not available`);
}

async function waitForInboxEvent(address, recipientId, eventId, auth) {
  const url = `${address}/recipients/${recipientId}/inbox`;

  for (let attempt = 0; attempt < 80; attempt += 1) {
    const response = await fetch(url, {
      headers: await auth.recipientHeaders(recipientId),
    });
    const body = await response.json();

    assert.equal(response.status, 200, JSON.stringify(body));

    const notification = body.notifications.find((candidate) => candidate.eventId === eventId);

    if (notification) {
      return notification;
    }

    await delay(250);
  }

  assert.fail(`Inbox for ${recipientId} did not include Notification for ${eventId}`);
}

async function waitForMetricValue(address, name, expected) {
  for (let attempt = 0; attempt < 80; attempt += 1) {
    const response = await fetch(`${address}/metrics`);
    const body = await response.text();
    const match = new RegExp(`^${name} (\\d+(?:\\.\\d+)?)$`, "m").exec(body);

    assert.ok(match, `expected ${name} in metrics body`);

    if (Number(match[1]) === expected) {
      return;
    }

    await delay(250);
  }

  assert.fail(`${name} did not reach ${expected}`);
}

async function openWebSocket(url, headers) {
  const socket = new WebSocket(url, { headers });

  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("WebSocket did not open")), 10_000);

    socket.once("open", () => {
      clearTimeout(timeout);
      resolve();
    });
    socket.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });

  return socket;
}

async function nextJsonMessage(socket) {
  return await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("WebSocket message timed out")), 30_000);

    socket.once("message", (data) => {
      clearTimeout(timeout);
      resolve(JSON.parse(data.toString()));
    });
  });
}

async function migrateNotificationsTable(postgres) {
  await postgres.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id text PRIMARY KEY,
      event_id text NOT NULL,
      recipient_id text NOT NULL,
      type text NOT NULL,
      title text NOT NULL,
      body text NOT NULL,
      payload jsonb NOT NULL,
      read boolean NOT NULL DEFAULT false,
      created_at timestamptz NOT NULL,
      UNIQUE (event_id, recipient_id)
    )
  `);
  await postgres.query(`
    CREATE INDEX IF NOT EXISTS notifications_recipient_created_idx
    ON notifications (recipient_id, created_at DESC, id DESC)
  `);
  await postgres.query(`
    CREATE INDEX IF NOT EXISTS notifications_recipient_unread_idx
    ON notifications (recipient_id)
    WHERE read = false
  `);
}

async function startDlqConsumer(kafka, topic) {
  const consumer = kafka.consumer({
    groupId: `t6-dlq-reader-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  });
  const messages = [];
  const waiters = [];

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const groupJoined = new Promise((resolve) => {
    const removeListener = consumer.on(consumer.events.GROUP_JOIN, () => {
      removeListener();
      resolve();
    });
  });

  function resolveWaiters() {
    for (let index = waiters.length - 1; index >= 0; index -= 1) {
      const waiter = waiters[index];
      const message = messages.find(waiter.predicate);

      if (message) {
        clearTimeout(waiter.timeout);
        waiters.splice(index, 1);
        waiter.resolve(message);
      }
    }
  }

  await consumer.run({
    eachMessage: async ({ message }) => {
      assert.ok(message.value, "DLQ message value is required");
      messages.push(JSON.parse(message.value.toString()));
      resolveWaiters();
    },
  });
  await groupJoined;

  return {
    async waitForParked(predicate, label) {
      const existing = messages.find(predicate);

      if (existing) {
        return existing;
      }

      return await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error(`timed out waiting for ${label}; parked messages: ${JSON.stringify(messages)}`));
        }, 45_000);

        waiters.push({ predicate, resolve, timeout });
      });
    },
    async stop() {
      for (const waiter of waiters) {
        clearTimeout(waiter.timeout);
      }
      await consumer.disconnect();
    },
  };
}

const port = Number(process.env.PORT);
const topic = process.env.KAFKA_TOPIC;
const dlqTopic = process.env.KAFKA_DLQ_TOPIC;
const brokers = process.env.KAFKA_BROKERS?.split(",").map((broker) => broker.trim()).filter(Boolean);

assert.ok(port > 0, "PORT is required");
assert.ok(topic, "KAFKA_TOPIC is required");
assert.ok(dlqTopic, "KAFKA_DLQ_TOPIC is required");
assert.ok(brokers?.length, "KAFKA_BROKERS is required");
assert.ok(process.env.POSTGRES_URL, "POSTGRES_URL is required");

const kafka = new Kafka({
  brokers,
  clientId: `t6-runner-${Date.now()}`,
  logLevel: logLevel.ERROR,
});
const auth = await startAuthHarness();
const service = buildService();
const postgres = new Client({ connectionString: process.env.POSTGRES_URL });
let dlqConsumer;
const sockets = [];

try {
  const address = await service.listen({ port, host: "127.0.0.1" });

  await postgres.connect();
  await waitForTopic(kafka, topic);
  await waitForTopic(kafka, dlqTopic);
  dlqConsumer = await startDlqConsumer(kafka, dlqTopic);

  const transientRecipientId = `recipient-t6-transient-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const transientEvent = eventFixture({
    id: `evt_t6_transient_${Date.now()}`,
    recipients: [transientRecipientId],
    title: "Recovered after transient database failure",
  });

  await postgres.query("DROP TABLE notifications");
  await postEvent(address, transientEvent, auth);
  await delay(150);
  await migrateNotificationsTable(postgres);

  const transientNotification = await waitForInboxEvent(address, transientRecipientId, transientEvent.id, auth);
  assert.equal(transientNotification.title, transientEvent.title);
  await waitForMetricValue(address, "notification_dlq_depth_messages", 0);

  await produceRawMessage(kafka, topic, "recipient-malformed", "not-json");

  const malformedParked = await dlqConsumer.waitForParked(
    (message) => message.failureKind === "malformed" && message.originalValue === "not-json",
    "malformed message on DLQ",
  );

  assert.equal(malformedParked.originalKey, "recipient-malformed");
  assert.equal(malformedParked.sourceTopic, topic);
  assert.equal(malformedParked.attempts, 0);
  assert.match(malformedParked.failureReason, /json|parse|unexpected/i);

  const recipientId = `recipient-t6-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const poisonEvent = eventFixture({
    id: `evt_t6_poison_${Date.now()}`,
    recipients: [recipientId],
    title: "Poison \u0000 shipment",
  });
  const healthyEvent = eventFixture({
    id: `evt_t6_after_poison_${Date.now()}`,
    recipients: [recipientId],
    title: "After poison",
  });
  const socket = await openWebSocket(address.replace("http://", "ws://") + "/connections/ws", {
    ...await auth.recipientHeaders(recipientId),
  });
  sockets.push(socket);

  await postEvent(address, poisonEvent, auth);
  const liveNotification = nextJsonMessage(socket);
  await postEvent(address, healthyEvent, auth);

  const poisonParked = await dlqConsumer.waitForParked(
    (message) => message.failureKind === "processing_failed" && message.originalValue?.includes(poisonEvent.id),
    "poison message on DLQ",
  );

  assert.equal(poisonParked.originalKey, recipientId);
  assert.equal(poisonParked.sourceTopic, topic);
  assert.equal(poisonParked.attempts, 6);
  assert.match(poisonParked.failureReason, /\S/);

  const notification = await waitForInboxEvent(address, recipientId, healthyEvent.id, auth);
  assert.equal(notification.title, healthyEvent.title);
  assert.equal((await liveNotification).eventId, healthyEvent.id);
  await waitForMetricValue(address, "notification_dlq_depth_messages", 2);
  await waitForMetricValue(address, "notification_delivery_notifications_total", 1);
} finally {
  for (const socket of sockets) {
    socket.close();
  }
  await dlqConsumer?.stop();
  await postgres.end();
  await service.close();
  await auth.stop();
}
