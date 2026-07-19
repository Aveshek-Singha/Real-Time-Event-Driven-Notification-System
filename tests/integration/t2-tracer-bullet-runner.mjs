import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { buildService } from "../../apps/service/src/server.ts";
import { startAuthHarness } from "./auth-harness.mjs";
import pg from "pg";
import WebSocket from "ws";

const { Client } = pg;

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

async function noJsonMessage(socket) {
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, 500);

    socket.once("message", (data) => {
      clearTimeout(timeout);
      reject(new Error(`unexpected WebSocket message: ${data.toString()}`));
    });
  });
}

async function postEvent(port, event, auth) {
  const response = await fetch(`http://127.0.0.1:${port}/events`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...await auth.producerHeaders(),
    },
    body: JSON.stringify(event),
  });
  const responseBody = await response.json();

  assert.equal(response.status, 202, JSON.stringify(responseBody));
  assert.deepEqual(responseBody, {
    accepted: true,
    eventId: event.id,
    notifications: event.recipients.length,
  });
}

function eventFixture(overrides) {
  return {
    id: `evt_t2_order_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    type: "order.shipped",
    recipients: ["recipient-a"],
    title: "Order shipped",
    body: "Your order is on the way.",
    payload: { orderId: "order-1" },
    occurredAt: "2026-07-19T10:00:00.000Z",
    ...overrides,
  };
}

function assertNotification(notification, event, recipientId) {
  assert.equal(notification.eventId, event.id);
  assert.equal(notification.recipientId, recipientId);
  assert.equal(notification.title, event.title);
  assert.equal(notification.read, false);
}

const port = Number(process.env.PORT);
assert.ok(port > 0, "PORT is required");
assert.ok(process.env.POSTGRES_URL, "POSTGRES_URL is required");

const auth = await startAuthHarness();
const service = buildService();
const sockets = [];
let postgres;

try {
  await service.listen({ port, host: "127.0.0.1" });
  postgres = new Client({ connectionString: process.env.POSTGRES_URL });
  await postgres.connect();

  const firstRecipientConnection = await openWebSocket(
    `ws://127.0.0.1:${port}/connections/ws`,
    await auth.recipientHeaders("recipient-a"),
  );
  const secondRecipientConnection = await openWebSocket(
    `ws://127.0.0.1:${port}/connections/ws`,
    await auth.recipientHeaders("recipient-a"),
  );
  const otherRecipientConnection = await openWebSocket(
    `ws://127.0.0.1:${port}/connections/ws`,
    await auth.recipientHeaders("recipient-b"),
  );
  sockets.push(firstRecipientConnection, secondRecipientConnection, otherRecipientConnection);
  await delay(100);

  const fanOutEvent = eventFixture({
    recipients: ["recipient-a", "recipient-b"],
    title: "Fan-out order shipped",
  });
  const fanOutMessages = [
    nextJsonMessage(firstRecipientConnection),
    nextJsonMessage(secondRecipientConnection),
    nextJsonMessage(otherRecipientConnection),
  ];

  await postEvent(port, fanOutEvent, auth);

  const [firstRecipientNotification, secondRecipientNotification, otherRecipientNotification] =
    await Promise.all(fanOutMessages);
  assertNotification(firstRecipientNotification, fanOutEvent, "recipient-a");
  assertNotification(secondRecipientNotification, fanOutEvent, "recipient-a");
  assertNotification(otherRecipientNotification, fanOutEvent, "recipient-b");

  const persisted = await postgres.query(
    "SELECT event_id, recipient_id, title, read FROM notifications WHERE event_id = $1 ORDER BY recipient_id",
    [fanOutEvent.id],
  );

  assert.deepEqual(persisted.rows, [{
    event_id: fanOutEvent.id,
    recipient_id: "recipient-a",
    read: false,
    title: fanOutEvent.title,
  }, {
    event_id: fanOutEvent.id,
    recipient_id: "recipient-b",
    read: false,
    title: fanOutEvent.title,
  }]);

  const retryConnection = await openWebSocket(
    `ws://127.0.0.1:${port}/connections/ws`,
    await auth.recipientHeaders("recipient-retry"),
  );
  sockets.push(retryConnection);
  await delay(100);

  const retryEvent = eventFixture({
    id: `evt_t2_retry_${Date.now()}`,
    recipients: ["recipient-retry"],
  });
  const firstRetryMessage = nextJsonMessage(retryConnection);

  await postEvent(port, retryEvent, auth);
  assertNotification(await firstRetryMessage, retryEvent, "recipient-retry");
  await postEvent(port, retryEvent, auth);
  await noJsonMessage(retryConnection);

  const retryRows = await postgres.query(
    "SELECT count(*)::int AS count FROM notifications WHERE event_id = $1 AND recipient_id = $2",
    [retryEvent.id, "recipient-retry"],
  );
  assert.deepEqual(retryRows.rows, [{ count: 1 }]);

  const orderedConnection = await openWebSocket(
    `ws://127.0.0.1:${port}/connections/ws`,
    await auth.recipientHeaders("recipient-order"),
  );
  sockets.push(orderedConnection);
  await delay(100);

  const firstOrderedEvent = eventFixture({
    id: `evt_t2_order_first_${Date.now()}`,
    recipients: ["recipient-order"],
    title: "First ordered notification",
  });
  const secondOrderedEvent = eventFixture({
    id: `evt_t2_order_second_${Date.now()}`,
    recipients: ["recipient-order"],
    title: "Second ordered notification",
  });
  const firstOrderedMessage = nextJsonMessage(orderedConnection);

  await postEvent(port, firstOrderedEvent, auth);
  assertNotification(await firstOrderedMessage, firstOrderedEvent, "recipient-order");

  const secondOrderedMessage = nextJsonMessage(orderedConnection);
  await postEvent(port, secondOrderedEvent, auth);
  assertNotification(await secondOrderedMessage, secondOrderedEvent, "recipient-order");
} finally {
  for (const socket of sockets) {
    socket.close();
  }
  await postgres?.end();
  await service.close();
  await auth.stop();
}
