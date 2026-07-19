import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { buildService } from "../../apps/service/src/server.ts";
import WebSocket from "ws";

async function openWebSocket(url) {
  const socket = new WebSocket(url);

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

async function nextWebSocketJson(socket) {
  return await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("WebSocket message timed out")), 30_000);

    socket.once("message", (data) => {
      clearTimeout(timeout);
      resolve(JSON.parse(data.toString()));
    });
  });
}

async function noWebSocketJson(socket) {
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, 500);

    socket.once("message", (data) => {
      clearTimeout(timeout);
      reject(new Error(`unexpected WebSocket message: ${data.toString()}`));
    });
  });
}

async function openSse(url) {
  const abortController = new AbortController();
  const response = await fetch(url, {
    headers: { accept: "text/event-stream" },
    signal: abortController.signal,
  });

  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type")?.startsWith("text/event-stream"), true);

  return {
    abortController,
    buffer: "",
    reader: response.body.getReader(),
  };
}

async function nextSseJson(connection) {
  const decoder = new TextDecoder();
  const timeout = AbortSignal.timeout(30_000);

  while (!timeout.aborted) {
    const eventEnd = connection.buffer.indexOf("\n\n");

    if (eventEnd >= 0) {
      const rawEvent = connection.buffer.slice(0, eventEnd);
      connection.buffer = connection.buffer.slice(eventEnd + 2);
      const data = rawEvent
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice("data:".length).trimStart())
        .join("\n");

      if (data) {
        return JSON.parse(data);
      }
    }

    const { done, value } = await Promise.race([
      connection.reader.read(),
      new Promise((_, reject) => {
        timeout.addEventListener("abort", () => reject(new Error("SSE message timed out")), { once: true });
      }),
    ]);

    if (done) {
      throw new Error("SSE stream ended before a Notification arrived");
    }

    connection.buffer += decoder.decode(value, { stream: true });
  }

  throw new Error("SSE message timed out");
}

async function postEvent(port, event) {
  const response = await fetch(`http://127.0.0.1:${port}/events`, {
    method: "POST",
    headers: { "content-type": "application/json" },
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
    id: `evt_t4_${Date.now()}_${Math.random().toString(36).slice(2)}`,
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
  assert.equal(notification.body, event.body);
  assert.deepEqual(notification.payload, event.payload);
  assert.equal(notification.read, false);
  assert.equal(notification.createdAt, event.occurredAt);
}

const port = Number(process.env.PORT);
assert.ok(port > 0, "PORT is required");
assert.ok(process.env.POSTGRES_URL, "POSTGRES_URL is required");

const service = buildService();
const sockets = [];
const sseConnections = [];

try {
  await service.listen({ port, host: "127.0.0.1" });

  const firstWebSocket = await openWebSocket(`ws://127.0.0.1:${port}/connections/ws?recipientId=recipient-mixed`);
  const secondWebSocket = await openWebSocket(`ws://127.0.0.1:${port}/connections/ws?recipientId=recipient-mixed`);
  const sse = await openSse(`http://127.0.0.1:${port}/connections/sse?recipientId=recipient-mixed`);
  sockets.push(firstWebSocket, secondWebSocket);
  sseConnections.push(sse);
  await delay(100);

  const mixedEvent = eventFixture({
    recipients: ["recipient-mixed"],
    title: "Mixed transport shipment",
  });
  const messages = [
    nextWebSocketJson(firstWebSocket),
    nextWebSocketJson(secondWebSocket),
    nextSseJson(sse),
  ];

  await postEvent(port, mixedEvent);

  const [firstWsNotification, secondWsNotification, sseNotification] = await Promise.all(messages);
  assertNotification(firstWsNotification, mixedEvent, "recipient-mixed");
  assert.deepEqual(secondWsNotification, firstWsNotification);
  assert.deepEqual(sseNotification, firstWsNotification);

  firstWebSocket.close();
  await delay(100);

  const afterDisconnectEvent = eventFixture({
    recipients: ["recipient-mixed"],
    title: "After one disconnect",
  });
  const survivingMessages = [
    nextWebSocketJson(secondWebSocket),
    nextSseJson(sse),
  ];

  await postEvent(port, afterDisconnectEvent);

  const [survivingWsNotification, survivingSseNotification] = await Promise.all(survivingMessages);
  assertNotification(survivingWsNotification, afterDisconnectEvent, "recipient-mixed");
  assert.deepEqual(survivingSseNotification, survivingWsNotification);
  await noWebSocketJson(firstWebSocket);

  const orderedWebSocket = await openWebSocket(`ws://127.0.0.1:${port}/connections/ws?recipientId=recipient-order`);
  const orderedSse = await openSse(`http://127.0.0.1:${port}/connections/sse?recipientId=recipient-order`);
  sockets.push(orderedWebSocket);
  sseConnections.push(orderedSse);
  await delay(100);

  const firstOrderedEvent = eventFixture({
    id: `evt_t4_order_first_${Date.now()}`,
    recipients: ["recipient-order"],
    title: "First ordered notification",
    occurredAt: "2026-07-19T10:00:00.000Z",
  });
  const secondOrderedEvent = eventFixture({
    id: `evt_t4_order_second_${Date.now()}`,
    recipients: ["recipient-order"],
    title: "Second ordered notification",
    occurredAt: "2026-07-19T10:01:00.000Z",
  });
  const firstOrderedMessages = [
    nextWebSocketJson(orderedWebSocket),
    nextSseJson(orderedSse),
  ];

  await postEvent(port, firstOrderedEvent);

  const [firstOrderedWsNotification, firstOrderedSseNotification] = await Promise.all(firstOrderedMessages);
  assertNotification(firstOrderedWsNotification, firstOrderedEvent, "recipient-order");
  assert.deepEqual(firstOrderedSseNotification, firstOrderedWsNotification);

  const secondOrderedMessages = [
    nextWebSocketJson(orderedWebSocket),
    nextSseJson(orderedSse),
  ];

  await postEvent(port, secondOrderedEvent);

  const [secondOrderedWsNotification, secondOrderedSseNotification] = await Promise.all(secondOrderedMessages);
  assertNotification(secondOrderedWsNotification, secondOrderedEvent, "recipient-order");
  assert.deepEqual(secondOrderedSseNotification, secondOrderedWsNotification);
} finally {
  for (const socket of sockets) {
    socket.close();
  }
  for (const connection of sseConnections) {
    connection.abortController.abort();
  }
  await service.close();
}
