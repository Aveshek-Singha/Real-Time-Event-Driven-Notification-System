import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { buildService } from "../../apps/service/src/server.ts";
import { startAuthHarness } from "./auth-harness.mjs";

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
}

async function waitForInbox(port, recipientId, expectedCount, auth) {
  const url = `http://127.0.0.1:${port}/recipients/${recipientId}/inbox`;

  for (let attempt = 0; attempt < 60; attempt += 1) {
    const response = await fetch(url, {
      headers: await auth.recipientHeaders(recipientId),
    });

    assert.equal(response.status, 200);

    const body = await response.json();

    if (body.notifications.length === expectedCount) {
      return body;
    }

    await delay(500);
  }

  assert.fail(`Inbox for ${recipientId} did not reach ${expectedCount} Notifications`);
}

async function getJson(url, auth, recipientId, expectedStatus = 200) {
  const response = await fetch(url, {
    headers: await auth.recipientHeaders(recipientId),
  });
  const body = await response.json();

  assert.equal(response.status, expectedStatus, JSON.stringify(body));

  return body;
}

async function postJson(url, auth, recipientId, expectedStatus = 200) {
  const response = await fetch(url, {
    method: "POST",
    headers: await auth.recipientHeaders(recipientId),
  });
  const body = await response.json();

  assert.equal(response.status, expectedStatus, JSON.stringify(body));

  return body;
}

function eventFixture(overrides) {
  return {
    id: `evt_t3_${Date.now()}_${Math.random().toString(36).slice(2)}`,
    type: "order.shipped",
    recipients: [recipientId],
    title: "Order shipped",
    body: "Your order is on the way.",
    payload: { orderId: "order-1" },
    occurredAt: "2026-07-19T10:00:00.000Z",
    ...overrides,
  };
}

const port = Number(process.env.PORT);
assert.ok(port > 0, "PORT is required");
assert.ok(process.env.POSTGRES_URL, "POSTGRES_URL is required");

const recipientId = `recipient-inbox-${Date.now()}-${Math.random().toString(36).slice(2)}`;
const auth = await startAuthHarness();
const service = buildService();

try {
  await service.listen({ port, host: "127.0.0.1" });

  const olderEvent = eventFixture({
    id: `evt_t3_older_${Date.now()}`,
    title: "Older shipment",
    occurredAt: "2026-07-19T10:00:00.000Z",
  });
  const middleEvent = eventFixture({
    id: `evt_t3_middle_${Date.now()}`,
    title: "Middle shipment",
    occurredAt: "2026-07-19T11:00:00.000Z",
  });
  const newerEvent = eventFixture({
    id: `evt_t3_newer_${Date.now()}`,
    title: "Newer shipment",
    occurredAt: "2026-07-19T12:00:00.000Z",
  });

  await postEvent(port, olderEvent, auth);
  await postEvent(port, middleEvent, auth);
  await postEvent(port, newerEvent, auth);

  const inbox = await waitForInbox(port, recipientId, 3, auth);

  assert.deepEqual(
    inbox.notifications.map((notification) => ({
      eventId: notification.eventId,
      title: notification.title,
      read: notification.read,
      createdAt: notification.createdAt,
    })),
    [{
      eventId: newerEvent.id,
      title: "Newer shipment",
      read: false,
      createdAt: "2026-07-19T12:00:00.000Z",
    }, {
      eventId: middleEvent.id,
      title: "Middle shipment",
      read: false,
      createdAt: "2026-07-19T11:00:00.000Z",
    }, {
      eventId: olderEvent.id,
      title: "Older shipment",
      read: false,
      createdAt: "2026-07-19T10:00:00.000Z",
    }],
  );
  assert.deepEqual(inbox.page, {
    limit: 20,
    offset: 0,
    nextOffset: null,
  });

  const firstPage = await getJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox?limit=2`, auth, recipientId);
  assert.deepEqual(firstPage.notifications.map((notification) => notification.eventId), [
    newerEvent.id,
    middleEvent.id,
  ]);
  assert.deepEqual(firstPage.page, {
    limit: 2,
    offset: 0,
    nextOffset: 2,
  });

  const secondPage = await getJson(
    `http://127.0.0.1:${port}/recipients/${recipientId}/inbox?limit=2&offset=2`,
    auth,
    recipientId,
  );
  assert.deepEqual(secondPage.notifications.map((notification) => notification.eventId), [olderEvent.id]);
  assert.deepEqual(secondPage.page, {
    limit: 2,
    offset: 2,
    nextOffset: null,
  });

  assert.deepEqual(
    await getJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox/unread-count`, auth, recipientId),
    { recipientId, unread: 3 },
  );

  const readResult = await postJson(
    `http://127.0.0.1:${port}/recipients/${recipientId}/inbox/${firstPage.notifications[1].id}/read`,
    auth,
    recipientId,
  );
  assert.equal(readResult.notification.eventId, middleEvent.id);
  assert.equal(readResult.notification.read, true);

  const idempotentReadResult = await postJson(
    `http://127.0.0.1:${port}/recipients/${recipientId}/inbox/${firstPage.notifications[1].id}/read`,
    auth,
    recipientId,
  );
  assert.equal(idempotentReadResult.notification.eventId, middleEvent.id);
  assert.equal(idempotentReadResult.notification.read, true);

  assert.deepEqual(
    await getJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox/unread-count`, auth, recipientId),
    { recipientId, unread: 2 },
  );

  const afterRead = await getJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox`, auth, recipientId);
  assert.deepEqual(afterRead.notifications.map((notification) => ({
    eventId: notification.eventId,
    read: notification.read,
  })), [{
    eventId: newerEvent.id,
    read: false,
  }, {
    eventId: middleEvent.id,
    read: true,
  }, {
    eventId: olderEvent.id,
    read: false,
  }]);

  const unreadOnly = await getJson(
    `http://127.0.0.1:${port}/recipients/${recipientId}/inbox?unreadOnly=true`,
    auth,
    recipientId,
  );
  assert.deepEqual(unreadOnly.notifications.map((notification) => notification.eventId), [
    newerEvent.id,
    olderEvent.id,
  ]);

  assert.deepEqual(
    await postJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox/read-all`, auth, recipientId),
    { recipientId, markedRead: 2 },
  );
  assert.deepEqual(
    await getJson(`http://127.0.0.1:${port}/recipients/${recipientId}/inbox/unread-count`, auth, recipientId),
    { recipientId, unread: 0 },
  );

  const emptyUnreadOnly = await getJson(
    `http://127.0.0.1:${port}/recipients/${recipientId}/inbox?unreadOnly=true`,
    auth,
    recipientId,
  );
  assert.deepEqual(emptyUnreadOnly.notifications, []);
} finally {
  await service.close();
  await auth.stop();
}
