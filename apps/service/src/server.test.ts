import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { buildService } from "./server.js";
import type { Notification } from "@notification-system/contracts";

const service = buildService();

after(async () => {
  await service.close();
});

describe("service hello world", () => {
  it("reports health at the HTTP edge", async () => {
    const response = await service.inject({ method: "GET", url: "/health" });

    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), { status: "ok" });
  });

  it("rejects malformed Events with validation details", async () => {
    const response = await service.inject({
      method: "POST",
      url: "/events",
      payload: { id: "evt_1", type: "demo.event", recipients: [] },
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error, "invalid_event");
    assert.match(JSON.stringify(response.json().details), /recipients/);
  });

  it("rejects Events addressed to the same Recipient more than once", async () => {
    const response = await service.inject({
      method: "POST",
      url: "/events",
      payload: {
        id: "evt_duplicate_recipient",
        type: "demo.event",
        recipients: ["recipient-a", "recipient-a"],
        title: "Duplicate recipient",
        body: "This Event addresses one Recipient twice.",
        payload: { example: true },
        occurredAt: "2026-07-19T10:00:00.000Z",
      },
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error, "invalid_event");
    assert.match(JSON.stringify(response.json().details), /unique/);
  });
});

describe("T3 Inbox REST", () => {
  function notificationFixture(overrides: Partial<Notification> = {}): Notification {
    return {
      id: "ntf_evt_1_recipient-a",
      eventId: "evt_1",
      recipientId: "recipient-a",
      type: "order.shipped",
      title: "Order shipped",
      body: "Your order is on the way.",
      payload: { orderId: "order-1" },
      read: false,
      createdAt: "2026-07-19T10:00:00.000Z",
      ...overrides,
    };
  }

  function buildInboxStore(seed: Notification[]) {
    const notifications = seed.map((notification) => ({ ...notification }));

    return {
      async migrate() {},
      async persist(notification: Notification) {
        if (notifications.some((stored) =>
          stored.eventId === notification.eventId && stored.recipientId === notification.recipientId
        )) {
          return false;
        }

        notifications.push({ ...notification });
        return true;
      },
      async list(recipientId: string, options: { limit: number; offset: number; unreadOnly: boolean }) {
        const filtered = notifications
          .filter((notification) => notification.recipientId === recipientId)
          .filter((notification) => !options.unreadOnly || !notification.read)
          .sort((left, right) => right.createdAt.localeCompare(left.createdAt));
        const page = filtered.slice(options.offset, options.offset + options.limit);
        const nextOffset = filtered.length > options.offset + options.limit
          ? options.offset + options.limit
          : null;

        return { notifications: page, nextOffset };
      },
      async unreadCount(recipientId: string) {
        return notifications.filter((notification) =>
          notification.recipientId === recipientId && !notification.read
        ).length;
      },
      async markRead(recipientId: string, notificationId: string) {
        const notification = notifications.find((stored) =>
          stored.recipientId === recipientId && stored.id === notificationId
        );

        if (!notification) {
          return undefined;
        }

        notification.read = true;
        return notification;
      },
      async markAllRead(recipientId: string) {
        let markedRead = 0;

        for (const notification of notifications) {
          if (notification.recipientId === recipientId && !notification.read) {
            notification.read = true;
            markedRead += 1;
          }
        }

        return markedRead;
      },
      async close() {},
    };
  }

  it("serves paginated and unread-only Inbox pages at the HTTP edge", async () => {
    const inboxStore = buildInboxStore([
      notificationFixture({
        id: "ntf_older",
        eventId: "evt_older",
        title: "Older shipment",
        createdAt: "2026-07-19T10:00:00.000Z",
      }),
      notificationFixture({
        id: "ntf_middle",
        eventId: "evt_middle",
        title: "Middle shipment",
        read: true,
        createdAt: "2026-07-19T11:00:00.000Z",
      }),
      notificationFixture({
        id: "ntf_newer",
        eventId: "evt_newer",
        title: "Newer shipment",
        createdAt: "2026-07-19T12:00:00.000Z",
      }),
    ]);
    const inboxService = buildService({ inboxStore });

    try {
      const page = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox?limit=2",
      });

      assert.equal(page.statusCode, 200);
      assert.deepEqual(page.json().notifications.map((notification: Notification) => notification.eventId), [
        "evt_newer",
        "evt_middle",
      ]);
      assert.deepEqual(page.json().page, { limit: 2, offset: 0, nextOffset: 2 });

      const unreadOnly = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox?unreadOnly=true",
      });

      assert.equal(unreadOnly.statusCode, 200);
      assert.deepEqual(unreadOnly.json().notifications.map((notification: Notification) => notification.eventId), [
        "evt_newer",
        "evt_older",
      ]);
    } finally {
      await inboxService.close();
    }
  });

  it("serves unread count, mark-read, and mark-all-read at the HTTP edge", async () => {
    const inboxStore = buildInboxStore([
      notificationFixture({
        id: "ntf_older",
        eventId: "evt_older",
      }),
      notificationFixture({
        id: "ntf_newer",
        eventId: "evt_newer",
        createdAt: "2026-07-19T12:00:00.000Z",
      }),
    ]);
    const inboxService = buildService({ inboxStore });

    try {
      const initialCount = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
      });
      assert.deepEqual(initialCount.json(), { recipientId: "recipient-a", unread: 2 });

      const markRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/ntf_newer/read",
      });
      assert.equal(markRead.statusCode, 200);
      assert.equal(markRead.json().notification.read, true);

      const idempotentMarkRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/ntf_newer/read",
      });
      assert.equal(idempotentMarkRead.statusCode, 200);
      assert.equal(idempotentMarkRead.json().notification.read, true);

      const afterOneRead = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
      });
      assert.deepEqual(afterOneRead.json(), { recipientId: "recipient-a", unread: 1 });

      const markAllRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/read-all",
      });
      assert.deepEqual(markAllRead.json(), { recipientId: "recipient-a", markedRead: 1 });

      const afterAllRead = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
      });
      assert.deepEqual(afterAllRead.json(), { recipientId: "recipient-a", unread: 0 });
    } finally {
      await inboxService.close();
    }
  });
});
