import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Notification } from "@notification-system/contracts";
import {
  buildLiveConnectionUrls,
  buildLoginUrl,
  createNotificationCenterClient,
  reduceNotificationCenter,
  resolveSessionFromUrl,
} from "./notification-center";

const token = [
  "header",
  Buffer.from(JSON.stringify({ recipient_id: "recipient-a" })).toString("base64url"),
  "signature",
].join(".");

function notification(overrides: Partial<Notification> = {}): Notification {
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

describe("T7 notification center client", () => {
  it("resolves the logged-in Recipient from a Keycloak access token callback", () => {
    const session = resolveSessionFromUrl(`http://localhost:3002/#access_token=${token}`);

    assert.deepEqual(session, {
      accessToken: token,
      recipientId: "recipient-a",
    });
  });

  it("builds Keycloak and live transport URLs from public configuration", () => {
    const loginUrl = buildLoginUrl({
      authorizationUrl: "http://localhost:8080/realms/notifications/protocol/openid-connect/auth",
      clientId: "notification-web",
      redirectUri: "http://localhost:3002/",
    });

    assert.equal(loginUrl.searchParams.get("client_id"), "notification-web");
    assert.equal(loginUrl.searchParams.get("response_type"), "token");
    assert.equal(loginUrl.searchParams.get("scope"), "openid profile");

    const urls = buildLiveConnectionUrls({
      apiBaseUrl: "http://localhost:3001",
      accessToken: "recipient token",
    });

    assert.equal(urls.websocket, "ws://localhost:3001/connections/ws?access_token=recipient+token");
    assert.equal(urls.sse, "http://localhost:3001/connections/sse?access_token=recipient+token");
  });

  it("uses the token Recipient identity for Inbox REST commands and parses contract payloads", async () => {
    const requests: Array<{ method: string; url: string; authorization: string | null }> = [];
    const fetcher: typeof fetch = async (input, init) => {
      const url = input.toString();
      const method = init?.method ?? "GET";
      const headers = new Headers(init?.headers);
      requests.push({ method, url, authorization: headers.get("authorization") });

      if (url.endsWith("/inbox")) {
        return Response.json({
          notifications: [notification()],
          page: { limit: 20, offset: 0, nextOffset: null },
        });
      }

      if (url.endsWith("/unread-count")) {
        return Response.json({ recipientId: "recipient-a", unread: 1 });
      }

      if (url.endsWith("/read-all")) {
        return Response.json({ recipientId: "recipient-a", markedRead: 1 });
      }

      return Response.json({ notification: notification({ read: true }) });
    };

    const client = createNotificationCenterClient({
      apiBaseUrl: "http://localhost:3001",
      accessToken: "recipient-token",
      recipientId: "recipient-a",
      fetcher,
    });

    assert.equal((await client.fetchInbox()).notifications[0]?.title, "Order shipped");
    assert.equal((await client.fetchUnreadCount()).unread, 1);
    assert.equal((await client.markRead("ntf_evt_1_recipient-a")).notification.read, true);
    assert.equal((await client.markAllRead()).markedRead, 1);
    assert.deepEqual(requests, [
      {
        method: "GET",
        url: "http://localhost:3001/recipients/recipient-a/inbox",
        authorization: "Bearer recipient-token",
      },
      {
        method: "GET",
        url: "http://localhost:3001/recipients/recipient-a/inbox/unread-count",
        authorization: "Bearer recipient-token",
      },
      {
        method: "POST",
        url: "http://localhost:3001/recipients/recipient-a/inbox/ntf_evt_1_recipient-a/read",
        authorization: "Bearer recipient-token",
      },
      {
        method: "POST",
        url: "http://localhost:3001/recipients/recipient-a/inbox/read-all",
        authorization: "Bearer recipient-token",
      },
    ]);
  });

  it("dedupes at-least-once live pushes and keeps unread state consistent", () => {
    const initial = reduceNotificationCenter(undefined, {
      type: "backlog_loaded",
      notifications: [notification()],
      unread: 1,
    });
    const duplicatePush = reduceNotificationCenter(initial, {
      type: "notification_arrived",
      notification: notification({ title: "Order shipped again" }),
    });
    const secondPush = reduceNotificationCenter(duplicatePush, {
      type: "notification_arrived",
      notification: notification({
        id: "ntf_evt_2_recipient-a",
        eventId: "evt_2",
        title: "Order delivered",
        createdAt: "2026-07-19T11:00:00.000Z",
      }),
    });
    const oneRead = reduceNotificationCenter(secondPush, {
      type: "notification_marked_read",
      notificationId: "ntf_evt_2_recipient-a",
    });
    const allRead = reduceNotificationCenter(oneRead, { type: "all_notifications_marked_read" });

    assert.deepEqual(duplicatePush.notifications.map((stored) => stored.title), ["Order shipped"]);
    assert.equal(duplicatePush.unread, 1);
    assert.deepEqual(secondPush.notifications.map((stored) => stored.eventId), ["evt_2", "evt_1"]);
    assert.equal(secondPush.unread, 2);
    assert.equal(oneRead.unread, 1);
    assert.deepEqual(allRead.notifications.map((stored) => stored.read), [true, true]);
    assert.equal(allRead.unread, 0);
  });
});
