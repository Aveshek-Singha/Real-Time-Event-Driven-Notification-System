import assert from "node:assert/strict";
import { after, describe, it } from "node:test";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { once } from "node:events";
import { buildService } from "./server.js";
import type { Event, Notification } from "@notification-system/contracts";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import WebSocket from "ws";

const producerAuthHeaders = { authorization: "Bearer test-producer" };

function recipientAuthHeaders(recipientId: string) {
  return { authorization: `Bearer test-recipient:${recipientId}` };
}

function buildTestAuthenticator() {
  return {
    async authenticateProducer(request: { headers: { authorization?: string } }) {
      if (!request.headers.authorization) {
        return { ok: false, statusCode: 401, error: "invalid_token" } as const;
      }

      if (request.headers.authorization !== producerAuthHeaders.authorization) {
        return { ok: false, statusCode: 403, error: "producer_token_required" } as const;
      }

      return { ok: true } as const;
    },
    async authenticateRecipient(request: { headers: { authorization?: string } }) {
      if (!request.headers.authorization) {
        return { ok: false, statusCode: 401, error: "invalid_token" } as const;
      }

      const prefix = "Bearer test-recipient:";

      if (!request.headers.authorization.startsWith(prefix)) {
        return { ok: false, statusCode: 403, error: "recipient_token_required" } as const;
      }

      return {
        ok: true,
        recipientId: request.headers.authorization.slice(prefix.length),
      } as const;
    },
  };
}

const service = buildService({ authenticator: buildTestAuthenticator() });

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
      headers: producerAuthHeaders,
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
      headers: producerAuthHeaders,
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

describe("T6 Prometheus metrics", () => {
  function buildPipeline() {
    return {
      async publish(event: Event) {
        return event.recipients.length;
      },
      async close() {},
    };
  }

  function eventFixture(overrides: Partial<Event> = {}): Event {
    return {
      id: "evt_metrics",
      type: "order.shipped",
      recipients: ["recipient-a"],
      title: "Order shipped",
      body: "Your order is on the way.",
      payload: { orderId: "order-1" },
      occurredAt: "2026-07-19T10:00:00.000Z",
      ...overrides,
    };
  }

  function metricValue(body: string, name: string) {
    const match = new RegExp(`^${name} (\\d+(?:\\.\\d+)?)$`, "m").exec(body);

    assert.ok(match, `expected ${name} in metrics body`);

    return Number(match[1]);
  }

  it("exports the promised metric families at the HTTP edge", async () => {
    const metricsService = buildService({ authenticator: buildTestAuthenticator() });

    try {
      const response = await metricsService.inject({ method: "GET", url: "/metrics" });
      const body = response.body;

      assert.equal(response.statusCode, 200);
      assert.equal(response.headers["content-type"]?.toString().startsWith("text/plain"), true);
      assert.match(body, /^# HELP notification_ingest_events_total /m);
      assert.match(body, /^# TYPE notification_ingest_events_total counter$/m);
      assert.match(body, /^# HELP notification_delivery_notifications_total /m);
      assert.match(body, /^# TYPE notification_delivery_notifications_total counter$/m);
      assert.match(body, /^# HELP notification_consumer_lag_messages /m);
      assert.match(body, /^# TYPE notification_consumer_lag_messages gauge$/m);
      assert.match(body, /^# HELP notification_dlq_depth_messages /m);
      assert.match(body, /^# TYPE notification_dlq_depth_messages gauge$/m);
      assert.match(body, /^# HELP notification_live_connections /m);
      assert.match(body, /^# TYPE notification_live_connections gauge$/m);
    } finally {
      await metricsService.close();
    }
  });

  it("updates ingest and live Connection metrics from public service activity", async () => {
    const metricsService = buildService({
      authenticator: buildTestAuthenticator(),
      pipeline: buildPipeline(),
    });

    try {
      const address = await metricsService.listen({ port: 0, host: "127.0.0.1" });
      const socket = new WebSocket(address.replace("http://", "ws://") + "/connections/ws", {
        headers: recipientAuthHeaders("recipient-a"),
      });

      await once(socket, "open");
      await delay(50);

      const connectedMetrics = await fetch(`${address}/metrics`);
      assert.equal(metricValue(await connectedMetrics.text(), "notification_live_connections"), 1);

      const eventResponse = await fetch(`${address}/events`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...producerAuthHeaders,
        },
        body: JSON.stringify(eventFixture()),
      });

      assert.equal(eventResponse.status, 202, await eventResponse.text());

      const afterEventMetrics = await fetch(`${address}/metrics`);
      assert.equal(metricValue(await afterEventMetrics.text(), "notification_ingest_events_total"), 1);

      socket.close();
      await once(socket, "close");
      await delay(50);

      const disconnectedMetrics = await fetch(`${address}/metrics`);
      assert.equal(metricValue(await disconnectedMetrics.text(), "notification_live_connections"), 0);
    } finally {
      await metricsService.close();
    }
  });
});

describe("T5 producer auth", () => {
  function eventFixture(overrides: Partial<Event> = {}): Event {
    return {
      id: "evt_auth_producer",
      type: "order.shipped",
      recipients: ["recipient-a"],
      title: "Order shipped",
      body: "Your order is on the way.",
      payload: { orderId: "order-1" },
      occurredAt: "2026-07-19T10:00:00.000Z",
      ...overrides,
    };
  }

  function buildPipeline() {
    return {
      published: [] as Event[],
      async publish(event: Event) {
        this.published.push(event);
        return event.recipients.length;
      },
      async close() {},
    };
  }

  function buildAuthenticator() {
    return {
      async authenticateProducer(request: { headers: { authorization?: string } }) {
        if (!request.headers.authorization) {
          return { ok: false, statusCode: 401, error: "invalid_token" } as const;
        }

        if (request.headers.authorization !== "Bearer producer-token") {
          return { ok: false, statusCode: 403, error: "producer_token_required" } as const;
        }

        return { ok: true } as const;
      },
      async authenticateRecipient() {
        return { ok: false, statusCode: 401, error: "invalid_token" } as const;
      },
    };
  }

  it("requires a valid producer token before accepting Events", async () => {
    const pipeline = buildPipeline();
    const authenticatedService = buildService({
      pipeline,
      authenticator: buildAuthenticator(),
    });

    try {
      const missingToken = await authenticatedService.inject({
        method: "POST",
        url: "/events",
        payload: eventFixture(),
      });
      assert.equal(missingToken.statusCode, 401);
      assert.deepEqual(missingToken.json(), { error: "invalid_token" });

      const recipientToken = await authenticatedService.inject({
        method: "POST",
        url: "/events",
        headers: { authorization: "Bearer recipient-token" },
        payload: eventFixture(),
      });
      assert.equal(recipientToken.statusCode, 403);
      assert.deepEqual(recipientToken.json(), { error: "producer_token_required" });

      const producerToken = await authenticatedService.inject({
        method: "POST",
        url: "/events",
        headers: { authorization: "Bearer producer-token" },
        payload: eventFixture(),
      });
      assert.equal(producerToken.statusCode, 202);
      assert.deepEqual(producerToken.json(), {
        accepted: true,
        eventId: "evt_auth_producer",
        notifications: 1,
      });
      assert.deepEqual(pipeline.published.map((event) => event.id), ["evt_auth_producer"]);
    } finally {
      await authenticatedService.close();
    }
  });
});

describe("T5 recipient REST auth", () => {
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
        notifications.push({ ...notification });
        return true;
      },
      async list(recipientId: string) {
        return {
          notifications: notifications.filter((notification) => notification.recipientId === recipientId),
          nextOffset: null,
        };
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

  function buildAuthenticator() {
    return {
      async authenticateProducer() {
        return { ok: false, statusCode: 401, error: "invalid_token" } as const;
      },
      async authenticateRecipient(request: { headers: { authorization?: string } }) {
        if (!request.headers.authorization) {
          return { ok: false, statusCode: 401, error: "invalid_token" } as const;
        }

        if (request.headers.authorization === "Bearer recipient-a-token") {
          return { ok: true, recipientId: "recipient-a" } as const;
        }

        if (request.headers.authorization === "Bearer recipient-b-token") {
          return { ok: true, recipientId: "recipient-b" } as const;
        }

        return { ok: false, statusCode: 403, error: "recipient_token_required" } as const;
      },
    };
  }

  it("uses the token Recipient identity for Inbox REST access", async () => {
    const inboxService = buildService({
      authenticator: buildAuthenticator(),
      inboxStore: buildInboxStore([
        notificationFixture(),
        notificationFixture({
          id: "ntf_evt_2_recipient-b",
          eventId: "evt_2",
          recipientId: "recipient-b",
        }),
      ]),
    });

    try {
      const missingToken = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
      });
      assert.equal(missingToken.statusCode, 401);
      assert.deepEqual(missingToken.json(), { error: "invalid_token" });

      const wrongRecipient = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
        headers: { authorization: "Bearer recipient-b-token" },
      });
      assert.equal(wrongRecipient.statusCode, 403);
      assert.deepEqual(wrongRecipient.json(), { error: "recipient_forbidden" });

      const ownInbox = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
        headers: { authorization: "Bearer recipient-a-token" },
      });
      assert.equal(ownInbox.statusCode, 200);
      assert.deepEqual(ownInbox.json().notifications.map((notification: Notification) => notification.recipientId), [
        "recipient-a",
      ]);

      const crossRecipientMarkRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/ntf_evt_1_recipient-a/read",
        headers: { authorization: "Bearer recipient-b-token" },
      });
      assert.equal(crossRecipientMarkRead.statusCode, 403);
      assert.deepEqual(crossRecipientMarkRead.json(), { error: "recipient_forbidden" });

      const ownUnreadCount = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
        headers: { authorization: "Bearer recipient-a-token" },
      });
      assert.equal(ownUnreadCount.statusCode, 200);
      assert.deepEqual(ownUnreadCount.json(), { recipientId: "recipient-a", unread: 1 });
    } finally {
      await inboxService.close();
    }
  });
});

describe("T5 live Connection auth", () => {
  function buildAuthenticator() {
    return {
      async authenticateProducer() {
        return { ok: false, statusCode: 401, error: "invalid_token" } as const;
      },
      async authenticateRecipient(request: { headers: { authorization?: string } }) {
        if (!request.headers.authorization) {
          return { ok: false, statusCode: 401, error: "invalid_token" } as const;
        }

        if (request.headers.authorization === "Bearer recipient-a-token") {
          return { ok: true, recipientId: "recipient-a" } as const;
        }

        return { ok: false, statusCode: 403, error: "recipient_token_required" } as const;
      },
    };
  }

  async function expectWebSocketRejected(url: string, expectedStatus: number) {
    await new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(url);

      socket.once("open", () => {
        socket.close();
        reject(new Error("WebSocket unexpectedly opened"));
      });
      socket.once("unexpected-response", (_request, response) => {
        try {
          assert.equal(response.statusCode, expectedStatus);
          resolve();
        } catch (error) {
          reject(error);
        }
      });
      socket.once("error", reject);
    });
  }

  async function openAuthorizedWebSocket(url: string) {
    const socket = new WebSocket(url, {
      headers: { authorization: "Bearer recipient-a-token" },
    });

    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    await delay(50);
    assert.equal(socket.readyState, WebSocket.OPEN);

    return socket;
  }

  it("authenticates WS and SSE handshakes from recipient tokens", async () => {
    const connectionService = buildService({ authenticator: buildAuthenticator() });

    try {
      const address = await connectionService.listen({ port: 0, host: "127.0.0.1" });
      const wsAddress = address.replace("http://", "ws://");

      const missingSseToken = await fetch(`${address}/connections/sse?recipientId=recipient-a`, {
        headers: { accept: "text/event-stream" },
      });
      assert.equal(missingSseToken.status, 401);
      assert.deepEqual(await missingSseToken.json(), { error: "invalid_token" });

      await expectWebSocketRejected(`${wsAddress}/connections/ws?recipientId=recipient-a`, 401);

      const abortController = new AbortController();
      const authorizedSse = await fetch(`${address}/connections/sse`, {
        headers: {
          accept: "text/event-stream",
          authorization: "Bearer recipient-a-token",
        },
        signal: abortController.signal,
      });
      abortController.abort();
      assert.equal(authorizedSse.status, 200);
      assert.equal(authorizedSse.headers.get("content-type")?.startsWith("text/event-stream"), true);

      const authorizedWebSocket = await openAuthorizedWebSocket(`${wsAddress}/connections/ws`);
      authorizedWebSocket.close();
    } finally {
      await connectionService.close();
    }
  });
});

describe("T5 OIDC JWKS validation", () => {
  function eventFixture(): Event {
    return {
      id: "evt_oidc_producer",
      type: "order.shipped",
      recipients: ["recipient-a"],
      title: "Order shipped",
      body: "Your order is on the way.",
      payload: { orderId: "order-1" },
      occurredAt: "2026-07-19T10:00:00.000Z",
    };
  }

  function buildPipeline() {
    return {
      async publish(event: Event) {
        return event.recipients.length;
      },
      async close() {},
    };
  }

  function buildInboxStore(seed: Notification[]) {
    return {
      async migrate() {},
      async persist() {
        return true;
      },
      async list(recipientId: string) {
        return {
          notifications: seed.filter((notification) => notification.recipientId === recipientId),
          nextOffset: null,
        };
      },
      async unreadCount() {
        return 0;
      },
      async markRead() {
        return undefined;
      },
      async markAllRead() {
        return 0;
      },
      async close() {},
    };
  }

  async function startOidcHarness() {
    const issuer = "http://issuer.example.test/realms/notifications";
    const audience = "notification-service";
    const keyId = "test-key";
    const { publicKey, privateKey } = await generateKeyPair("RS256");
    const jwk = await exportJWK(publicKey);
    const jwks = {
      keys: [{
        ...jwk,
        alg: "RS256",
        kid: keyId,
        use: "sig",
      }],
    };
    const jwksServer = createServer((request, response) => {
      if (request.url !== "/jwks") {
        response.writeHead(404).end();
        return;
      }

      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(jwks));
    });

    jwksServer.listen(0, "127.0.0.1");
    await once(jwksServer, "listening");
    const address = jwksServer.address();
    assert.ok(address && typeof address === "object");
    const jwksUri = `http://127.0.0.1:${address.port}/jwks`;

    return {
      audience,
      issuer,
      jwksUri,
      async producerToken(overrides: Record<string, unknown> = {}, tokenAudience = audience) {
        return await new SignJWT({ azp: "notification-producer", ...overrides })
          .setProtectedHeader({ alg: "RS256", kid: keyId })
          .setIssuedAt()
          .setIssuer(issuer)
          .setAudience(tokenAudience)
          .setExpirationTime("5m")
          .sign(privateKey);
      },
      async recipientToken(recipientId: string, overrides: Record<string, unknown> = {}, tokenAudience = audience) {
        return await new SignJWT({
          azp: "notification-web",
          recipient_id: recipientId,
          ...overrides,
        })
          .setProtectedHeader({ alg: "RS256", kid: keyId })
          .setIssuedAt()
          .setIssuer(issuer)
          .setAudience(tokenAudience)
          .setExpirationTime("5m")
          .sign(privateKey);
      },
      async stop() {
        jwksServer.close();
        await once(jwksServer, "close");
      },
    };
  }

  function withOidcEnvironment(auth: { audience: string; issuer: string; jwksUri: string }) {
    const previous = {
      OIDC_AUDIENCE: process.env.OIDC_AUDIENCE,
      OIDC_ISSUER: process.env.OIDC_ISSUER,
      OIDC_JWKS_URI: process.env.OIDC_JWKS_URI,
      OIDC_PRODUCER_CLIENT_ID: process.env.OIDC_PRODUCER_CLIENT_ID,
      OIDC_RECIPIENT_CLIENT_ID: process.env.OIDC_RECIPIENT_CLIENT_ID,
      OIDC_RECIPIENT_ID_CLAIM: process.env.OIDC_RECIPIENT_ID_CLAIM,
    };

    process.env.OIDC_AUDIENCE = auth.audience;
    process.env.OIDC_ISSUER = auth.issuer;
    process.env.OIDC_JWKS_URI = auth.jwksUri;
    process.env.OIDC_PRODUCER_CLIENT_ID = "notification-producer";
    process.env.OIDC_RECIPIENT_CLIENT_ID = "notification-web";
    process.env.OIDC_RECIPIENT_ID_CLAIM = "recipient_id";

    return () => {
      for (const [name, value] of Object.entries(previous)) {
        if (value === undefined) {
          delete process.env[name];
        } else {
          process.env[name] = value;
        }
      }
    };
  }

  it("validates producer and recipient access tokens through configured JWKS", async () => {
    const auth = await startOidcHarness();
    const restoreEnvironment = withOidcEnvironment(auth);
    const oidcService = buildService({
      inboxStore: buildInboxStore([{
        id: "ntf_evt_1_recipient-a",
        eventId: "evt_1",
        recipientId: "recipient-a",
        type: "order.shipped",
        title: "Order shipped",
        body: "Your order is on the way.",
        payload: { orderId: "order-1" },
        read: false,
        createdAt: "2026-07-19T10:00:00.000Z",
      }]),
      pipeline: buildPipeline(),
    });

    try {
      const missingProducerToken = await oidcService.inject({
        method: "POST",
        url: "/events",
        payload: eventFixture(),
      });
      assert.equal(missingProducerToken.statusCode, 401);
      assert.deepEqual(missingProducerToken.json(), { error: "invalid_token" });

      const wrongAudienceToken = await auth.producerToken({}, "other-service");
      const wrongAudienceProducer = await oidcService.inject({
        method: "POST",
        url: "/events",
        headers: { authorization: `Bearer ${wrongAudienceToken}` },
        payload: eventFixture(),
      });
      assert.equal(wrongAudienceProducer.statusCode, 401);
      assert.deepEqual(wrongAudienceProducer.json(), { error: "invalid_token" });

      const accepted = await oidcService.inject({
        method: "POST",
        url: "/events",
        headers: { authorization: `Bearer ${await auth.producerToken()}` },
        payload: eventFixture(),
      });
      assert.equal(accepted.statusCode, 202);

      const ownInbox = await oidcService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
        headers: { authorization: `Bearer ${await auth.recipientToken("recipient-a")}` },
      });
      assert.equal(ownInbox.statusCode, 200);
      assert.deepEqual(ownInbox.json().notifications.map((notification: Notification) => notification.recipientId), [
        "recipient-a",
      ]);

      const queryTokenInbox = await oidcService.inject({
        method: "GET",
        url: `/recipients/recipient-a/inbox?access_token=${await auth.recipientToken("recipient-a")}`,
      });
      assert.equal(queryTokenInbox.statusCode, 401);
      assert.deepEqual(queryTokenInbox.json(), { error: "invalid_token" });

      const missingRecipientClaim = await oidcService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
        headers: { authorization: `Bearer ${await auth.recipientToken("recipient-a", { recipient_id: undefined })}` },
      });
      assert.equal(missingRecipientClaim.statusCode, 403);
      assert.deepEqual(missingRecipientClaim.json(), { error: "recipient_id_claim_required" });
    } finally {
      await oidcService.close();
      restoreEnvironment();
      await auth.stop();
    }
  });
});

describe("T5 fail-closed auth", () => {
  function eventFixture(): Event {
    return {
      id: "evt_fail_closed",
      type: "order.shipped",
      recipients: ["recipient-a"],
      title: "Order shipped",
      body: "Your order is on the way.",
      payload: { orderId: "order-1" },
      occurredAt: "2026-07-19T10:00:00.000Z",
    };
  }

  it("rejects protected surfaces when no OIDC authenticator is configured", async () => {
    const failClosedService = buildService({
      inboxStore: {
        async migrate() {},
        async persist() {
          return true;
        },
        async list() {
          return { notifications: [], nextOffset: null };
        },
        async unreadCount() {
          return 0;
        },
        async markRead() {
          return undefined;
        },
        async markAllRead() {
          return 0;
        },
        async close() {},
      },
      pipeline: {
        async publish(event: Event) {
          return event.recipients.length;
        },
        async close() {},
      },
    });

    try {
      const eventResponse = await failClosedService.inject({
        method: "POST",
        url: "/events",
        payload: eventFixture(),
      });
      assert.equal(eventResponse.statusCode, 401);
      assert.deepEqual(eventResponse.json(), { error: "invalid_token" });

      const inboxResponse = await failClosedService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox",
      });
      assert.equal(inboxResponse.statusCode, 401);
      assert.deepEqual(inboxResponse.json(), { error: "invalid_token" });
    } finally {
      await failClosedService.close();
    }
  });
});

describe("T4 SSE transport", () => {
  it("opens an SSE Connection at the HTTP edge", async () => {
    const sseService = buildService({ authenticator: buildTestAuthenticator() });

    try {
      const address = await sseService.listen({ port: 0, host: "127.0.0.1" });
      const abortController = new AbortController();
      const response = await fetch(`${address}/connections/sse`, {
        headers: {
          accept: "text/event-stream",
          ...recipientAuthHeaders("recipient-sse"),
        },
        signal: abortController.signal,
      });

      abortController.abort();

      assert.equal(response.status, 200);
      assert.equal(response.headers.get("content-type")?.startsWith("text/event-stream"), true);
      assert.equal(response.headers.get("cache-control"), "no-cache");
    } finally {
      await sseService.close();
    }
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
    const inboxService = buildService({
      authenticator: buildTestAuthenticator(),
      inboxStore,
    });

    try {
      const page = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox?limit=2",
        headers: recipientAuthHeaders("recipient-a"),
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
        headers: recipientAuthHeaders("recipient-a"),
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
    const inboxService = buildService({
      authenticator: buildTestAuthenticator(),
      inboxStore,
    });

    try {
      const initialCount = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.deepEqual(initialCount.json(), { recipientId: "recipient-a", unread: 2 });

      const markRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/ntf_newer/read",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.equal(markRead.statusCode, 200);
      assert.equal(markRead.json().notification.read, true);

      const idempotentMarkRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/ntf_newer/read",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.equal(idempotentMarkRead.statusCode, 200);
      assert.equal(idempotentMarkRead.json().notification.read, true);

      const afterOneRead = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.deepEqual(afterOneRead.json(), { recipientId: "recipient-a", unread: 1 });

      const markAllRead = await inboxService.inject({
        method: "POST",
        url: "/recipients/recipient-a/inbox/read-all",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.deepEqual(markAllRead.json(), { recipientId: "recipient-a", markedRead: 1 });

      const afterAllRead = await inboxService.inject({
        method: "GET",
        url: "/recipients/recipient-a/inbox/unread-count",
        headers: recipientAuthHeaders("recipient-a"),
      });
      assert.deepEqual(afterAllRead.json(), { recipientId: "recipient-a", unread: 0 });
    } finally {
      await inboxService.close();
    }
  });
});
