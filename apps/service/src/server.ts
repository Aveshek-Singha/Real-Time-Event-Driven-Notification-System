import Fastify from "fastify";
import websocket from "@fastify/websocket";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";
import {
  eventSchema,
  kafkaNotificationMessageSchema,
  type Event,
  type KafkaNotificationMessage,
  type Notification,
  type ParkedEvent,
} from "@notification-system/contracts";
import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";
import pg from "pg";

const { Pool } = pg;

type LiveConnection = {
  readyState: number;
  send(data: string): void;
};

type RecipientConnection = {
  send(notification: Notification): void;
};

type Pipeline = {
  publish(event: Event): Promise<number>;
  close(): Promise<void>;
};

type AuthResult<T = unknown> =
  | ({ ok: true } & T)
  | { ok: false; statusCode: 401 | 403; error: string };

type RecipientPrincipal = {
  recipientId: string;
};

type RequestWithRecipientPrincipal = {
  recipientPrincipal?: RecipientPrincipal;
};

type Authenticator = {
  authenticateProducer(request: { headers: { authorization?: string } }): Promise<AuthResult>;
  authenticateRecipient(
    request: { headers: { authorization?: string }; query: unknown },
    options?: { allowQueryAccessToken?: boolean },
  ): Promise<AuthResult<RecipientPrincipal>>;
};

type InboxPageOptions = {
  limit: number;
  offset: number;
  unreadOnly: boolean;
};

type InboxPage = {
  notifications: Notification[];
  nextOffset: number | null;
};

type InboxStore = {
  migrate(): Promise<void>;
  persist(notification: Notification): Promise<boolean>;
  list(recipientId: string, options: InboxPageOptions): Promise<InboxPage>;
  unreadCount(recipientId: string): Promise<number>;
  markRead(recipientId: string, notificationId: string): Promise<Notification | undefined>;
  markAllRead(recipientId: string): Promise<number>;
  close(): Promise<void>;
};

type ServiceOptions = {
  authenticator?: Authenticator;
  pipeline?: Pipeline;
  inboxStore?: InboxStore;
};

type JwksAuthenticatorOptions = {
  audience: string;
  issuer: string;
  jwksUri: string;
  producerClientId: string;
  recipientClientId: string;
  recipientIdClaim: string;
};

type KafkaPipelineOptions = {
  brokers: string[];
  dlqTopic: string;
  groupId: string;
  retryAttempts: number;
  retryBackoffMs: number;
  topic: string;
};

type MetricFamily = {
  help: string;
  name: string;
  type: "counter" | "gauge";
};

const metricFamilies: MetricFamily[] = [{
  name: "notification_ingest_events_total",
  help: "Events accepted by the ingest API.",
  type: "counter",
}, {
  name: "notification_delivery_notifications_total",
  help: "Notifications delivered to live Connections.",
  type: "counter",
}, {
  name: "notification_consumer_lag_messages",
  help: "Kafka consumer lag in messages.",
  type: "gauge",
}, {
  name: "notification_dlq_depth_messages",
  help: "Parked Events currently visible on the dead-letter topic.",
  type: "gauge",
}, {
  name: "notification_live_connections",
  help: "Live recipient Connections currently registered with the service.",
  type: "gauge",
}];

class PrometheusMetrics {
  readonly #values = new Map(metricFamilies.map((family) => [family.name, 0]));

  increment(name: string, amount = 1) {
    this.#values.set(name, (this.#values.get(name) ?? 0) + amount);
  }

  set(name: string, value: number) {
    this.#values.set(name, value);
  }

  render() {
    return metricFamilies
      .map((family) => [
        `# HELP ${family.name} ${family.help}`,
        `# TYPE ${family.name} ${family.type}`,
        `${family.name} ${this.#values.get(family.name) ?? 0}`,
      ].join("\n"))
      .join("\n") + "\n";
  }
}

const openConnectionState = 1;

function notificationId(eventId: string, recipientId: string) {
  const readable = `${eventId}_${recipientId}`.replaceAll(/[^a-zA-Z0-9_-]/g, "_");
  return `ntf_${readable}`;
}

function messageFromEvent(event: Event, recipientId: string): KafkaNotificationMessage {
  return {
    eventId: event.id,
    type: event.type,
    recipientId,
    title: event.title,
    body: event.body,
    payload: event.payload,
    occurredAt: event.occurredAt,
  };
}

function notificationFromMessage(message: KafkaNotificationMessage): Notification {
  return {
    id: notificationId(message.eventId, message.recipientId),
    eventId: message.eventId,
    recipientId: message.recipientId,
    type: message.type,
    title: message.title,
    body: message.body,
    payload: message.payload,
    read: false,
    createdAt: message.occurredAt,
  };
}

class LiveConnections {
  readonly #connectionsByRecipient = new Map<string, Set<RecipientConnection>>();
  readonly #metrics: PrometheusMetrics;
  #connectionCount = 0;

  constructor(metrics: PrometheusMetrics) {
    this.#metrics = metrics;
  }

  add(recipientId: string, connection: RecipientConnection) {
    const connections = this.#connectionsByRecipient.get(recipientId) ?? new Set<RecipientConnection>();
    const existingConnection = connections.has(connection);

    connections.add(connection);
    this.#connectionsByRecipient.set(recipientId, connections);

    if (!existingConnection) {
      this.#connectionCount += 1;
      this.#metrics.set("notification_live_connections", this.#connectionCount);
    }
  }

  remove(recipientId: string, connection: RecipientConnection) {
    const connections = this.#connectionsByRecipient.get(recipientId);
    const removed = connections?.delete(connection);

    if (connections?.size === 0) {
      this.#connectionsByRecipient.delete(recipientId);
    }

    if (removed) {
      this.#connectionCount -= 1;
      this.#metrics.set("notification_live_connections", this.#connectionCount);
    }
  }

  push(notification: Notification) {
    const connections = this.#connectionsByRecipient.get(notification.recipientId) ?? [];
    let deliveries = 0;

    for (const connection of connections) {
      connection.send(notification);
      deliveries += 1;
    }

    return deliveries;
  }
}

class WebSocketRecipientConnection implements RecipientConnection {
  readonly #connection: LiveConnection;

  constructor(connection: LiveConnection) {
    this.#connection = connection;
  }

  send(notification: Notification) {
    if (this.#connection.readyState === openConnectionState) {
      this.#connection.send(JSON.stringify(notification));
    }
  }
}

class SseRecipientConnection implements RecipientConnection {
  readonly #stream: {
    destroyed: boolean;
    writableEnded: boolean;
    write(data: string): void;
  };

  constructor(stream: { destroyed: boolean; writableEnded: boolean; write(data: string): void }) {
    this.#stream = stream;
  }

  send(notification: Notification) {
    if (!this.#stream.destroyed && !this.#stream.writableEnded) {
      this.#stream.write(`event: notification\ndata: ${JSON.stringify(notification)}\n\n`);
    }
  }
}

class JwksAuthenticator implements Authenticator {
  readonly #audience: string;
  readonly #issuer: string;
  readonly #jwks: ReturnType<typeof createRemoteJWKSet>;
  readonly #producerClientId: string;
  readonly #recipientClientId: string;
  readonly #recipientIdClaim: string;

  constructor(options: JwksAuthenticatorOptions) {
    this.#audience = options.audience;
    this.#issuer = options.issuer;
    this.#jwks = createRemoteJWKSet(new URL(options.jwksUri));
    this.#producerClientId = options.producerClientId;
    this.#recipientClientId = options.recipientClientId;
    this.#recipientIdClaim = options.recipientIdClaim;
  }

  async authenticateProducer(request: { headers: { authorization?: string } }) {
    const verified = await this.#verify(extractAuthorizationBearerToken(request));

    if (!verified.ok) {
      return verified;
    }

    if (!tokenMatchesClient(verified.payload, this.#producerClientId)) {
      return { ok: false, statusCode: 403, error: "producer_token_required" } as const;
    }

    return { ok: true } as const;
  }

  async authenticateRecipient(
    request: { headers: { authorization?: string }; query: unknown },
    options: { allowQueryAccessToken?: boolean } = {},
  ) {
    const verified = await this.#verify(
      extractAuthorizationBearerToken(request) ??
      (options.allowQueryAccessToken ? extractQueryAccessToken(request.query) : undefined),
    );

    if (!verified.ok) {
      return verified;
    }

    if (!tokenMatchesClient(verified.payload, this.#recipientClientId)) {
      return { ok: false, statusCode: 403, error: "recipient_token_required" } as const;
    }

    const recipientId = verified.payload[this.#recipientIdClaim];

    if (typeof recipientId !== "string" || recipientId.length === 0) {
      return { ok: false, statusCode: 403, error: "recipient_id_claim_required" } as const;
    }

    return { ok: true, recipientId } as const;
  }

  async #verify(token: string | undefined): Promise<AuthResult<{ payload: JWTPayload }>> {
    if (!token) {
      return { ok: false, statusCode: 401, error: "invalid_token" };
    }

    try {
      const { payload } = await jwtVerify(token, this.#jwks, {
        audience: this.#audience,
        issuer: this.#issuer,
      });

      return { ok: true, payload };
    } catch {
      return { ok: false, statusCode: 401, error: "invalid_token" };
    }
  }
}

class RejectingAuthenticator implements Authenticator {
  async authenticateProducer() {
    return { ok: false, statusCode: 401, error: "invalid_token" } as const;
  }

  async authenticateRecipient() {
    return { ok: false, statusCode: 401, error: "invalid_token" } as const;
  }
}

class KafkaNotificationPipeline implements Pipeline {
  readonly #consumer: Consumer;
  #dlqDepth = 0;
  readonly #dlqTopic: string;
  readonly #kafka: Kafka;
  readonly #liveConnections: LiveConnections;
  readonly #metrics: PrometheusMetrics;
  readonly #producer: Producer;
  readonly #retryAttempts: number;
  readonly #retryBackoffMs: number;
  readonly #store: InboxStore;
  readonly #topic: string;

  constructor(
    options: KafkaPipelineOptions,
    liveConnections: LiveConnections,
    store: InboxStore,
    metrics: PrometheusMetrics,
  ) {
    this.#kafka = new Kafka({
      brokers: options.brokers,
      clientId: "notification-service",
      logLevel: logLevel.ERROR,
    });

    this.#consumer = this.#kafka.consumer({ groupId: options.groupId });
    this.#dlqTopic = options.dlqTopic;
    this.#liveConnections = liveConnections;
    this.#metrics = metrics;
    this.#producer = this.#kafka.producer();
    this.#retryAttempts = options.retryAttempts;
    this.#retryBackoffMs = options.retryBackoffMs;
    this.#store = store;
    this.#topic = options.topic;
  }

  async start() {
    await this.#store.migrate();
    await this.#ensureTopics();
    await this.#producer.connect();
    await this.#consumer.connect();
    await this.#consumer.subscribe({ topic: this.#topic, fromBeginning: true });
    const groupJoined = new Promise<void>((resolve) => {
      const removeListener = this.#consumer.on(this.#consumer.events.GROUP_JOIN, () => {
        removeListener();
        resolve();
      });
    });
    this.#consumer.on(this.#consumer.events.CRASH, (event) => {
      console.error("notification consumer crashed", event.payload.error);
    });
    this.#consumer.on(this.#consumer.events.START_BATCH_PROCESS, (event) => {
      const lag = Number(event.payload.offsetLag);

      if (Number.isFinite(lag)) {
        this.#metrics.set("notification_consumer_lag_messages", Math.max(0, lag));
      }
    });

    await this.#consumer.run({
      eachMessage: async ({ message }) => {
        await this.#handleMessage(message);
      },
    });
    await groupJoined;
  }

  async publish(event: Event) {
    const messages = event.recipients.map((recipientId) => {
      const notificationMessage = messageFromEvent(event, recipientId);

      return {
        key: recipientId,
        value: JSON.stringify(notificationMessage),
      };
    });

    await this.#producer.send({
      topic: this.#topic,
      messages,
    });

    return messages.length;
  }

  async close() {
    await Promise.allSettled([
      this.#consumer.disconnect(),
      this.#producer.disconnect(),
    ]);
  }

  async #ensureTopics() {
    const admin = this.#kafka.admin();

    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [this.#topic, this.#dlqTopic].map((topic) => ({
          topic,
          numPartitions: 3,
          replicationFactor: 1,
        })),
      });
    } finally {
      await admin.disconnect();
    }
  }

  async #handleMessage(message: { key?: Buffer | null; value?: Buffer | null }) {
    const originalKey = message.key?.toString() ?? null;
    const originalValue = message.value?.toString() ?? null;
    let parsedMessage: KafkaNotificationMessage;

    try {
      if (!message.value) {
        throw new Error("Kafka notification message value is required");
      }

      parsedMessage = kafkaNotificationMessageSchema.parse(JSON.parse(message.value.toString()));
    } catch (error) {
      await this.#parkMessage({
        attempts: 0,
        failureKind: "malformed",
        failureReason: failureReason(error),
        originalKey,
        originalValue,
      });
      return;
    }

    const maxAttempts = this.#retryAttempts + 1;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const notification = notificationFromMessage(parsedMessage);
        const inserted = await this.#store.persist(notification);

        if (inserted) {
          const deliveries = this.#liveConnections.push(notification);

          if (deliveries > 0) {
            this.#metrics.increment("notification_delivery_notifications_total", deliveries);
          }
        }

        this.#metrics.set("notification_consumer_lag_messages", 0);
        return;
      } catch (error) {
        if (attempt === maxAttempts) {
          await this.#parkMessage({
            attempts: attempt,
            failureKind: "processing_failed",
            failureReason: failureReason(error),
            originalKey,
            originalValue,
          });
          return;
        }

        await delay(this.#retryBackoffMs);
      }
    }
  }

  async #parkMessage(options: {
    attempts: number;
    failureKind: ParkedEvent["failureKind"];
    failureReason: string;
    originalKey: string | null;
    originalValue: string | null;
  }) {
    const parkedEvent: ParkedEvent = {
      attempts: options.attempts,
      failureKind: options.failureKind,
      failureReason: options.failureReason,
      originalKey: options.originalKey,
      originalValue: options.originalValue,
      parkedAt: new Date().toISOString(),
      sourceTopic: this.#topic,
    };

    await this.#producer.send({
      topic: this.#dlqTopic,
      messages: [{
        key: options.originalKey ?? undefined,
        value: JSON.stringify(parkedEvent),
      }],
    });

    this.#dlqDepth += 1;
    this.#metrics.set("notification_consumer_lag_messages", 0);
    this.#metrics.set("notification_dlq_depth_messages", this.#dlqDepth);
  }
}

class PgInboxStore implements InboxStore {
  readonly #pool: pg.Pool;

  constructor(postgresUrl: string) {
    this.#pool = new Pool({ connectionString: postgresUrl });
  }

  async migrate() {
    await this.#pool.query(`
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
    await this.#pool.query(`
      CREATE INDEX IF NOT EXISTS notifications_recipient_created_idx
      ON notifications (recipient_id, created_at DESC, id DESC)
    `);
    await this.#pool.query(`
      CREATE INDEX IF NOT EXISTS notifications_recipient_unread_idx
      ON notifications (recipient_id)
      WHERE read = false
    `);
  }

  async persist(notification: Notification) {
    const result = await this.#pool.query(
      `
        INSERT INTO notifications (
          id,
          event_id,
          recipient_id,
          type,
          title,
          body,
          payload,
          read,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (event_id, recipient_id) DO NOTHING
      `,
      [
        notification.id,
        notification.eventId,
        notification.recipientId,
        notification.type,
        notification.title,
        notification.body,
        JSON.stringify(notification.payload),
        notification.read,
        notification.createdAt,
      ],
    );

    return result.rowCount === 1;
  }

  async list(recipientId: string, options: InboxPageOptions) {
    const rowsToFetch = options.limit + 1;
    const unreadPredicate = options.unreadOnly ? "AND read = false" : "";
    const result = await this.#pool.query(
      `
        SELECT id, event_id, recipient_id, type, title, body, payload, read, created_at
        FROM notifications
        WHERE recipient_id = $1
        ${unreadPredicate}
        ORDER BY created_at DESC, id DESC
        LIMIT $2 OFFSET $3
      `,
      [recipientId, rowsToFetch, options.offset],
    );
    const notifications = result.rows.slice(0, options.limit).map(notificationFromRow);

    return {
      notifications,
      nextOffset: result.rows.length > options.limit ? options.offset + options.limit : null,
    };
  }

  async unreadCount(recipientId: string) {
    const result = await this.#pool.query(
      "SELECT count(*)::int AS count FROM notifications WHERE recipient_id = $1 AND read = false",
      [recipientId],
    );

    return result.rows[0].count;
  }

  async markRead(recipientId: string, notificationId: string) {
    const result = await this.#pool.query(
      `
        UPDATE notifications
        SET read = true
        WHERE recipient_id = $1 AND id = $2
        RETURNING id, event_id, recipient_id, type, title, body, payload, read, created_at
      `,
      [recipientId, notificationId],
    );

    return result.rows[0] ? notificationFromRow(result.rows[0]) : undefined;
  }

  async markAllRead(recipientId: string) {
    const result = await this.#pool.query(
      "UPDATE notifications SET read = true WHERE recipient_id = $1 AND read = false",
      [recipientId],
    );

    return result.rowCount ?? 0;
  }

  async close() {
    await this.#pool.end();
  }
}

function notificationFromRow(row: {
  id: string;
  event_id: string;
  recipient_id: string;
  type: string;
  title: string;
  body: string;
  payload: unknown;
  read: boolean;
  created_at: Date | string;
}): Notification {
  const createdAt = row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at;

  return {
    id: row.id,
    eventId: row.event_id,
    recipientId: row.recipient_id,
    type: row.type,
    title: row.title,
    body: row.body,
    payload: row.payload,
    read: row.read,
    createdAt,
  };
}

function parseIntegerQuery(value: unknown, fallback: number, options: { min: number; max: number }) {
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < options.min || parsed > options.max) {
    return undefined;
  }

  return parsed;
}

function parseEnvironmentInteger(name: string, fallback: number, options: { min: number; max: number }) {
  const value = process.env[name];

  if (value === undefined) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < options.min || parsed > options.max) {
    return fallback;
  }

  return parsed;
}

function failureReason(error: unknown) {
  if (error instanceof Error) {
    return error.message || error.name;
  }

  return String(error);
}

function extractAuthorizationBearerToken(request: { headers: { authorization?: string } }) {
  const authorization = request.headers.authorization;

  if (!authorization) {
    return undefined;
  }

  const match = /^Bearer\s+(.+)$/i.exec(authorization);

  return match?.[1];
}

function extractQueryAccessToken(query: unknown) {
  if (!query || typeof query !== "object" || Array.isArray(query)) {
    return undefined;
  }

  const accessToken = (query as { access_token?: unknown }).access_token;

  return typeof accessToken === "string" && accessToken.length > 0 ? accessToken : undefined;
}

function tokenMatchesClient(payload: JWTPayload, clientId: string) {
  return payload.azp === clientId ||
    payload.client_id === clientId ||
    audienceIncludes(payload.aud, clientId);
}

function audienceIncludes(audience: JWTPayload["aud"], expected: string) {
  return audience === expected || (Array.isArray(audience) && audience.includes(expected));
}

function environmentAuthenticator() {
  const issuer = process.env.OIDC_ISSUER?.trim();

  if (!issuer) {
    return undefined;
  }

  const normalizedIssuer = issuer.replace(/\/$/, "");

  return new JwksAuthenticator({
    audience: process.env.OIDC_AUDIENCE?.trim() || "notification-service",
    issuer: normalizedIssuer,
    jwksUri: process.env.OIDC_JWKS_URI?.trim() || `${normalizedIssuer}/protocol/openid-connect/certs`,
    producerClientId: process.env.OIDC_PRODUCER_CLIENT_ID?.trim() || "notification-producer",
    recipientClientId: process.env.OIDC_RECIPIENT_CLIENT_ID?.trim() || "notification-web",
    recipientIdClaim: process.env.OIDC_RECIPIENT_ID_CLAIM?.trim() || "recipient_id",
  });
}

function environmentDependencies(liveConnections: LiveConnections, metrics: PrometheusMetrics) {
  const brokers = process.env.KAFKA_BROKERS?.split(",").map((broker) => broker.trim()).filter(Boolean);
  const postgresUrl = process.env.POSTGRES_URL;

  if (!brokers?.length || !postgresUrl) {
    return undefined;
  }

  const inboxStore = new PgInboxStore(postgresUrl);
  const topic = process.env.KAFKA_TOPIC ?? "notifications";
  const pipeline = new KafkaNotificationPipeline({
    brokers,
    dlqTopic: process.env.KAFKA_DLQ_TOPIC ?? `${topic}.dlq`,
    groupId: process.env.KAFKA_GROUP_ID ?? `notification-service-${randomUUID()}`,
    retryAttempts: parseEnvironmentInteger("KAFKA_RETRY_ATTEMPTS", 3, { min: 0, max: 20 }),
    retryBackoffMs: parseEnvironmentInteger("KAFKA_RETRY_BACKOFF_MS", 250, { min: 0, max: 60_000 }),
    topic,
  }, liveConnections, inboxStore, metrics);

  return { inboxStore, pipeline };
}

export function buildService(options: ServiceOptions = {}) {
  const app = Fastify({ logger: true, pluginTimeout: 120_000 });
  const metrics = new PrometheusMetrics();
  const liveConnections = new LiveConnections(metrics);
  const environment = options.pipeline || options.inboxStore ? undefined : environmentDependencies(liveConnections, metrics);
  const authenticator = options.authenticator ?? environmentAuthenticator() ?? new RejectingAuthenticator();
  const pipeline = options.pipeline ?? environment?.pipeline;
  const inboxStore = options.inboxStore ?? environment?.inboxStore;

  async function requireProducer(request: { headers: { authorization?: string } }, reply: { code(statusCode: number): { send(payload: unknown): unknown } }) {
    const result = await authenticator.authenticateProducer(request);

    if (!result.ok) {
      reply.code(result.statusCode).send({ error: result.error });
      return false;
    }

    return true;
  }

  async function requireRecipient(
    request: { headers: { authorization?: string }; query: unknown },
    reply: { code(statusCode: number): { send(payload: unknown): unknown } },
    recipientId: string,
  ) {
    const result = await authenticator.authenticateRecipient(request);

    if (!result.ok) {
      reply.code(result.statusCode).send({ error: result.error });
      return false;
    }

    if (result.recipientId !== recipientId) {
      reply.code(403).send({ error: "recipient_forbidden" });
      return false;
    }

    return true;
  }

  async function authenticateRecipient(
    request: { headers: { authorization?: string }; query: unknown },
    reply: { code(statusCode: number): { send(payload: unknown): unknown } },
    options: { allowQueryAccessToken?: boolean } = {},
  ) {
    const result = await authenticator.authenticateRecipient(request, options);

    if (!result.ok) {
      reply.code(result.statusCode).send({ error: result.error });
      return undefined;
    }

    return { recipientId: result.recipientId };
  }

  app.register(websocket);
  app.after(() => {
    app.get("/connections/ws", {
      websocket: true,
      preHandler: async (request, reply) => {
        const principal = await authenticateRecipient(request, reply, { allowQueryAccessToken: true });

        if (!principal) {
          return reply;
        }

        (request as typeof request & RequestWithRecipientPrincipal).recipientPrincipal = principal;
      },
    }, (connection, request) => {
      const principal = (request as typeof request & RequestWithRecipientPrincipal).recipientPrincipal;
      const recipientId = principal?.recipientId;

      if (!recipientId) {
        connection.close(1008, "recipient token is required");
        return;
      }

      const recipientConnection = new WebSocketRecipientConnection(connection);
      liveConnections.add(recipientId, recipientConnection);
      connection.on("message", () => {});
      connection.on("close", () => liveConnections.remove(recipientId, recipientConnection));
    });
  });

  app.get("/connections/sse", (request, reply) => {
    return (async () => {
      const principal = await authenticateRecipient(request, reply, { allowQueryAccessToken: true });

      if (!principal) {
        return;
      }

      const recipientId = principal.recipientId;

      reply.hijack();
      reply.raw.writeHead(200, {
        "cache-control": "no-cache",
        "connection": "keep-alive",
        "content-type": "text/event-stream; charset=utf-8",
        "x-accel-buffering": "no",
      });
      reply.raw.write(": connected\n\n");

      const recipientConnection = new SseRecipientConnection(reply.raw);
      liveConnections.add(recipientId, recipientConnection);
      request.raw.on("close", () => {
        liveConnections.remove(recipientId, recipientConnection);
      });
    })();
  });

  app.get("/", async () => ({
    service: "notification-service",
    status: "ok",
  }));

  app.get("/health", async () => ({
    status: "ok",
  }));

  app.get("/metrics", async (_request, reply) => {
    return reply
      .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
      .send(metrics.render());
  });

  app.post("/events", async (request, reply) => {
    if (!await requireProducer(request, reply)) {
      return;
    }

    const parsedEvent = eventSchema.safeParse(request.body);

    if (!parsedEvent.success) {
      return reply.code(400).send({
        error: "invalid_event",
        details: parsedEvent.error.issues,
      });
    }

    if (!pipeline) {
      return reply.code(503).send({
        error: "event_pipeline_unavailable",
      });
    }

    const notificationCount = await pipeline.publish(parsedEvent.data);
    metrics.increment("notification_ingest_events_total");

    return reply.code(202).send({
      accepted: true,
      eventId: parsedEvent.data.id,
      notifications: notificationCount,
    });
  });

  app.get("/recipients/:recipientId/inbox", async (request, reply) => {
    if (!inboxStore) {
      return reply.code(503).send({
        error: "inbox_unavailable",
      });
    }

    const query = request.query as { limit?: string; offset?: string; unreadOnly?: string };
    const limit = parseIntegerQuery(query.limit, 20, { min: 1, max: 100 });
    const offset = parseIntegerQuery(query.offset, 0, { min: 0, max: 1_000_000 });
    const unreadOnly = query.unreadOnly === "true";

    if (limit === undefined || offset === undefined) {
      return reply.code(400).send({
        error: "invalid_inbox_query",
      });
    }

    const params = request.params as { recipientId: string };

    if (!await requireRecipient(request, reply, params.recipientId)) {
      return;
    }

    const page = await inboxStore.list(params.recipientId, { limit, offset, unreadOnly });

    return {
      notifications: page.notifications,
      page: {
        limit,
        offset,
        nextOffset: page.nextOffset,
      },
    };
  });

  app.get("/recipients/:recipientId/inbox/unread-count", async (request, reply) => {
    if (!inboxStore) {
      return reply.code(503).send({
        error: "inbox_unavailable",
      });
    }

    const params = request.params as { recipientId: string };

    if (!await requireRecipient(request, reply, params.recipientId)) {
      return;
    }

    return {
      recipientId: params.recipientId,
      unread: await inboxStore.unreadCount(params.recipientId),
    };
  });

  app.post("/recipients/:recipientId/inbox/:notificationId/read", async (request, reply) => {
    if (!inboxStore) {
      return reply.code(503).send({
        error: "inbox_unavailable",
      });
    }

    const params = request.params as { recipientId: string; notificationId: string };

    if (!await requireRecipient(request, reply, params.recipientId)) {
      return;
    }

    const notification = await inboxStore.markRead(params.recipientId, params.notificationId);

    if (!notification) {
      return reply.code(404).send({
        error: "notification_not_found",
      });
    }

    return { notification };
  });

  app.post("/recipients/:recipientId/inbox/read-all", async (request, reply) => {
    if (!inboxStore) {
      return reply.code(503).send({
        error: "inbox_unavailable",
      });
    }

    const params = request.params as { recipientId: string };

    if (!await requireRecipient(request, reply, params.recipientId)) {
      return;
    }

    return {
      recipientId: params.recipientId,
      markedRead: await inboxStore.markAllRead(params.recipientId),
    };
  });

  app.addHook("onReady", async () => {
    if (inboxStore && !(pipeline instanceof KafkaNotificationPipeline)) {
      await inboxStore.migrate();
    }

    if (pipeline instanceof KafkaNotificationPipeline) {
      await pipeline.start();
    }
  });

  app.addHook("onClose", async () => {
    await Promise.allSettled([
      pipeline?.close(),
      inboxStore?.close(),
    ]);
  });

  return app;
}
