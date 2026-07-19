import Fastify from "fastify";
import websocket from "@fastify/websocket";
import { randomUUID } from "node:crypto";
import {
  eventSchema,
  kafkaNotificationMessageSchema,
  type Event,
  type KafkaNotificationMessage,
  type Notification,
} from "@notification-system/contracts";
import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";
import pg from "pg";

const { Pool } = pg;

type LiveConnection = {
  readyState: number;
  send(data: string): void;
};

type Pipeline = {
  publish(event: Event): Promise<number>;
  close(): Promise<void>;
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
  pipeline?: Pipeline;
  inboxStore?: InboxStore;
};

type KafkaPipelineOptions = {
  brokers: string[];
  groupId: string;
  topic: string;
};

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
  readonly #connectionsByRecipient = new Map<string, Set<LiveConnection>>();

  add(recipientId: string, connection: LiveConnection) {
    const connections = this.#connectionsByRecipient.get(recipientId) ?? new Set<LiveConnection>();
    connections.add(connection);
    this.#connectionsByRecipient.set(recipientId, connections);
  }

  remove(recipientId: string, connection: LiveConnection) {
    const connections = this.#connectionsByRecipient.get(recipientId);
    connections?.delete(connection);

    if (connections?.size === 0) {
      this.#connectionsByRecipient.delete(recipientId);
    }
  }

  push(notification: Notification) {
    const connections = this.#connectionsByRecipient.get(notification.recipientId) ?? [];
    const serialized = JSON.stringify(notification);

    for (const connection of connections) {
      if (connection.readyState === openConnectionState) {
        connection.send(serialized);
      }
    }
  }
}

class KafkaNotificationPipeline implements Pipeline {
  readonly #consumer: Consumer;
  readonly #kafka: Kafka;
  readonly #liveConnections: LiveConnections;
  readonly #producer: Producer;
  readonly #store: InboxStore;
  readonly #topic: string;

  constructor(options: KafkaPipelineOptions, liveConnections: LiveConnections, store: InboxStore) {
    this.#kafka = new Kafka({
      brokers: options.brokers,
      clientId: "notification-service",
      logLevel: logLevel.ERROR,
    });

    this.#consumer = this.#kafka.consumer({ groupId: options.groupId });
    this.#liveConnections = liveConnections;
    this.#producer = this.#kafka.producer();
    this.#store = store;
    this.#topic = options.topic;
  }

  async start() {
    await this.#store.migrate();
    await this.#ensureTopic();
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

    await this.#consumer.run({
      eachMessage: async ({ message }) => {
        try {
          if (!message.value) {
            throw new Error("Kafka notification message value is required");
          }

          const parsedMessage = kafkaNotificationMessageSchema.parse(JSON.parse(message.value.toString()));
          const notification = notificationFromMessage(parsedMessage);
          const inserted = await this.#store.persist(notification);

          if (inserted) {
            this.#liveConnections.push(notification);
          }
        } catch (error) {
          console.error("notification message handling failed", error);
          throw error;
        }
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

  async #ensureTopic() {
    const admin = this.#kafka.admin();

    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{
          topic: this.#topic,
          numPartitions: 3,
          replicationFactor: 1,
        }],
      });
    } finally {
      await admin.disconnect();
    }
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

function environmentDependencies(liveConnections: LiveConnections) {
  const brokers = process.env.KAFKA_BROKERS?.split(",").map((broker) => broker.trim()).filter(Boolean);
  const postgresUrl = process.env.POSTGRES_URL;

  if (!brokers?.length || !postgresUrl) {
    return undefined;
  }

  const inboxStore = new PgInboxStore(postgresUrl);
  const pipeline = new KafkaNotificationPipeline({
    brokers,
    groupId: process.env.KAFKA_GROUP_ID ?? `notification-service-${randomUUID()}`,
    topic: process.env.KAFKA_TOPIC ?? "notifications",
  }, liveConnections, inboxStore);

  return { inboxStore, pipeline };
}

export function buildService(options: ServiceOptions = {}) {
  const app = Fastify({ logger: true, pluginTimeout: 120_000 });
  const liveConnections = new LiveConnections();
  const environment = options.pipeline || options.inboxStore ? undefined : environmentDependencies(liveConnections);
  const pipeline = options.pipeline ?? environment?.pipeline;
  const inboxStore = options.inboxStore ?? environment?.inboxStore;

  app.register(websocket);
  app.after(() => {
    app.get("/connections/ws", { websocket: true }, (connection, request) => {
      const recipientId = (request.query as { recipientId?: string }).recipientId;

      if (!recipientId) {
        connection.close(1008, "recipientId is required");
        return;
      }

      liveConnections.add(recipientId, connection);
      connection.on("message", () => {});
      connection.on("close", () => liveConnections.remove(recipientId, connection));
    });
  });

  app.get("/", async () => ({
    service: "notification-service",
    status: "ok",
  }));

  app.get("/health", async () => ({
    status: "ok",
  }));

  app.post("/events", async (request, reply) => {
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
