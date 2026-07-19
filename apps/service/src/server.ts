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

type ServiceOptions = {
  pipeline?: Pipeline;
};

type KafkaPipelineOptions = {
  brokers: string[];
  groupId: string;
  postgresUrl: string;
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
  readonly #pool: pg.Pool;
  readonly #producer: Producer;
  readonly #topic: string;

  constructor(options: KafkaPipelineOptions, liveConnections: LiveConnections) {
    this.#kafka = new Kafka({
      brokers: options.brokers,
      clientId: "notification-service",
      logLevel: logLevel.ERROR,
    });

    this.#consumer = this.#kafka.consumer({ groupId: options.groupId });
    this.#liveConnections = liveConnections;
    this.#pool = new Pool({ connectionString: options.postgresUrl });
    this.#producer = this.#kafka.producer();
    this.#topic = options.topic;
  }

  async start() {
    await this.#migrate();
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
          const inserted = await this.#persist(notification);

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
      this.#pool.end(),
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

  async #migrate() {
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
  }

  async #persist(notification: Notification) {
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
}

function pipelineFromEnvironment(liveConnections: LiveConnections) {
  const brokers = process.env.KAFKA_BROKERS?.split(",").map((broker) => broker.trim()).filter(Boolean);
  const postgresUrl = process.env.POSTGRES_URL;

  if (!brokers?.length || !postgresUrl) {
    return undefined;
  }

  return new KafkaNotificationPipeline({
    brokers,
    groupId: process.env.KAFKA_GROUP_ID ?? `notification-service-${randomUUID()}`,
    postgresUrl,
    topic: process.env.KAFKA_TOPIC ?? "notifications",
  }, liveConnections);
}

export function buildService(options: ServiceOptions = {}) {
  const app = Fastify({ logger: true, pluginTimeout: 120_000 });
  const liveConnections = new LiveConnections();
  const pipeline = options.pipeline ?? pipelineFromEnvironment(liveConnections);

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

  app.addHook("onReady", async () => {
    if (pipeline instanceof KafkaNotificationPipeline) {
      await pipeline.start();
    }
  });

  app.addHook("onClose", async () => {
    await pipeline?.close();
  });

  return app;
}
