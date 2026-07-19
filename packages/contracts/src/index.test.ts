import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  eventSchema,
  kafkaNotificationMessageSchema,
  notificationSchema,
} from "./index.js";

describe("T2 notification contracts", () => {
  const event = {
    id: "evt_order_123",
    type: "order.shipped",
    recipients: ["recipient-a", "recipient-b"],
    title: "Order shipped",
    body: "Your order is on the way.",
    payload: { orderId: "order-123" },
    occurredAt: "2026-07-19T10:00:00.000Z",
  };

  it("accepts a producer Event addressed to recipients", () => {
    assert.deepEqual(eventSchema.parse(event), event);
  });

  it("rejects an Event with no recipients", () => {
    assert.throws(
      () => eventSchema.parse({ ...event, recipients: [] }),
      /recipients/i,
    );
  });

  it("rejects an Event with duplicate recipients", () => {
    assert.throws(
      () => eventSchema.parse({ ...event, recipients: ["recipient-a", "recipient-a"] }),
      /recipient/i,
    );
  });

  it("pins the per-recipient Kafka message shape", () => {
    const message = {
      eventId: event.id,
      type: event.type,
      recipientId: "recipient-a",
      title: event.title,
      body: event.body,
      payload: event.payload,
      occurredAt: event.occurredAt,
    };

    assert.deepEqual(kafkaNotificationMessageSchema.parse(message), message);
  });

  it("pins the delivered Notification shape", () => {
    const notification = {
      id: "ntf_evt_order_123_recipient-a",
      eventId: event.id,
      recipientId: "recipient-a",
      type: event.type,
      title: event.title,
      body: event.body,
      payload: event.payload,
      read: false,
      createdAt: event.occurredAt,
    };

    assert.deepEqual(notificationSchema.parse(notification), notification);
  });
});
