import { z } from "zod";

const jsonValueSchema: z.ZodType<unknown> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValueSchema),
    z.record(jsonValueSchema),
  ]),
);

export const eventSchema = z.object({
  id: z.string().min(1),
  type: z.string().min(1),
  recipients: z.array(z.string().min(1)).min(1).refine(
    (recipients) => new Set(recipients).size === recipients.length,
    "recipients must be unique",
  ),
  title: z.string().min(1),
  body: z.string().min(1),
  payload: jsonValueSchema,
  occurredAt: z.string().datetime(),
});

export const kafkaNotificationMessageSchema = z.object({
  eventId: z.string().min(1),
  type: z.string().min(1),
  recipientId: z.string().min(1),
  title: z.string().min(1),
  body: z.string().min(1),
  payload: jsonValueSchema,
  occurredAt: z.string().datetime(),
});

export const parkedEventSchema = z.object({
  parkedAt: z.string().datetime(),
  sourceTopic: z.string().min(1),
  originalKey: z.string().nullable(),
  originalValue: z.string().nullable(),
  failureKind: z.enum(["malformed", "processing_failed"]),
  failureReason: z.string().min(1),
  attempts: z.number().int().min(0),
});

export const notificationSchema = z.object({
  id: z.string().min(1),
  eventId: z.string().min(1),
  recipientId: z.string().min(1),
  type: z.string().min(1),
  title: z.string().min(1),
  body: z.string().min(1),
  payload: jsonValueSchema,
  read: z.boolean(),
  createdAt: z.string().datetime(),
});

export const inboxPageSchema = z.object({
  notifications: z.array(notificationSchema),
  page: z.object({
    limit: z.number().int().positive(),
    offset: z.number().int().min(0),
    nextOffset: z.number().int().min(0).nullable(),
  }),
});

export const unreadCountSchema = z.object({
  recipientId: z.string().min(1),
  unread: z.number().int().min(0),
});

export const markReadResponseSchema = z.object({
  notification: notificationSchema,
});

export const markAllReadResponseSchema = z.object({
  recipientId: z.string().min(1),
  markedRead: z.number().int().min(0),
});

export type Event = z.infer<typeof eventSchema>;
export type KafkaNotificationMessage = z.infer<typeof kafkaNotificationMessageSchema>;
export type ParkedEvent = z.infer<typeof parkedEventSchema>;
export type Notification = z.infer<typeof notificationSchema>;
export type InboxPageResponse = z.infer<typeof inboxPageSchema>;
export type UnreadCountResponse = z.infer<typeof unreadCountSchema>;
export type MarkReadResponse = z.infer<typeof markReadResponseSchema>;
export type MarkAllReadResponse = z.infer<typeof markAllReadResponseSchema>;
