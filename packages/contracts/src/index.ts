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

export type Event = z.infer<typeof eventSchema>;
export type KafkaNotificationMessage = z.infer<typeof kafkaNotificationMessageSchema>;
export type Notification = z.infer<typeof notificationSchema>;
