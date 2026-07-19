# CONTEXT.md

Glossary for the Real-Time Event-Driven Notification System — a standalone notification service. Producer systems publish events to it; it delivers notifications to users.

## Terms

### Producer

An external system that publishes Events to the service via the HTTP ingest API. Producers decide who an Event is for; the service does not hold routing rules.

### Event

A fact published by a Producer, already addressed to one or more Recipients. Has a type (e.g. `order.shipped`), a recipient list, and a payload. An Event is input — it is never shown to users directly.

### Notification

What a Recipient receives. Created by the service: one Notification per Recipient per Event. Notifications are persisted (durable), forming the Recipient's Inbox.

### Recipient

A user identified by an ID in an Event's recipient list. The service trusts Producer-supplied recipient IDs; it does not own a user database.

### Inbox

The persistent, per-Recipient collection of Notifications. Survives disconnection: a Recipient with no live connection receives their backlog on reconnect.

### Delivery

The act of pushing a Notification to a Recipient's live in-app connections. A Notification is pushed to **every** live Connection the Recipient has. Delivery is at-least-once: a Notification may arrive more than once (clients dedupe by Notification ID) but is never silently lost — the Inbox is the durable record regardless of delivery.

### Connection

A single live in-app link (one browser tab, one device session) belonging to a Recipient, over WebSocket or SSE. A Recipient may have many Connections at once. Connections are push-only: the server pushes Notifications down them; all client writes (marking read, fetching backlog) go through the REST API.

### Read state

Whether a Notification is `unread` or `read`. Belongs to the Notification, not the Connection — marking read on one device marks it read everywhere. A Recipient can mark one Notification read or mark all read.

### Parked Event

A per-recipient message the pipeline could not process after retries (malformed, persistent downstream failure). Parked Events are set aside for inspection and possible replay rather than blocking other Notifications or being silently dropped.

## Ordering

Notifications for the same Recipient are delivered in the order their Events were published. No ordering promise is made across different Recipients.
