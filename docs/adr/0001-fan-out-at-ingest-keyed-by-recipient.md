# ADR-0001: Fan out Events to per-recipient Kafka messages at ingest, keyed by recipient ID

## Status

Accepted (2026-07-18)

## Context

The service promises per-recipient ordering: two Events addressed to the same Recipient are delivered in publish order. An Event may address many Recipients, and Kafka only orders messages within a partition, so the choice of when to fan out (one Event → N Notifications) and what to key messages by decides whether that promise actually holds.

Two candidate designs:

1. **Fan-out at ingest** — the ingest API produces one Kafka message per (Event, Recipient), keyed by recipient ID. Same Recipient always lands on the same partition, so Kafka's partition ordering *is* the per-recipient ordering.
2. **Fan-out at consumer** — one Kafka message per Event, keyed by event ID; the consumer explodes it into Notifications. Fewer messages, but successive Events for the same Recipient land on arbitrary partitions and may be consumed out of order — the ordering promise would silently not hold.

## Decision

Fan out at ingest. The ingest API produces one message per (Event, Recipient) to the notifications topic, keyed by recipient ID. The consumer treats one message as one Notification. Idempotency is enforced at persist time with a unique constraint on (event ID, recipient ID).

## Consequences

- Per-recipient ordering falls out of Kafka partitioning; no ordering logic in the service.
- A large recipient list multiplies ingest-side produce volume (1000 recipients = 1000 messages). Acceptable at this system's scale; a broadcast/segment feature would need a different design and a new ADR.
- The 202 ingest ack means "all per-recipient messages durably acknowledged by Kafka" — partial produce failure must be retried or rejected before acking, and Producer retries are safe because of the (event ID, recipient ID) dedupe.
- The Kafka message schema is per-recipient from day one; changing to per-event messages later is a breaking topic migration.
