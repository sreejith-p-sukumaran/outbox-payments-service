# outbox-payments-service

A payments service that publishes domain events to Kafka using the
**Transactional Outbox** pattern — solving the dual-write problem between a
database and a message broker.

> Portfolio / interview-prep piece, and a continuation of an idempotency
> service. The full design rationale lives in [`DESIGN.md`](./DESIGN.md); this
> README is the practical "what it is and how to run it".

## The idea in one line

When a payment completes, the event is written to an `outbox` table **in the
same database transaction as the payment** (so they can never disagree). A
scheduled relay then publishes outbox rows to Kafka and marks them sent, giving
**at-least-once** delivery. The downstream
[`payment-events-consumer`](https://github.com/sreejith-p-sukumaran/payment-events-consumer)
dedupes, so the *effect* happens exactly once.

```
completePayment()                         OutboxRelay (@Scheduled)
   │  one @Transactional                     │ poll PENDING, oldest-first
   ├─▶ payment   (COMPLETED)                  │ send(key=paymentId, payload).get()
   └─▶ outbox    (PENDING)  ────────────────▶ │ markSent()  ← only after the ack
                                              ▼
                                   Kafka topic: payment-events
                                              │
                                              ▼  (separate repo)
                                   payment-events-consumer  → idempotent
```

## Stack

Kotlin · Spring Boot 3.5 · Spring Data JPA · Flyway · MySQL 8 ·
Spring for Apache Kafka · Java 21 · JUnit 5 · Testcontainers (MySQL + Kafka).

## Run locally

Requires Docker and Java 21.

```bash
# 1. Start Kafka (KRaft), kafka-ui, and MySQL
docker compose up -d

# 2. Run the service (relay + cleanup schedulers start automatically)
./gradlew bootRun
```

- Kafka broker: `localhost:9092`
- **kafka-ui** (inspect the `payment-events` topic): http://localhost:8081
- MySQL: `localhost:3306`, database `payments` (user `payments` / `payments`)

> Note: this learning build has **no HTTP endpoint** — the focus is the
> outbox → relay → Kafka mechanics. Completing a payment is exercised through
> the test suite (`PaymentService.completePayment`), not a REST call.

## Test

```bash
./gradlew test
```

Tests use Testcontainers, so **Docker must be running**. They spin up real MySQL
and (for the end-to-end tests) a real Kafka broker — nothing is mocked at the
infrastructure boundary.

## How it's built (by phase)

| Phase | What | Where |
|------|------|------|
| 0 | Local Kafka (KRaft) + kafka-ui via docker-compose | `docker-compose.yml` |
| 1 | Outbox schema; payment + outbox written in one transaction | `service/PaymentService.kt`, `db/migration/V1__*.sql` |
| 2 | Scheduled relay → Kafka, keyed by payment id | `relay/OutboxRelay.kt` |
| 3 | At-least-once: mark SENT only after confirmed publish | `relay/OutboxRelay.kt` + `OutboxRelayAtLeastOnceTest` |
| 5 | Scheduled cleanup of old SENT rows | `relay/OutboxCleanupJob.kt` |
| 6 | Testcontainers end-to-end + partition-ordering test | `e2e/OutboxEndToEndTest.kt` |

*(Phase 4 — the idempotent consumer — lives in the sibling repo.)*

## Project structure

```
src/main/kotlin/com/sreejith/outbox/
  domain/      Payment, OutboxEvent, statuses, event-type constants
  event/       PaymentCompletedEvent (the published contract)
  repository/  PaymentRepository, OutboxEventRepository
  service/     PaymentService (the transactional write)
  relay/       OutboxRelay (publish), OutboxCleanupJob (retention)
  config/      Kafka topic, outbox properties, clock
src/main/resources/db/migration/   Flyway migrations
```

## Related

- [`payment-events-consumer`](https://github.com/sreejith-p-sukumaran/payment-events-consumer)
  — the idempotent consumer. The two services share no code; they meet only at
  the `payment-events` topic.
