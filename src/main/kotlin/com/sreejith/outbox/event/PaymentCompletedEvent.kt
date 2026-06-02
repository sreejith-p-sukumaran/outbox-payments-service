package com.sreejith.outbox.event

import java.math.BigDecimal
import java.time.Instant

/**
 * The contract published to the `payment-events` topic as the outbox payload.
 *
 * [eventId] equals the outbox row id; the consumer dedupes on it, which is how
 * at-least-once delivery is made safe.
 */
data class PaymentCompletedEvent(
	val eventId: String,
	val paymentId: String,
	val amount: BigDecimal,
	val currency: String,
	val occurredAt: Instant,
)
