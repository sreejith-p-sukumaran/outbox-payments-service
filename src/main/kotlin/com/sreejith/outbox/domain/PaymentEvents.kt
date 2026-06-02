package com.sreejith.outbox.domain

/** Stable string constants used for the outbox aggregate/event type columns. */
object PaymentEvents {
	const val AGGREGATE_TYPE = "Payment"
	const val PAYMENT_COMPLETED = "PaymentCompleted"
}
