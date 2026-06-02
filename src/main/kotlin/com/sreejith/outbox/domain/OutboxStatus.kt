package com.sreejith.outbox.domain

enum class OutboxStatus {
	/** Written inside the business transaction, not yet published to Kafka. */
	PENDING,

	/** Confirmed published to Kafka by the relay. */
	SENT,
}
