package com.sreejith.outbox.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties(prefix = "outbox")
data class OutboxProperties(
	/** Kafka topic the relay publishes payment events to. */
	val topic: String,
	val relay: Relay,
	val cleanup: Cleanup,
) {
	data class Relay(
		/** How often the relay polls for PENDING rows. */
		val pollInterval: Duration,
		/** Maximum rows drained per relay run. */
		val batchSize: Int,
	)

	data class Cleanup(
		/** How long SENT rows are kept before the sweep deletes them. */
		val retention: Duration,
		/** How often the cleanup sweep runs. */
		val interval: Duration,
	)
}
