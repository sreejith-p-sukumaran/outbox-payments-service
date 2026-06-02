package com.sreejith.outbox.relay

import com.sreejith.outbox.config.OutboxProperties
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.repository.OutboxEventRepository
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * Polls the outbox for PENDING rows and publishes them to Kafka, keyed by the
 * payment id so all events for one payment land on the same partition (ordering).
 *
 * Single-instance assumption: this relay does not lock rows, which is fine for
 * one running instance. To scale to multiple instances you would add
 * `SELECT ... FOR UPDATE SKIP LOCKED` on the fetch so instances claim disjoint
 * batches. That is intentionally left out to keep the polling path simple.
 */
@Component
class OutboxRelay(
	private val outboxEventRepository: OutboxEventRepository,
	private val kafkaTemplate: KafkaTemplate<String, String>,
	private val properties: OutboxProperties,
	private val clock: Clock,
) {
	private val log = LoggerFactory.getLogger(OutboxRelay::class.java)

	@Scheduled(fixedDelayString = "\${outbox.relay.poll-interval}")
	fun relayPendingEvents() {
		val batch = outboxEventRepository.findByStatusOrderByCreatedAtAscIdAsc(
			OutboxStatus.PENDING,
			PageRequest.of(0, properties.relay.batchSize),
		)
		if (batch.isEmpty()) return

		var published = 0
		for (event in batch) {
			// Block until the broker confirms the publish, THEN mark SENT. If we
			// crash in between, the row stays PENDING and is re-sent next tick —
			// at-least-once. Stop the batch on the first failure so we never
			// publish a later event for the same payment ahead of an earlier one.
			kafkaTemplate.send(properties.topic, event.aggregateId, event.payload).get()
			outboxEventRepository.markSent(event.id, clock.instant())
			published++
		}
		log.info("Relayed {} outbox event(s) to {}", published, properties.topic)
	}
}
