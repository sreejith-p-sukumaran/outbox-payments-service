package com.sreejith.outbox.relay

import com.sreejith.outbox.config.OutboxProperties
import com.sreejith.outbox.repository.OutboxEventRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock

/**
 * Periodically deletes SENT outbox rows older than the retention window. The
 * outbox is a relay buffer, not an event store — once a row is safely published
 * and past retention it has no further purpose, and pruning keeps the polling
 * index small. PENDING rows are never eligible for deletion.
 */
@Component
class OutboxCleanupJob(
	private val outboxEventRepository: OutboxEventRepository,
	private val properties: OutboxProperties,
	private val clock: Clock,
) {
	private val log = LoggerFactory.getLogger(OutboxCleanupJob::class.java)

	@Scheduled(fixedDelayString = "\${outbox.cleanup.interval}")
	fun cleanupPublishedEvents() {
		val cutoff = clock.instant().minus(properties.cleanup.retention)
		val deleted = outboxEventRepository.deleteSentBefore(cutoff)
		if (deleted > 0) {
			log.info("Cleaned up {} published outbox row(s) older than {}", deleted, cutoff)
		}
	}
}
