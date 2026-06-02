package com.sreejith.outbox.relay

import com.sreejith.outbox.domain.OutboxEvent
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.domain.PaymentEvents
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.support.AbstractMySqlIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.time.Instant

@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	// Retain published rows for one day; the test seeds rows around that window.
	properties = [
		"outbox.cleanup.retention=1d",
		"outbox.cleanup.interval=1h",
		"outbox.relay.poll-interval=1h",
	],
)
class OutboxCleanupJobTest(
	@Autowired private val cleanupJob: OutboxCleanupJob,
	@Autowired private val outboxEventRepository: OutboxEventRepository,
) : AbstractMySqlIntegrationTest() {

	@BeforeEach
	fun clean() {
		outboxEventRepository.deleteAll()
	}

	@Test
	fun `cleanup deletes old sent rows but keeps recent and pending ones`() {
		val now = Instant.now()

		outboxEventRepository.saveAll(
			listOf(
				row("old-sent", OutboxStatus.SENT, sentAt = now.minus(Duration.ofDays(2))),
				row("recent-sent", OutboxStatus.SENT, sentAt = now.minus(Duration.ofHours(1))),
				// PENDING is old but unpublished — must never be deleted.
				row("old-pending", OutboxStatus.PENDING, sentAt = null, createdAt = now.minus(Duration.ofDays(5))),
			),
		)

		cleanupJob.cleanupPublishedEvents()

		val remaining = outboxEventRepository.findAll().map { it.id }.toSet()
		assertThat(remaining).containsExactlyInAnyOrder("recent-sent", "old-pending")
	}

	private fun row(
		id: String,
		status: OutboxStatus,
		sentAt: Instant?,
		createdAt: Instant = Instant.now(),
	) = OutboxEvent(
		id = id,
		aggregateType = PaymentEvents.AGGREGATE_TYPE,
		aggregateId = "pay-$id",
		eventType = PaymentEvents.PAYMENT_COMPLETED,
		payload = """{"eventId":"$id"}""",
		status = status,
		createdAt = createdAt,
		sentAt = sentAt,
	)
}
