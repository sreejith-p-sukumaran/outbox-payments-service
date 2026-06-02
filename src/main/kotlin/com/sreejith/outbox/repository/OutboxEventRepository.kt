package com.sreejith.outbox.repository

import com.sreejith.outbox.domain.OutboxEvent
import com.sreejith.outbox.domain.OutboxStatus
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

interface OutboxEventRepository : JpaRepository<OutboxEvent, String> {

	/**
	 * Oldest-first batch of unpublished events. Created-at ordering is what
	 * preserves per-payment ordering on the wire; id is a stable tiebreaker.
	 */
	fun findByStatusOrderByCreatedAtAscIdAsc(
		status: OutboxStatus,
		pageable: Pageable,
	): List<OutboxEvent>

	/**
	 * Flips a single row to SENT, but only if it is still PENDING. The status
	 * guard makes a re-mark a no-op (returns 0), so a row published twice under
	 * at-least-once never regresses its sent_at.
	 */
	@Modifying
	@Transactional
	@Query(
		"""
		UPDATE OutboxEvent e
		   SET e.status = com.sreejith.outbox.domain.OutboxStatus.SENT,
		       e.sentAt = :sentAt
		 WHERE e.id = :id
		   AND e.status = com.sreejith.outbox.domain.OutboxStatus.PENDING
		""",
	)
	fun markSent(@Param("id") id: String, @Param("sentAt") sentAt: Instant): Int

	/**
	 * Bulk-deletes published rows whose sent_at is older than [cutoff]. Only SENT
	 * rows are eligible, so unpublished work is never discarded.
	 */
	@Modifying
	@Transactional
	@Query(
		"""
		DELETE FROM OutboxEvent e
		 WHERE e.status = com.sreejith.outbox.domain.OutboxStatus.SENT
		   AND e.sentAt < :cutoff
		""",
	)
	fun deleteSentBefore(@Param("cutoff") cutoff: Instant): Int
}
