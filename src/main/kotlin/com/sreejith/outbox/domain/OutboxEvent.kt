package com.sreejith.outbox.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

/**
 * A single row in the transactional outbox.
 *
 * The [id] doubles as the event id carried in the published payload, which is
 * what lets the downstream consumer dedupe under at-least-once delivery.
 */
@Entity
@Table(name = "outbox")
class OutboxEvent(
	@Id
	@Column(name = "id", length = 36)
	val id: String,

	@Column(name = "aggregate_type", length = 64, nullable = false)
	val aggregateType: String,

	@Column(name = "aggregate_id", length = 64, nullable = false)
	val aggregateId: String,

	@Column(name = "event_type", length = 64, nullable = false)
	val eventType: String,

	@Column(name = "payload", columnDefinition = "json", nullable = false)
	val payload: String,

	@Enumerated(EnumType.STRING)
	@Column(name = "status", length = 16, nullable = false)
	var status: OutboxStatus,

	@Column(name = "created_at", nullable = false)
	val createdAt: Instant,

	@Column(name = "sent_at")
	var sentAt: Instant? = null,
)
