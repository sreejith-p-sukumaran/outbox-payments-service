package com.sreejith.outbox.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.math.BigDecimal
import java.time.Instant

@Entity
@Table(name = "payment")
class Payment(
	@Id
	@Column(name = "id", length = 36)
	val id: String,

	@Column(name = "amount", nullable = false)
	val amount: BigDecimal,

	@Column(name = "currency", length = 3, nullable = false)
	val currency: String,

	@Enumerated(EnumType.STRING)
	@Column(name = "status", length = 20, nullable = false)
	var status: PaymentStatus,

	@Column(name = "created_at", nullable = false)
	val createdAt: Instant,

	@Column(name = "completed_at")
	var completedAt: Instant? = null,
)
