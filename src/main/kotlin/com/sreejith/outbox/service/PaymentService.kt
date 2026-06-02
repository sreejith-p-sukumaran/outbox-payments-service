package com.sreejith.outbox.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.outbox.domain.OutboxEvent
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.domain.Payment
import com.sreejith.outbox.domain.PaymentEvents
import com.sreejith.outbox.domain.PaymentStatus
import com.sreejith.outbox.event.PaymentCompletedEvent
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.util.UUID

@Service
class PaymentService(
	private val paymentRepository: PaymentRepository,
	private val outboxEventRepository: OutboxEventRepository,
	private val objectMapper: ObjectMapper,
	private val clock: Clock,
) {
	/**
	 * Completes a payment and records its [PaymentCompletedEvent] in the outbox
	 * within a single transaction. Either both rows commit or neither does —
	 * this is the whole point of the outbox pattern: no dual write to Kafka.
	 */
	@Transactional
	fun completePayment(command: CompletePaymentCommand): Payment {
		val now = clock.instant()

		val payment = Payment(
			id = command.paymentId,
			amount = command.amount,
			currency = command.currency,
			status = PaymentStatus.COMPLETED,
			createdAt = now,
			completedAt = now,
		)
		paymentRepository.save(payment)

		// The event id is also the outbox row id, so the consumer can dedupe on it.
		val eventId = UUID.randomUUID().toString()
		val event = PaymentCompletedEvent(
			eventId = eventId,
			paymentId = payment.id,
			amount = payment.amount,
			currency = payment.currency,
			occurredAt = now,
		)
		val outboxEvent = OutboxEvent(
			id = eventId,
			aggregateType = PaymentEvents.AGGREGATE_TYPE,
			aggregateId = payment.id,
			eventType = PaymentEvents.PAYMENT_COMPLETED,
			payload = objectMapper.writeValueAsString(event),
			status = OutboxStatus.PENDING,
			createdAt = now,
		)
		outboxEventRepository.save(outboxEvent)

		return payment
	}
}
