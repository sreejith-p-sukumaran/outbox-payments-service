package com.sreejith.outbox.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.domain.PaymentEvents
import com.sreejith.outbox.domain.PaymentStatus
import com.sreejith.outbox.event.PaymentCompletedEvent
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import com.sreejith.outbox.support.AbstractMySqlIntegrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PaymentOutboxTransactionTest(
	@Autowired private val paymentService: PaymentService,
	@Autowired private val paymentRepository: PaymentRepository,
	@Autowired private val outboxEventRepository: OutboxEventRepository,
	@Autowired private val objectMapper: ObjectMapper,
) : AbstractMySqlIntegrationTest() {

	@BeforeEach
	fun clean() {
		outboxEventRepository.deleteAll()
		paymentRepository.deleteAll()
	}

	@Test
	fun `completing a payment commits the payment and its outbox event together`() {
		val command = CompletePaymentCommand(
			paymentId = "11111111-1111-1111-1111-111111111111",
			amount = BigDecimal("49.99"),
			currency = "EUR",
		)

		paymentService.completePayment(command)

		val payment = paymentRepository.findById(command.paymentId).orElseThrow()
		assertThat(payment.status).isEqualTo(PaymentStatus.COMPLETED)
		assertThat(payment.completedAt).isNotNull()

		val outbox = outboxEventRepository.findAll().single()
		assertThat(outbox.aggregateType).isEqualTo(PaymentEvents.AGGREGATE_TYPE)
		assertThat(outbox.aggregateId).isEqualTo(command.paymentId)
		assertThat(outbox.eventType).isEqualTo(PaymentEvents.PAYMENT_COMPLETED)
		assertThat(outbox.status).isEqualTo(OutboxStatus.PENDING)
		assertThat(outbox.sentAt).isNull()

		// The payload is the published contract; its event id matches the row id.
		val event = objectMapper.readValue(outbox.payload, PaymentCompletedEvent::class.java)
		assertThat(event.eventId).isEqualTo(outbox.id)
		assertThat(event.paymentId).isEqualTo(command.paymentId)
		assertThat(event.amount).isEqualByComparingTo(BigDecimal("49.99"))
		assertThat(event.currency).isEqualTo("EUR")
	}
}
