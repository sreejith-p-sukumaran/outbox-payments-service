package com.sreejith.outbox.service

import com.ninjasquad.springmockk.MockkBean
import com.sreejith.outbox.domain.OutboxEvent
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import com.sreejith.outbox.support.AbstractMySqlIntegrationTest
import io.mockk.every
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal

/**
 * Proves atomicity from the other direction: if the outbox write fails, the
 * payment write must roll back with it. The outbox repository is mocked to
 * throw on save, standing in for any failure during the outbox insert.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PaymentOutboxRollbackTest(
	@Autowired private val paymentService: PaymentService,
	@Autowired private val paymentRepository: PaymentRepository,
) : AbstractMySqlIntegrationTest() {

	@MockkBean
	private lateinit var outboxEventRepository: OutboxEventRepository

	@BeforeEach
	fun clean() {
		paymentRepository.deleteAll()
	}

	@Test
	fun `a failed outbox write rolls back the payment in the same transaction`() {
		every { outboxEventRepository.save(any<OutboxEvent>()) } throws
			RuntimeException("outbox insert failed")

		val command = CompletePaymentCommand(
			paymentId = "22222222-2222-2222-2222-222222222222",
			amount = BigDecimal("10.00"),
			currency = "USD",
		)

		assertThatThrownBy { paymentService.completePayment(command) }
			.isInstanceOf(RuntimeException::class.java)

		// The payment must not survive a failed outbox write — same transaction.
		assertThat(paymentRepository.count()).isZero()
	}
}
