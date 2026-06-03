package com.sreejith.outbox.relay

import com.sreejith.outbox.config.OutboxProperties
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import com.sreejith.outbox.service.CompletePaymentCommand
import com.sreejith.outbox.service.PaymentService
import com.sreejith.outbox.support.AbstractMySqlIntegrationTest
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * At-least-once guarantee: a row flips to SENT only after a confirmed publish.
 * If the process publishes but dies before the SENT update, the row stays
 * PENDING and is re-published next run — a duplicate the consumer must dedupe.
 */
@EmbeddedKafka(partitions = 3, topics = ["payment-events"])
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = [
		"spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
		"outbox.relay.poll-interval=1h",
	],
)
class OutboxRelayAtLeastOnceTest(
	@Autowired private val paymentService: PaymentService,
	@Autowired private val paymentRepository: PaymentRepository,
	@Autowired private val outboxEventRepository: OutboxEventRepository,
	@Autowired private val kafkaTemplate: KafkaTemplate<String, String>,
	@Autowired private val properties: OutboxProperties,
	@Autowired private val clock: Clock,
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
) : AbstractMySqlIntegrationTest() {

	/**
	 * Delegates everything to the real repository but fails the first markSent,
	 * standing in for a process that crashed right after the Kafka publish.
	 */
	private class FailFirstMarkSentRepository(
		private val delegate: OutboxEventRepository,
	) : OutboxEventRepository by delegate {
		private var alreadyFailed = false

		override fun markSent(id: String, sentAt: Instant): Int {
			if (!alreadyFailed) {
				alreadyFailed = true
				throw RuntimeException("crashed after publish, before mark-sent")
			}
			return delegate.markSent(id, sentAt)
		}
	}

	private lateinit var consumer: Consumer<String, String>

	@BeforeEach
	fun setUp() {
		outboxEventRepository.deleteAll()
		paymentRepository.deleteAll()

		val props = KafkaTestUtils.consumerProps("at-least-once-test", "true", embeddedKafka)
		consumer = DefaultKafkaConsumerFactory(props, StringDeserializer(), StringDeserializer())
			.createConsumer()
		consumer.subscribe(listOf("payment-events"))
	}

	@AfterEach
	fun tearDown() {
		consumer.close()
	}

	@Test
	fun `a crash after publish but before mark-sent causes a re-send`() {
		val paymentId = "44444444-4444-4444-4444-444444444444"
		paymentService.completePayment(
			CompletePaymentCommand(paymentId, BigDecimal("99.00"), "EUR"),
		)
		val eventId = outboxEventRepository.findAll().single().id

		val relay = OutboxRelay(
			FailFirstMarkSentRepository(outboxEventRepository),
			kafkaTemplate,
			properties,
			clock,
		)

		// Run 1: publish succeeds, then the SENT update blows up.
		assertThatThrownBy { relay.relayPendingEvents() }
			.isInstanceOf(RuntimeException::class.java)

		// The row was published but never marked — still PENDING, ready to re-send.
		assertThat(outboxEventRepository.findById(eventId).get().status)
			.isEqualTo(OutboxStatus.PENDING)

		// Run 2: re-publishes the same event (a duplicate) and marks it SENT.
		relay.relayPendingEvents()

		assertThat(outboxEventRepository.findById(eventId).get().status)
			.isEqualTo(OutboxStatus.SENT)

		// Two physical records for the one logical event — at-least-once in action.
		// Scope to this test's key: the embedded broker is shared via the context
		// cache, so the topic may carry records from other tests.
		val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))
			.records("payment-events").asSequence()
			.filter { it.key() == paymentId }
			.toList()
		assertThat(records).hasSize(2)
		assertThat(records.map { it.value() }.toSet()).hasSize(1)
	}
}
