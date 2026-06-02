package com.sreejith.outbox.relay

import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import com.sreejith.outbox.service.CompletePaymentCommand
import com.sreejith.outbox.service.PaymentService
import com.sreejith.outbox.support.AbstractMySqlIntegrationTest
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import java.math.BigDecimal
import java.time.Duration

@EmbeddedKafka(partitions = 3, topics = ["payment-events"])
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = [
		"spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}",
		// Disable the scheduled run so the test drives the relay deterministically.
		"outbox.relay.poll-interval=1h",
	],
)
class OutboxRelayIntegrationTest(
	@Autowired private val relay: OutboxRelay,
	@Autowired private val paymentService: PaymentService,
	@Autowired private val paymentRepository: PaymentRepository,
	@Autowired private val outboxEventRepository: OutboxEventRepository,
	@Autowired private val embeddedKafka: EmbeddedKafkaBroker,
) : AbstractMySqlIntegrationTest() {

	private lateinit var consumer: Consumer<String, String>

	@BeforeEach
	fun setUp() {
		outboxEventRepository.deleteAll()
		paymentRepository.deleteAll()

		val props = KafkaTestUtils.consumerProps("relay-test", "true", embeddedKafka)
		consumer = DefaultKafkaConsumerFactory(props, StringDeserializer(), StringDeserializer())
			.createConsumer()
		consumer.subscribe(listOf("payment-events"))
	}

	@AfterEach
	fun tearDown() {
		consumer.close()
	}

	@Test
	fun `relay publishes pending events keyed by payment id and marks them sent`() {
		val paymentId = "33333333-3333-3333-3333-333333333333"
		paymentService.completePayment(
			CompletePaymentCommand(paymentId, BigDecimal("12.50"), "EUR"),
		)

		relay.relayPendingEvents()

		val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))
		assertThat(records.count()).isEqualTo(1)
		val record = records.records("payment-events").single()
		assertThat(record.key()).isEqualTo(paymentId)
		assertThat(record.value()).contains(paymentId)

		val outbox = outboxEventRepository.findAll().single()
		assertThat(outbox.status).isEqualTo(OutboxStatus.SENT)
		assertThat(outbox.sentAt).isNotNull()
	}
}
