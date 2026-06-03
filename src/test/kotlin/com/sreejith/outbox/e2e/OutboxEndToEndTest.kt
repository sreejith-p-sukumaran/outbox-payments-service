package com.sreejith.outbox.e2e

import com.sreejith.outbox.domain.OutboxEvent
import com.sreejith.outbox.domain.OutboxStatus
import com.sreejith.outbox.domain.PaymentEvents
import com.sreejith.outbox.relay.OutboxRelay
import com.sreejith.outbox.repository.OutboxEventRepository
import com.sreejith.outbox.repository.PaymentRepository
import com.sreejith.outbox.service.CompletePaymentCommand
import com.sreejith.outbox.service.PaymentService
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant

/**
 * End-to-end over real MySQL and a real Kafka broker (both Testcontainers):
 * the producer side of the flow, from completing a payment to the event
 * landing on the `payment-events` topic. The consumer's half of the flow lives
 * in the payment-events-consumer repo; the two meet at this topic.
 */
@Testcontainers
@SpringBootTest(
	webEnvironment = SpringBootTest.WebEnvironment.NONE,
	// Drive the relay manually for determinism.
	properties = ["outbox.relay.poll-interval=1h"],
)
class OutboxEndToEndTest(
	@Autowired private val paymentService: PaymentService,
	@Autowired private val paymentRepository: PaymentRepository,
	@Autowired private val outboxEventRepository: OutboxEventRepository,
	@Autowired private val relay: OutboxRelay,
) {
	companion object {
		@Container
		@JvmStatic
		val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4")
			.withDatabaseName("payments")
			.withUsername("payments")
			.withPassword("payments")

		@Container
		@JvmStatic
		val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))

		@DynamicPropertySource
		@JvmStatic
		fun props(registry: DynamicPropertyRegistry) {
			registry.add("spring.datasource.url", mysql::getJdbcUrl)
			registry.add("spring.datasource.username", mysql::getUsername)
			registry.add("spring.datasource.password", mysql::getPassword)
			registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
		}
	}

	private lateinit var consumer: Consumer<String, String>

	@BeforeEach
	fun setUp() {
		outboxEventRepository.deleteAll()
		paymentRepository.deleteAll()

		val props = KafkaTestUtils.consumerProps(kafka.bootstrapServers, "e2e-test", "true")
		props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
		consumer = DefaultKafkaConsumerFactory(props, StringDeserializer(), StringDeserializer())
			.createConsumer()
		consumer.subscribe(listOf("payment-events"))
	}

	@AfterEach
	fun tearDown() {
		consumer.close()
	}

	@Test
	fun `completing a payment lands a single event on the topic keyed by payment id`() {
		val paymentId = "e2e-pay-1"
		paymentService.completePayment(
			CompletePaymentCommand(paymentId, BigDecimal("75.00"), "EUR"),
		)

		relay.relayPendingEvents()

		val records = recordsForKey(paymentId)
		assertThat(records).hasSize(1)
		assertThat(records.single().value()).contains(paymentId)
		assertThat(outboxEventRepository.findAll().single().status).isEqualTo(OutboxStatus.SENT)
	}

	@Test
	fun `events for one payment keep their order on a single partition`() {
		val paymentId = "e2e-order"
		val base = Instant.parse("2026-06-02T09:00:00Z")
		// Three events for the same aggregate, created in a defined order.
		(1..3).forEach { seq ->
			outboxEventRepository.save(
				OutboxEvent(
					id = "order-evt-$seq",
					aggregateType = PaymentEvents.AGGREGATE_TYPE,
					aggregateId = paymentId,
					eventType = PaymentEvents.PAYMENT_COMPLETED,
					payload = """{"seq":$seq}""",
					status = OutboxStatus.PENDING,
					createdAt = base.plusSeconds(seq.toLong()),
				),
			)
		}

		relay.relayPendingEvents()

		val records = recordsForKey(paymentId)
		assertThat(records).hasSize(3)
		// Same key -> same partition -> order preserved.
		assertThat(records.map { it.partition() }.toSet()).hasSize(1)
		// Parse the seq out of each payload (MySQL's JSON column re-formats the
		// stored text, e.g. inserts a space after the colon, so compare on value).
		val sequence = records.map { Regex("\"seq\"\\s*:\\s*(\\d+)").find(it.value())!!.groupValues[1].toInt() }
		assertThat(sequence).containsExactly(1, 2, 3)
	}

	private fun recordsForKey(key: String): List<ConsumerRecord<String, String>> =
		KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(15))
			.records("payment-events")
			.asSequence()
			.filter { it.key() == key }
			.toList()
}
