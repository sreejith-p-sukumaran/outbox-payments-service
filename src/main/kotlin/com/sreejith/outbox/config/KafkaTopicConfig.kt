package com.sreejith.outbox.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
@EnableConfigurationProperties(OutboxProperties::class)
class KafkaTopicConfig(private val properties: OutboxProperties) {
	/**
	 * Declares the payment-events topic so KafkaAdmin creates it on startup.
	 * Multiple partitions let events for different payments scale out while
	 * keeping per-payment ordering (events share a partition via the payment id key).
	 */
	@Bean
	fun paymentEventsTopic(): NewTopic =
		TopicBuilder.name(properties.topic)
			.partitions(3)
			.replicas(1)
			.build()
}
