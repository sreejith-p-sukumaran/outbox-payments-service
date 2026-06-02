package com.sreejith.outbox.support

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

/**
 * Base class for tests that need a real MySQL. One container is shared across
 * all subclasses (static), and Flyway migrates it on context startup.
 */
@Testcontainers
abstract class AbstractMySqlIntegrationTest {

	companion object {
		@Container
		@JvmStatic
		val mysql: MySQLContainer<*> = MySQLContainer("mysql:8.4")
			.withDatabaseName("payments")
			.withUsername("payments")
			.withPassword("payments")

		@DynamicPropertySource
		@JvmStatic
		fun datasourceProps(registry: DynamicPropertyRegistry) {
			registry.add("spring.datasource.url", mysql::getJdbcUrl)
			registry.add("spring.datasource.username", mysql::getUsername)
			registry.add("spring.datasource.password", mysql::getPassword)
		}
	}
}
