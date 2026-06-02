package com.sreejith.outbox.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class ClockConfig {
	/** A single injectable clock so timestamps can be fixed in tests. */
	@Bean
	fun clock(): Clock = Clock.systemUTC()
}
