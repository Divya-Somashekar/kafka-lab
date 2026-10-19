package com.example.kafkalab.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * An injected [Clock] rather than `Instant.now()` — the same convention as the sibling repos, and
 * what lets a test assert on an event's `occurredAt` without sleeping.
 */
@Configuration
class ClockConfiguration {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}
