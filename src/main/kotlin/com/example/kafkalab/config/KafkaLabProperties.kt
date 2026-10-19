package com.example.kafkalab.config

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Topic and group names live here rather than in annotations so a test can point the same code at
 * a throwaway topic. `@KafkaListener(topics = "...")` accepts a SpEL/property placeholder for
 * exactly this reason — see [com.example.kafkalab.orders.OrderConsumer].
 */
@ConfigurationProperties("kafkalab")
data class KafkaLabProperties(
    val ordersTopic: String,
    val partitions: Int,
    val replicas: Short,
)
