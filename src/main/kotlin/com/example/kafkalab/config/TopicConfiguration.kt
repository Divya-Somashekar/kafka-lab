package com.example.kafkalab.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

/**
 * Declaring topics as beans lets `KafkaAdmin` create them at startup — handy locally, and the
 * reason you do not need `kafka-topics.sh` to run this project.
 *
 * It is a learning convenience, not a production pattern. In a real deployment the topic is
 * created by whoever owns the cluster, because **partition count is close to permanent**:
 * increasing it later rewrites which partition a key lands on, so the per-key ordering guarantee
 * breaks for every key in flight. `KafkaAdmin` will also never *shrink* or reconfigure an existing
 * topic — if the topic is already there, this bean is silently ignored.
 */
@Configuration
class TopicConfiguration {
    @Bean
    fun ordersTopic(properties: KafkaLabProperties): NewTopic =
        TopicBuilder
            .name(properties.ordersTopic)
            .partitions(properties.partitions)
            .replicas(properties.replicas.toInt())
            .build()
}
