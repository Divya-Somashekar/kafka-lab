package com.example.kafkalab.orders

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

/**
 * The fulfilment consumer group. One `@KafkaListener` is one consumer in one group.
 *
 * Delivery here is **at-least-once**: `application.yml` sets `enable-auto-commit: false` and lets
 * the container commit the offset *after* this method returns normally, so a crash mid-method
 * replays the record. The handler must therefore be idempotent — which is a property of your code,
 * not something Kafka can give you.
 *
 * Throwing from here does not drop the record either. The container's default error handler
 * retries and then, with no dead-letter topic configured, gives up and logs — see the exercises in
 * `README.md`.
 *
 * [handled] exists so the tests and the REST endpoint can see what arrived. A real consumer writes
 * to a database instead; an in-memory list is not state you can trust after a restart.
 */
@Component
class OrderConsumer(
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val received = ConcurrentLinkedQueue<Handled>()

    @KafkaListener(topics = ["\${kafkalab.orders-topic}"], groupId = "fulfilment")
    fun onOrderEvent(record: ConsumerRecord<String, String>) {
        val event = objectMapper.readValue(record.value(), OrderEvent::class.java)
        log.info(
            "fulfilment consumed key={} partition={} offset={} status={}",
            record.key(),
            record.partition(),
            record.offset(),
            event.status,
        )
        received += Handled(record.partition(), record.offset(), event)
    }

    fun handled(): List<Handled> = received.toList()

    fun clear() = received.clear()

    data class Handled(
        val partition: Int,
        val offset: Long,
        val event: OrderEvent,
    )
}

/**
 * A second group over the same topic — the point of a log over a queue.
 *
 * `analytics` keeps its own offsets, so it reads every record `fulfilment` reads. Adding a consumer
 * to the *same* group would have split the partitions between them instead. One topic, two
 * independent readers, no republishing: that is the fan-out a queue cannot do.
 */
@Component
class OrderAnalyticsConsumer {
    private val log = LoggerFactory.getLogger(javaClass)

    private val consumed = AtomicLong()

    val count: Long get() = consumed.get()

    @KafkaListener(topics = ["\${kafkalab.orders-topic}"], groupId = "analytics")
    fun onOrderEvent(record: ConsumerRecord<String, String>) {
        val total = consumed.incrementAndGet()
        log.info("analytics consumed key={} offset={} (total={})", record.key(), record.offset(), total)
    }
}
