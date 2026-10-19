package com.example.kafkalab.orders

import java.time.Instant

/**
 * The one event this skeleton publishes.
 *
 * [orderId] is the **message key**, and that is the whole design decision here: Kafka guarantees
 * order within a partition, never across a topic, and the key is what pins a message to a
 * partition (`hash(key) % partitions`). Every event for one order therefore lands on one
 * partition, read by one consumer, in the order it was produced. A null key would round-robin
 * instead, and `PLACED` could be processed after `CANCELLED`.
 */
data class OrderEvent(
    val orderId: String,
    val status: Status,
    val amountMinor: Long,
    val occurredAt: Instant,
) {
    enum class Status { PLACED, PAID, SHIPPED, CANCELLED }
}
