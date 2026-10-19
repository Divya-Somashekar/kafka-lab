package com.example.kafkalab.orders

import com.example.kafkalab.config.KafkaLabProperties
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.CompletableFuture

/**
 * Produces onto the orders topic.
 *
 * Two things worth noticing:
 *
 * 1. **`send` is asynchronous and returns a future.** Nothing has been durably stored when it
 *    returns — the record sits in the client's accumulator until a batch is full or `linger.ms`
 *    expires. Fire-and-forget plus `acks=all` is a contradiction: you asked for the strongest
 *    durability and then threw away the acknowledgement. [sendAndWait] blocks on the future so
 *    the REST endpoint can report the real partition and offset.
 * 2. **The value is a plain JSON string.** Spring's `JsonSerializer` would work too, but it adds
 *    a type header and the bytes on the wire stop matching what you read in `kafka-console-consumer`
 *    — which is where most of the learning happens while the topic is small.
 */
@Component
class OrderProducer(
    private val kafka: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val properties: KafkaLabProperties,
) {
    fun send(event: OrderEvent): CompletableFuture<SendResult<String, String>> =
        kafka.send(properties.ordersTopic, event.orderId, objectMapper.writeValueAsString(event))

    /** Blocks until the broker has acknowledged, and reports where the record landed. */
    fun sendAndWait(event: OrderEvent): Ack {
        val metadata = send(event).join().recordMetadata
        return Ack(metadata.topic(), metadata.partition(), metadata.offset(), event.orderId)
    }

    data class Ack(
        val topic: String,
        val partition: Int,
        val offset: Long,
        val key: String,
    )
}
