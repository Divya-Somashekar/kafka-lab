package com.example.kafkalab.api

import com.example.kafkalab.orders.OrderAnalyticsConsumer
import com.example.kafkalab.orders.OrderConsumer
import com.example.kafkalab.orders.OrderEvent
import com.example.kafkalab.orders.OrderProducer
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Clock

/**
 * One endpoint per thing worth poking by hand. Read `local/api.http` alongside this.
 */
@RestController
@RequestMapping("/api/orders")
class LabController(
    private val producer: OrderProducer,
    private val consumer: OrderConsumer,
    private val analytics: OrderAnalyticsConsumer,
    private val clock: Clock,
) {
    /** Publish one event. The response is the partition and offset the broker assigned. */
    @PostMapping
    fun publish(
        @RequestParam orderId: String,
        @RequestParam(defaultValue = "PLACED") status: OrderEvent.Status,
        @RequestParam(defaultValue = "1999") amountMinor: Long,
    ): OrderProducer.Ack = producer.sendAndWait(OrderEvent(orderId, status, amountMinor, clock.instant()))

    /**
     * Publish the same key several times. Every record lands on one partition, in order — the
     * guarantee that makes keying by `orderId` the right call.
     */
    @PostMapping("/lifecycle")
    fun lifecycle(
        @RequestParam orderId: String,
    ): List<OrderProducer.Ack> =
        OrderEvent.Status.entries
            .filter { it != OrderEvent.Status.CANCELLED }
            .map { producer.sendAndWait(OrderEvent(orderId, it, 1999, clock.instant())) }

    /** What the `fulfilment` group has seen, and how far behind `analytics` is. */
    @GetMapping("/handled")
    fun handled(): Map<String, Any> =
        mapOf(
            "fulfilment" to consumer.handled(),
            "analyticsCount" to analytics.count,
        )

    @DeleteMapping("/handled")
    fun clear() = consumer.clear()
}
