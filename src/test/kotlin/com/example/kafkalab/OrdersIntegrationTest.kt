package com.example.kafkalab

import com.example.kafkalab.orders.OrderAnalyticsConsumer
import com.example.kafkalab.orders.OrderConsumer
import com.example.kafkalab.orders.OrderEvent
import com.example.kafkalab.orders.OrderProducer
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.TestPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import java.time.Clock
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Against a real broker, because the parts worth learning — partition assignment, offsets,
 * independent group cursors — are exactly the parts a mock would invent.
 *
 * `@ServiceConnection` is what replaces `spring.kafka.bootstrap-servers` with the container's
 * mapped port; there is no `@DynamicPropertySource` needed.
 */
@SpringBootTest
@Testcontainers
@TestPropertySource(properties = ["kafkalab.orders-topic=orders-test"])
class OrdersIntegrationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val kafka = KafkaContainer("apache/kafka:4.1.0")
    }

    @Autowired
    private lateinit var producer: OrderProducer

    @Autowired
    private lateinit var consumer: OrderConsumer

    @Autowired
    private lateinit var analytics: OrderAnalyticsConsumer

    @Autowired
    private lateinit var clock: Clock

    @BeforeEach
    fun clearHandled() = consumer.clear()

    @Test
    fun `a record is acknowledged with the partition and offset it landed on`() {
        val ack = producer.sendAndWait(event("order-ack", OrderEvent.Status.PLACED))

        assertEquals("orders-test", ack.topic)
        assertEquals("order-ack", ack.key)
        assertTrue(ack.partition in 0..2, "expected one of the three partitions, got ${ack.partition}")
        assertTrue(ack.offset >= 0)
    }

    @Test
    fun `every record for one key lands on one partition, in order`() {
        val acks =
            listOf(OrderEvent.Status.PLACED, OrderEvent.Status.PAID, OrderEvent.Status.SHIPPED)
                .map { producer.sendAndWait(event("order-keyed", it)) }

        assertEquals(1, acks.map { it.partition }.distinct().size, "a single key must not span partitions")
        assertEquals(acks.map { it.offset }.sorted(), acks.map { it.offset }, "offsets must ascend")
    }

    @Test
    fun `the listener consumes what was produced`() {
        producer.sendAndWait(event("order-consumed", OrderEvent.Status.PLACED))

        await().atMost(Duration.ofSeconds(20)).until {
            consumer.handled().any { it.event.orderId == "order-consumed" }
        }

        val handled = consumer.handled().first { it.event.orderId == "order-consumed" }
        assertEquals(OrderEvent.Status.PLACED, handled.event.status)
    }

    @Test
    fun `a second group reads the same records, because a topic is a log and not a queue`() {
        val before = analytics.count

        producer.sendAndWait(event("order-fanout", OrderEvent.Status.PAID))

        await().atMost(Duration.ofSeconds(20)).until {
            consumer.handled().any { it.event.orderId == "order-fanout" }
        }
        await().atMost(Duration.ofSeconds(20)).until { analytics.count > before }
    }

    private fun event(
        orderId: String,
        status: OrderEvent.Status,
    ) = OrderEvent(orderId, status, 1999, clock.instant())
}
