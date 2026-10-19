# kafka-lab

A deliberately small Kafka skeleton to get hands on: one topic, one producer, two consumer groups,
a real broker in Docker, and an integration test that runs against a throwaway one.

> The companion write-up is **[Kafka, to interview depth](https://ashtegottaytha.com/learn/kafka/)** —
> partitions and ordering, consumer groups, `acks` and ISR, delivery semantics, and the follow-up
> questions this lab is built to answer.

Kotlin 2.3 · Spring Boot 4.1 · Java 25 · Apache Kafka 4.1 (KRaft) · Testcontainers · Gradle 9.8 (wrapper included)

Everything here is working code — the *learning* is in the [exercises](#exercises), which are
written as "make this break, then fix it" rather than as things to read.

## Quick start

Prerequisites: a running Docker daemon. The Gradle toolchain fetches Java 25 if you lack it.

```bash
# 1. Tests only. Needs nothing running — Testcontainers starts and stops its own broker.
./gradlew test

# 2. Poke it by hand.
docker compose -f local/docker-compose.yml up -d
./gradlew bootRun
```

Then open `local/api.http` in IntelliJ, select the **local** environment, and work down it.

- App + actuator: <http://localhost:8085>
- kafka-ui (topics, messages, consumer-group lag): <http://localhost:8090>
- Broker, from your laptop: `localhost:9092`

**Keep the `bootRun` log visible while you click through `api.http`.** Each consumed record logs
its partition and offset; that output is the actual lesson, not the HTTP responses.

## What is in here

```text
kafka-lab/
├── src/main/kotlin/com/example/kafkalab/
│   ├── KafkaLabApplication.kt
│   ├── config/
│   │   ├── KafkaLabProperties.kt    topic name, partition count
│   │   ├── TopicConfiguration.kt    NewTopic bean — KafkaAdmin creates it at startup
│   │   └── ClockConfiguration.kt
│   ├── orders/
│   │   ├── OrderEvent.kt            the payload, keyed by orderId
│   │   ├── OrderProducer.kt         KafkaTemplate.send, and why the future matters
│   │   └── OrderConsumer.kt         two @KafkaListeners, two groups, one topic
│   └── api/LabController.kt         publish / inspect, for poking by hand
├── src/main/resources/application.yml   every Kafka setting here has a comment saying why
├── src/test/kotlin/…/OrdersIntegrationTest.kt
└── local/
    ├── docker-compose.yml   one KRaft broker + kafka-ui
    ├── api.http             the walkthrough, plus the CLI commands worth knowing
    └── http-client.env.json
```

## The four ideas the skeleton demonstrates

| Idea | Where to look | Why it matters |
|---|---|---|
| The key picks the partition | `OrderEvent` KDoc, `/api/orders/lifecycle` | Ordering is **per partition**, never per topic. A null key round-robins, and `PLACED` can be processed after `CANCELLED`. |
| A group is a cursor, not a queue | `OrderAnalyticsConsumer` | `fulfilment` and `analytics` both read every record. Two consumers in *one* group would have split the partitions instead. |
| `send` is asynchronous | `OrderProducer` | Nothing is durable when `send` returns. Fire-and-forget plus `acks=all` is a contradiction. |
| Delivery is at-least-once | `OrderConsumer` KDoc, `application.yml` | The offset is committed after the handler returns, so a crash replays. Idempotency is your code's job. |

## Exercises

Roughly in order. Each one is a change you make, not a file to read.

1. **Watch a rebalance.** Run a second instance: `SERVER_PORT=8086 ./gradlew bootRun`. The three
   partitions split between the two app instances. Start a third and one consumer sits idle —
   partition count is the ceiling on a group's parallelism. Watch it in kafka-ui's *Consumers* tab.
2. **Lose the ordering guarantee.** Make `OrderProducer` send with a `null` key and run
   `/api/orders/lifecycle` again. The three statuses now spread across partitions, and nothing
   promises you they are processed in order.
3. **Replay the topic.** Stop the app, reset the `fulfilment` group to earliest (the command is at
   the bottom of `local/api.http`), restart. Every record is delivered again. Note `analytics` is
   unaffected — that is what separate cursors buy you.
4. **Make the consumer fail.** Throw in `OrderConsumer.onOrderEvent` for one `orderId`. The default
   error handler retries and then logs — the record is not dropped, and the offset is not committed,
   so a restart retries forever. This is the *blocked partition* failure mode: head-of-line blocking
   stops every later record on that partition too.
5. **Add a dead-letter topic.** Fix exercise 4 properly: a `DefaultErrorHandler` with a
   `DeadLetterPublishingRecoverer` and a `FixedBackOff`. Then decide what you want in the DLT
   headers, and who is going to read them — a DLT nobody drains is a silent data-loss queue.
6. **Make the handler idempotent.** Give `OrderEvent` a dedup key and make a replayed record a
   no-op. This is the only real answer to at-least-once delivery; exactly-once over Kafka exists
   (`transactional.id` + `read_committed`) but only for Kafka-to-Kafka work, not for the database
   write your consumer actually does.
7. **Measure lag.** `kafka-consumer-groups.sh --describe --group fulfilment` while producing in a
   loop. Consumer lag is *the* Kafka alert; latency and error rate tell you nothing about a consumer
   that quietly stopped committing.
8. **Try a schema.** Swap the hand-rolled JSON string for Spring's `JsonSerializer`/
   `JsonDeserializer`, then rename a field in `OrderEvent` and watch the consumer break. That is the
   argument for a schema registry, felt rather than read.

## Deliberate omissions

- **No Kafka Streams, no Connect.** Both are worth learning, and both are a different repo — they
  obscure the producer/consumer/offset mechanics while you are still learning them.
- **No Avro or schema registry.** Exercise 8 is where you earn the need for one.
- **Single broker.** So replication, ISR shrink and `min.insync.replicas` cannot actually be
  demonstrated: `acks=all` with one replica is one disk. Adding two more brokers to the compose file
  is the honest way to study that, and a reasonable ninth exercise.
- **No security.** `PLAINTEXT` everywhere, no SASL, no ACLs. A real cluster has all three.

## Troubleshooting

- **Produce times out after metadata succeeds.** The advertised-listener trap: the client connected
  to `localhost:9092`, was handed `kafka:19092`, and cannot resolve it. The two listeners in
  `local/docker-compose.yml` exist to prevent this; read the comment there.
- **A new consumer group sees nothing.** `auto-offset-reset` only applies when a group has no
  committed offset, and `latest` means "from now on".
- **`--reset-offsets` refuses.** A group with a live member cannot be rewound. Stop the app first.
- **Tests cannot find Docker.** On colima, `build.gradle.kts` points Testcontainers at
  `~/.colima/default/docker.sock`; check that it exists.
