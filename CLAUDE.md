# CLAUDE.md

## Project Overview

A personal learning repo: a minimal Kafka skeleton — one topic, one producer, two consumer groups —
built to be modified by hand. Kotlin 2.3 / Spring Boot 4.1 / Java 25 / Apache Kafka 4.1 in KRaft
mode, reusing the build conventions of the sibling `redis-lab` and `product-search` repos
(hand-written Gradle, ktlint, Testcontainers with the colima socket shim).

Not a service to deploy — no Dockerfile, no `deploy/` entry. The deliverable is the skeleton, its
one integration test, `local/api.http`, and the exercise list in `README.md`.

**Read `README.md` first.** It carries the exercises, and they are the point of the repo.

## The rule that matters most

**Keep it a skeleton.** The value here is a surface small enough to read in one sitting and change
without fear. Resist adding Kafka Streams, Connect, Avro, a schema registry, a second topic or more
abstraction layers — every one of those is listed in README's *Deliberate omissions* or *Exercises*
for a reason, and belongs there until the exercise that needs it is actually being done.

If a feature cannot be explained in a KDoc comment that says *why*, it is too big for this repo.

## Layout

```text
kafka-lab/
├── build.gradle.kts, settings.gradle.kts   one Gradle project, root is the repo
├── src/main/kotlin/com/example/kafkalab/
│   ├── KafkaLabApplication.kt
│   ├── config/     KafkaLabProperties, TopicConfiguration (NewTopic bean), Clock
│   ├── orders/     OrderEvent, OrderProducer, OrderConsumer (+ OrderAnalyticsConsumer)
│   └── api/        LabController — publish and inspect, for poking by hand
├── src/main/resources/application.yml
├── src/test/kotlin/…/OrdersIntegrationTest.kt
└── local/          docker-compose.yml (broker + kafka-ui), api.http, http-client.env.json
```

No hexagonal layering here, unlike `user-service` — four flat packages, deliberately. A learning
skeleton with ports and adapters would hide the Kafka API behind exactly the indirection the repo
exists to show.

## Commands

```bash
./gradlew test          # Testcontainers starts its own apache/kafka broker
./gradlew check         # + ktlint
./gradlew ktlintFormat
./gradlew bootRun       # needs local/docker-compose.yml up
docker compose -f local/docker-compose.yml up -d
```

## Conventions

- **App on 8085, broker on 9092, kafka-ui on 8090.** Not 8080/8081/8082/8083 — yatra, user-service,
  redis-lab and product-search hold those, and several run at once. 9092 is free across the repos.
- **`name: kafka-lab` in the compose file is mandatory.** Without it Compose names the project after
  the directory, which is `local` in every sibling repo, and they share volumes.
- **Every Kafka setting in `application.yml` carries a comment saying why**, including the defaults
  that were left alone. An uncommented tuning knob in a learning repo is noise.
- **No mocks in tests.** Partition assignment, offsets and group cursors are exactly what a mock
  would invent. `@ServiceConnection` on the `KafkaContainer` wires the bootstrap servers.
- **The topic name comes from `kafkalab.orders-topic`**, referenced as a placeholder in
  `@KafkaListener`, so the test can point the same code at `orders-test`.

## Things that will bite

- **Two advertised listeners, and both are load-bearing.** A client always reconnects to the address
  the broker *advertises*, not the one it dialled. Containers resolve `kafka:19092`; the laptop needs
  `localhost:9092`. Collapse them into one and the symptom is a successful metadata fetch followed by
  every produce timing out — which looks nothing like a networking problem.
- **Internal-topic replication factors must be 1 on a single broker.** `__consumer_offsets` defaults
  to 3 replicas and never reaches in-sync with one node, so a consumer group hangs on join with no
  obvious error.
- **`KafkaAdmin` creates topics but never changes them.** If a `NewTopic` bean's partition count
  changes, an existing topic keeps the old one, silently. Delete the topic (or the volume) to apply.
- **Partition count is close to permanent.** Raising it rehashes which partition a key lands on, so
  per-key ordering breaks for every key in flight. That is why `TopicConfiguration` says a real
  cluster's topics belong to whoever owns the cluster.
- **Delivery is at-least-once and the repo says so in three places.** `enable-auto-commit: false`
  plus `ack-mode: record` commits after the handler returns, so a crash replays. Do not add
  wording anywhere that implies exactly-once — Kafka transactions only cover Kafka-to-Kafka work,
  never the database write a consumer actually performs.
- **`enable.idempotence: true` is what makes producer retries safe.** With retries and without it, a
  retried batch can be appended twice or out of order after a transient failure.
