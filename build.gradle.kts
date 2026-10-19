plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.1"
    id("io.spring.dependency-management") version "1.1.7"
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"
}

group = "com.example"
version = "0.1.0"
description = "A hands-on skeleton for learning Kafka from Spring Boot"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    // KafkaTemplate, @KafkaListener and the kafka-clients the broker speaks to. The starter
    // rather than a bare org.springframework.kafka:spring-kafka: Boot 4 moved the Kafka
    // auto-configuration (and the Testcontainers @ServiceConnection factory) into
    // spring-boot-kafka, which only the starter pulls in.
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()

    // Testcontainers only probes DOCKER_HOST, ~/.testcontainers.properties and
    // /var/run/docker.sock, so it does not find colima's socket on its own.
    val colimaSocket = file("${System.getProperty("user.home")}/.colima/default/docker.sock")
    if (!file("/var/run/docker.sock").exists() && colimaSocket.exists()) {
        if (System.getenv("DOCKER_HOST") == null) {
            environment("DOCKER_HOST", "unix://${colimaSocket.absolutePath}")
        }
        if (System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == null) {
            environment("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
        }
    }
}

tasks.jar {
    enabled = false
}
