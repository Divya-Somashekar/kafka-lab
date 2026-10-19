package com.example.kafkalab

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class KafkaLabApplication

fun main(args: Array<String>) {
    runApplication<KafkaLabApplication>(*args)
}
