package io.github.LeoRam0s.intelbras_event_engine.infra.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Creates the development topics. Producers should use {@code did} as the
 * record key, fall back to {@code deviceId}, and route records without either
 * identifier to the dead-letter topic.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics kafkaTopics(
            @Value("${app.kafka.topics.device-events.name:device-events}") String eventsTopic,
            @Value("${app.kafka.topics.partitions:6}") int partitions,
            @Value("${app.kafka.topics.replication-factor:1}") int replicationFactor
    ) {
        NewTopic eventsTopicDefinition = TopicBuilder
                .name(eventsTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();

        NewTopic deadLetterTopicDefinition = TopicBuilder
                .name(eventsTopic + "-dlt")
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();

        return new KafkaAdmin.NewTopics(
                eventsTopicDefinition,
                deadLetterTopicDefinition
        );
    }
}
