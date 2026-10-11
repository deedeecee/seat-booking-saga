package com.debankar.saga.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

/**
 * Gives every {@code @KafkaListener} in the service the same consumer
 * settings, retry policy, and dead-letter handling, via a bean named
 * {@code kafkaListenerContainerFactory} — the exact name Spring Boot's own
 * auto-configuration would otherwise register under, so this one is picked
 * up automatically without any listener needing to reference it by name.
 *
 * <p>A failing listener retries with exponential backoff; once the retry
 * budget is spent, {@link DeadLetterPublishingRecoverer} publishes the
 * original record, unchanged, to {@code <topic>.DLT}. The destination is
 * an explicit resolver rather than the recoverer's own default, and
 * partition verification is off: DLT records always go to partition 0 of
 * the DLT topic regardless of which partition the original record was on.
 * DLT topics here are low-volume, for manual investigation and replay, so
 * consolidating onto one partition is a fine trade for not depending on a
 * freshly auto-created topic having its partition metadata fully
 * propagated at the moment of publish.
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class KafkaListenerAutoConfiguration {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id:#{null}}") String configuredGroupId,
            @Value("${spring.application.name}") String applicationName,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${kafka.retry.max-attempts:3}") int maxAttempts,
            @Value("${kafka.retry.initial-interval-ms:200}") long initialIntervalMs,
            @Value("${kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${kafka.retry.max-interval-ms:5000}") long maxIntervalMs) {

        String groupId = (configuredGroupId != null && !configuredGroupId.isBlank())
                ? configuredGroupId : applicationName;

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps));
        factory.setCommonErrorHandler(buildErrorHandler(
                kafkaTemplate, maxAttempts, initialIntervalMs, multiplier, maxIntervalMs));
        return factory;
    }

    static DefaultErrorHandler buildErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                 int maxAttempts,
                                                 long initialIntervalMs,
                                                 double multiplier,
                                                 long maxIntervalMs) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", 0));
        recoverer.setVerifyPartition(false);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxAttempts);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}