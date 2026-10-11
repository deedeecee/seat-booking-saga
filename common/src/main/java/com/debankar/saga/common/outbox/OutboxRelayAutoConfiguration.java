package com.debankar.saga.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Wires the outbox relay into a service. Imported explicitly by each
 * service's {@code @SpringBootApplication} class rather than picked up by
 * component scanning, so which services run an outbox relay is visible at a
 * glance from their application class.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class OutboxRelayAutoConfiguration {

    @Bean
    public OutboxDao outboxDao(JdbcTemplate jdbcTemplate) {
        return new OutboxDao(jdbcTemplate);
    }

    /**
     * A mapper dedicated to outbox envelopes, kept separate from whatever
     * mapper Spring MVC ends up using for HTTP JSON later, so the two never
     * fight over configuration or bean priority.
     */
    @Bean
    public ObjectMapper outboxObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public OutboxRelay outboxRelay(OutboxDao outboxDao,
                                   KafkaTemplate<String, String> kafkaTemplate,
                                   @Qualifier("outboxObjectMapper") ObjectMapper objectMapper,
                                   PlatformTransactionManager transactionManager,
                                   @Value("${outbox.batch-size:100}") int batchSize) {
        return new OutboxRelay(outboxDao, kafkaTemplate, objectMapper,
                new TransactionTemplate(transactionManager), batchSize);
    }

    @Bean
    public OutboxRelayScheduler outboxRelayScheduler(OutboxRelay outboxRelay) {
        return new OutboxRelayScheduler(outboxRelay);
    }

    static class OutboxRelayScheduler {

        private final OutboxRelay outboxRelay;

        OutboxRelayScheduler(OutboxRelay outboxRelay) {
            this.outboxRelay = outboxRelay;
        }

        @Scheduled(fixedDelayString = "${outbox.poll-delay-ms:200}")
        void poll() {
            outboxRelay.pollAndPublish();
        }
    }
}