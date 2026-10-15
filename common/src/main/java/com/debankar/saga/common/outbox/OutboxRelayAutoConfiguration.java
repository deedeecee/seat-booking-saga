package com.debankar.saga.common.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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

@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class OutboxRelayAutoConfiguration {

    @Bean
    public OutboxDao outboxDao(JdbcTemplate jdbcTemplate) {
        return new OutboxDao(jdbcTemplate);
    }

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

    // Queried on demand at scrape time, not polled separately — always
    // reflects the real row count, with no refresh logic to keep in sync.
    @Bean
    public Gauge outboxBacklogGauge(OutboxDao outboxDao, MeterRegistry meterRegistry) {
        return Gauge.builder("saga.outbox.backlog", outboxDao, OutboxDao::countUnpublished)
                .description("Outbox rows not yet published to Kafka")
                .register(meterRegistry);
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