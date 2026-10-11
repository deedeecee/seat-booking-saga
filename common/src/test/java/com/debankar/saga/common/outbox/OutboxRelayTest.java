package com.debankar.saga.common.outbox;

import com.debankar.saga.common.messaging.MessageEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxRelayTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;
    static KafkaTemplate<String, String> kafkaTemplate;
    static ObjectMapper objectMapper;
    static KafkaConsumer<String, String> testConsumer;

    @BeforeAll
    static void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        dataSource = new HikariDataSource(config);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate = new JdbcTemplate(dataSource);

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-relay-test");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(List.of("inventory.commands"));
    }

    @AfterAll
    static void tearDown() {
        testConsumer.close();
        dataSource.close();
    }

    @AfterEach
    void cleanUpTable() {
        jdbcTemplate.update("DELETE FROM outbox_message");
    }

    private OutboxRelay newRelay() {
        TransactionTemplate transactionTemplate =
                new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        return new OutboxRelay(new OutboxDao(jdbcTemplate), kafkaTemplate, objectMapper, transactionTemplate, 50);
    }

    @Test
    void publishesUnpublishedRowToKafkaAndMarksItPublished() {
        OutboxRecord record = OutboxRecord.newRecord(
                "Booking", "booking-100", "inventory.commands", "booking-100",
                "HoldSeats", "{\"seatIds\":[\"A1\",\"A2\"]}");
        OutboxDao dao = new OutboxDao(jdbcTemplate);
        new TransactionTemplate(new DataSourceTransactionManager(dataSource))
                .executeWithoutResult(status -> dao.insert(record));

        int published = newRelay().pollAndPublish();
        assertThat(published).isEqualTo(1);
        assertThat(dao.findUnpublished(50)).isEmpty();

        ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isEqualTo(1);

        ConsumerRecord<String, String> received = records.iterator().next();
        assertThat(received.key()).isEqualTo("booking-100");

        MessageEnvelope envelope = readEnvelope(received.value());
        assertThat(envelope.messageId()).isEqualTo(record.id());
        assertThat(envelope.eventType()).isEqualTo("HoldSeats");
        assertThat(envelope.aggregateId()).isEqualTo("booking-100");
        assertThat(envelope.payload()).isEqualTo("{\"seatIds\":[\"A1\",\"A2\"]}");
    }

    @Test
    void pollWithNothingUnpublishedPublishesNothing() {
        assertThat(newRelay().pollAndPublish()).isZero();
    }

    private MessageEnvelope readEnvelope(String json) {
        try {
            return objectMapper.readValue(json, MessageEnvelope.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}