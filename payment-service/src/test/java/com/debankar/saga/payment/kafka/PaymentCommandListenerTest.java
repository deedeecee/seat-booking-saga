package com.debankar.saga.payment.kafka;

import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.inbox.InboxDao;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.debankar.saga.payment.gateway.Payment;
import com.debankar.saga.payment.gateway.PaymentDao;
import com.debankar.saga.payment.gateway.PaymentService;
import com.debankar.saga.payment.gateway.PaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PaymentCommandListenerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;
    static ObjectMapper objectMapper;

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
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    @AfterEach
    void cleanUp() {
        jdbcTemplate.update("DELETE FROM outbox_message");
        jdbcTemplate.update("DELETE FROM inbox_message");
    }

    private PaymentCommandListener newListener() {
        PaymentService paymentService = new PaymentService(
                new PaymentDao(jdbcTemplate), new DataSourceTransactionManager(dataSource), Duration.ZERO);
        IdempotentConsumer idempotentConsumer = new IdempotentConsumer(
                new InboxDao(jdbcTemplate), new TransactionTemplate(new DataSourceTransactionManager(dataSource)));
        return new PaymentCommandListener(paymentService, idempotentConsumer, new OutboxDao(jdbcTemplate), objectMapper);
    }

    private static String envelopeJson(UUID messageId, String eventType, String bookingId, Object payload) throws Exception {
        String payloadJson = objectMapper.writeValueAsString(payload);
        MessageEnvelope envelope =
                new MessageEnvelope(messageId, eventType, "Booking", bookingId, Instant.now(), payloadJson);
        return objectMapper.writeValueAsString(envelope);
    }

    private List<OutboxRecord> outboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void authorizePaymentCommandAuthorizesAndPublishesPaymentAuthorized() throws Exception {
        String json = envelopeJson(UUID.randomUUID(), "AuthorizePayment", "booking-1",
                new PaymentCommandListener.AuthorizePaymentPayload(4200));

        newListener().onCommand(json);

        Payment payment = new PaymentDao(jdbcTemplate).findByBookingId("booking-1").orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(payment.amountCents()).isEqualTo(4200);

        List<OutboxRecord> events = outboxRowsFor("booking-1");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("PaymentAuthorized");
    }

    @Test
    void authorizePaymentCommandForADeclinePrefixedBookingPublishesPaymentDeclined() throws Exception {
        String json = envelopeJson(UUID.randomUUID(), "AuthorizePayment", "decline-booking-2",
                new PaymentCommandListener.AuthorizePaymentPayload(1000));

        newListener().onCommand(json);

        Payment payment = new PaymentDao(jdbcTemplate).findByBookingId("decline-booking-2").orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.DECLINED);

        List<OutboxRecord> events = outboxRowsFor("decline-booking-2");
        assertThat(events).hasSize(1);
        assertThat(events.get(0).eventType()).isEqualTo("PaymentDeclined");
    }

    @Test
    void voidPaymentCommandVoidsAnAuthorizedPaymentAndPublishesNoEvent() throws Exception {
        newListener().onCommand(envelopeJson(UUID.randomUUID(), "AuthorizePayment", "booking-3",
                new PaymentCommandListener.AuthorizePaymentPayload(1000)));
        jdbcTemplate.update("DELETE FROM outbox_message"); // isolate what void itself publishes

        // VoidPayment carries no payload — handleVoid never reads one — so
        // this sends an empty JSON object rather than a type-specific record.
        String json = envelopeJson(UUID.randomUUID(), "VoidPayment", "booking-3", Map.of());
        newListener().onCommand(json);

        Payment payment = new PaymentDao(jdbcTemplate).findByBookingId("booking-3").orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.VOIDED);
        assertThat(outboxRowsFor("booking-3")).isEmpty();
    }

    @Test
    void sameAuthorizePaymentMessageDeliveredTwiceAppliesItsEffectExactlyOnce() throws Exception {
        String json = envelopeJson(UUID.randomUUID(), "AuthorizePayment", "booking-4",
                new PaymentCommandListener.AuthorizePaymentPayload(777));

        PaymentCommandListener listener = newListener();
        listener.onCommand(json);
        listener.onCommand(json); // simulated redelivery, identical message

        List<OutboxRecord> events = outboxRowsFor("booking-4");
        assertThat(events)
                .as("a redelivered command must not publish a second PaymentAuthorized event")
                .hasSize(1);
    }
}