package com.debankar.saga.booking.kafka;

import com.debankar.saga.booking.saga.Booking;
import com.debankar.saga.booking.saga.BookingDao;
import com.debankar.saga.booking.saga.BookingOrchestrator;
import com.debankar.saga.booking.saga.BookingState;
import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.inbox.InboxDao;
import com.debankar.saga.common.messaging.MessageEnvelope;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.common.outbox.OutboxRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BookingEventListenerTest {

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

    private BookingEventListener newListener() {
        BookingOrchestrator orchestrator = new BookingOrchestrator(
                new BookingDao(jdbcTemplate, objectMapper), new OutboxDao(jdbcTemplate), objectMapper,
                new DataSourceTransactionManager(dataSource));
        IdempotentConsumer idempotentConsumer = new IdempotentConsumer(
                new InboxDao(jdbcTemplate), new TransactionTemplate(new DataSourceTransactionManager(dataSource)), new SimpleMeterRegistry());
        return new BookingEventListener(orchestrator, idempotentConsumer, objectMapper);
    }

    private String seedBooking(BookingState state) {
        String id = "booking-" + UUID.randomUUID();
        Booking booking = new Booking(id, UUID.randomUUID(), List.of("A1"), 5000,
                state, 0, Instant.now(), Instant.now());
        new BookingDao(jdbcTemplate, objectMapper).insert(booking);
        return id;
    }

    private static String envelopeJson(UUID messageId, String eventType, String bookingId) throws Exception {
        MessageEnvelope envelope =
                new MessageEnvelope(messageId, eventType, "Booking", bookingId, Instant.now(), "{}");
        return objectMapper.writeValueAsString(envelope);
    }

    private List<OutboxRecord> outboxRowsFor(String bookingId) {
        return new OutboxDao(jdbcTemplate).findUnpublished(50).stream()
                .filter(r -> r.aggregateId().equals(bookingId))
                .toList();
    }

    @Test
    void inventoryEventAdvancesTheBookingAndPublishesTheNextCommand() throws Exception {
        String bookingId = seedBooking(BookingState.PENDING);
        String json = envelopeJson(UUID.randomUUID(), "SeatsHeld", bookingId);

        newListener().onInventoryEvent(json);

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.HELD);
        assertThat(outboxRowsFor(bookingId)).extracting(OutboxRecord::eventType).containsExactly("AuthorizePayment");
    }

    @Test
    void paymentEventAdvancesTheBookingAndPublishesTheNextCommand() throws Exception {
        String bookingId = seedBooking(BookingState.HELD);
        String json = envelopeJson(UUID.randomUUID(), "PaymentAuthorized", bookingId);

        newListener().onPaymentEvent(json);

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.PAID);
        assertThat(outboxRowsFor(bookingId)).extracting(OutboxRecord::eventType).containsExactly("ConfirmSeats");
    }

    @Test
    void sameEventDeliveredTwiceAppliesItsEffectExactlyOnce() throws Exception {
        String bookingId = seedBooking(BookingState.PENDING);
        String json = envelopeJson(UUID.randomUUID(), "SeatsHeld", bookingId);

        BookingEventListener listener = newListener();
        listener.onInventoryEvent(json);
        listener.onInventoryEvent(json); // simulated redelivery, identical message

        Booking booking = new BookingDao(jdbcTemplate, objectMapper).findById(bookingId).orElseThrow();
        assertThat(booking.state()).isEqualTo(BookingState.HELD);
        assertThat(booking.version())
                .as("a redelivered event must not advance the state a second time")
                .isEqualTo(1);

        List<OutboxRecord> events = outboxRowsFor(bookingId);
        assertThat(events)
                .as("a redelivered event must not publish a second AuthorizePayment command")
                .hasSize(1);
    }

    @Test
    void eventForAnUnknownBookingIsIgnoredWithoutError() throws Exception {
        String json = envelopeJson(UUID.randomUUID(), "SeatsHeld", "no-such-booking");

        newListener().onInventoryEvent(json);
        // No exception is the assertion; there's nothing else to check.
    }
}