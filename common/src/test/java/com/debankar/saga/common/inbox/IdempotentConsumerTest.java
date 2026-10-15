package com.debankar.saga.common.inbox;

import com.debankar.saga.common.messaging.MessageEnvelope;
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
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class IdempotentConsumerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;

    @BeforeAll
    static void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS held_seat_scratch (seat_id TEXT)");
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    @AfterEach
    void cleanUpTables() {
        jdbcTemplate.update("DELETE FROM inbox_message");
        jdbcTemplate.update("DELETE FROM held_seat_scratch");
    }

    private IdempotentConsumer newConsumer() {
        TransactionTemplate transactionTemplate =
                new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        return new IdempotentConsumer(new InboxDao(jdbcTemplate), transactionTemplate, new SimpleMeterRegistry());
    }

    private MessageEnvelope envelope(UUID messageId) {
        return new MessageEnvelope(messageId, "HoldSeats", "Booking", "booking-1", Instant.now(), "{}");
    }

    @Test
    void firstDeliveryRunsEffectSecondDeliveryIsSkipped() {
        MessageEnvelope envelope = envelope(UUID.randomUUID());
        Runnable holdSeat = () -> jdbcTemplate.update("INSERT INTO held_seat_scratch (seat_id) VALUES (?)", "A1");

        boolean firstResult = newConsumer().processOnce(envelope, holdSeat);
        boolean secondResult = newConsumer().processOnce(envelope, holdSeat);

        assertThat(firstResult).isTrue();
        assertThat(secondResult).isFalse();
        Integer rows = jdbcTemplate.queryForObject("SELECT count(*) FROM held_seat_scratch", Integer.class);
        assertThat(rows).isEqualTo(1);
    }

    @Test
    void differentMessagesAreIndependentlyProcessed() {
        Runnable holdSeat = () -> jdbcTemplate.update("INSERT INTO held_seat_scratch (seat_id) VALUES (?)", "A1");

        boolean first = newConsumer().processOnce(envelope(UUID.randomUUID()), holdSeat);
        boolean second = newConsumer().processOnce(envelope(UUID.randomUUID()), holdSeat);

        assertThat(first).isTrue();
        assertThat(second).isTrue();
        Integer rows = jdbcTemplate.queryForObject("SELECT count(*) FROM held_seat_scratch", Integer.class);
        assertThat(rows).isEqualTo(2);
    }

    @Test
    void concurrentDeliveryOfTheSameMessageAppliesTheEffectExactlyOnce() throws Exception {
        MessageEnvelope envelope = envelope(UUID.randomUUID());
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Runnable holdSeatZ9 = () -> {
                jdbcTemplate.update("INSERT INTO held_seat_scratch (seat_id) VALUES (?)", "Z9");
                awaitBarrier(barrier);
            };

            Future<Boolean> resultA = executor.submit(() -> newConsumer().processOnce(envelope, holdSeatZ9));
            Future<Boolean> resultB = executor.submit(() -> newConsumer().processOnce(envelope, holdSeatZ9));

            boolean claimedA = resultA.get(10, TimeUnit.SECONDS);
            boolean claimedB = resultB.get(10, TimeUnit.SECONDS);

            assertThat(claimedA ^ claimedB).as("exactly one caller should claim the message").isTrue();

            Integer heldRows = jdbcTemplate.queryForObject("SELECT count(*) FROM held_seat_scratch", Integer.class);
            assertThat(heldRows).as("the loser's hold must be rolled back, not just its inbox row").isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    private void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}