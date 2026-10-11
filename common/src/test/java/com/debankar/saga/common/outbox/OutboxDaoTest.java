package com.debankar.saga.common.outbox;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxDaoTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static HikariDataSource dataSource;
    static JdbcTemplate jdbcTemplate;
    static TransactionTemplate transactionTemplate;

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
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    private OutboxDao newDao() {
        return new OutboxDao(jdbcTemplate);
    }

    @Test
    void insertedRowIsReadableAsUnpublished() {
        OutboxDao dao = newDao();
        OutboxRecord record = OutboxRecord.newRecord(
                "Booking", "booking-1", "inventory.commands", "booking-1",
                "HoldSeats", "{\"seatIds\":[\"A1\"]}");

        transactionTemplate.executeWithoutResult(status -> dao.insert(record));

        assertThat(dao.findUnpublished(50))
                .extracting(OutboxRecord::id)
                .contains(record.id());
    }

    @Test
    void insertRollsBackWithCallersTransaction() {
        OutboxDao dao = newDao();
        OutboxRecord record = OutboxRecord.newRecord(
                "Booking", "booking-2", "inventory.commands", "booking-2",
                "HoldSeats", "{\"seatIds\":[\"B1\"]}");

        transactionTemplate.executeWithoutResult(status -> {
            dao.insert(record);
            status.setRollbackOnly();
        });

        assertThat(dao.findUnpublished(50))
                .extracting(OutboxRecord::id)
                .doesNotContain(record.id());
    }

    @Test
    void markPublishedRemovesRowFromUnpublishedQuery() {
        OutboxDao dao = newDao();
        OutboxRecord record = OutboxRecord.newRecord(
                "Booking", "booking-3", "inventory.commands", "booking-3",
                "HoldSeats", "{\"seatIds\":[\"C1\"]}");

        transactionTemplate.executeWithoutResult(status -> dao.insert(record));
        transactionTemplate.executeWithoutResult(status -> dao.markPublished(record.id()));

        assertThat(dao.findUnpublished(50))
                .extracting(OutboxRecord::id)
                .doesNotContain(record.id());
    }

    @Test
    void findUnpublishedOrdersByCreatedAtAscending() throws InterruptedException {
        OutboxDao dao = newDao();
        OutboxRecord first = OutboxRecord.newRecord(
                "Booking", "booking-4", "inventory.commands", "booking-4", "HoldSeats", "{}");
        Thread.sleep(5);
        OutboxRecord second = OutboxRecord.newRecord(
                "Booking", "booking-5", "inventory.commands", "booking-5", "HoldSeats", "{}");

        transactionTemplate.executeWithoutResult(status -> {
            dao.insert(second);
            dao.insert(first);
        });

        List<OutboxRecord> unpublished = dao.findUnpublished(50);
        assertThat(indexOfId(unpublished, first.id())).isLessThan(indexOfId(unpublished, second.id()));
    }

    private static int indexOfId(List<OutboxRecord> records, UUID id) {
        for (int i = 0; i < records.size(); i++) {
            if (records.get(i).id().equals(id)) return i;
        }
        throw new AssertionError("id not found: " + id);
    }
}