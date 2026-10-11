package com.debankar.saga.payment.gateway;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PaymentDaoTest {

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
        dataSource = new HikariDataSource(config);

        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();

        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    private PaymentDao newDao() {
        return new PaymentDao(jdbcTemplate);
    }

    @Test
    void insertIfAbsentCreatesANewAuthorizedPayment() {
        PaymentDao dao = newDao();

        Payment payment = dao.insertIfAbsent("booking-1", PaymentStatus.AUTHORIZED, 5000);

        assertThat(payment.bookingId()).isEqualTo("booking-1");
        assertThat(payment.status()).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(payment.amountCents()).isEqualTo(5000);
    }

    @Test
    void insertIfAbsentOnAnExistingBookingReturnsTheOriginalStatusNotANewDecision() {
        PaymentDao dao = newDao();
        dao.insertIfAbsent("booking-2", PaymentStatus.AUTHORIZED, 1000);

        // A second attempt with a DIFFERENT decision must not overwrite the first.
        Payment second = dao.insertIfAbsent("booking-2", PaymentStatus.DECLINED, 9999);

        assertThat(second.status()).isEqualTo(PaymentStatus.AUTHORIZED);
        assertThat(second.amountCents()).isEqualTo(1000);
    }

    @Test
    void findByBookingIdReturnsEmptyWhenNoPaymentExists() {
        Optional<Payment> found = newDao().findByBookingId("no-such-booking");
        assertThat(found).isEmpty();
    }

    @Test
    void voidPaymentMovesAnAuthorizedPaymentToVoided() {
        PaymentDao dao = newDao();
        dao.insertIfAbsent("booking-3", PaymentStatus.AUTHORIZED, 2000);

        dao.voidPayment("booking-3");

        assertThat(dao.findByBookingId("booking-3").orElseThrow().status()).isEqualTo(PaymentStatus.VOIDED);
    }

    @Test
    void voidingAnAlreadyVoidedPaymentIsASafeNoOp() {
        PaymentDao dao = newDao();
        dao.insertIfAbsent("booking-4", PaymentStatus.AUTHORIZED, 2000);
        dao.voidPayment("booking-4");

        dao.voidPayment("booking-4"); // second void: must not error or change anything

        assertThat(dao.findByBookingId("booking-4").orElseThrow().status()).isEqualTo(PaymentStatus.VOIDED);
    }

    @Test
    void voidingADeclinedPaymentDoesNothingSinceItWasNeverAuthorized() {
        PaymentDao dao = newDao();
        dao.insertIfAbsent("booking-5", PaymentStatus.DECLINED, 2000);

        dao.voidPayment("booking-5");

        assertThat(dao.findByBookingId("booking-5").orElseThrow().status())
                .as("a declined payment was never authorized, so void must not touch it")
                .isEqualTo(PaymentStatus.DECLINED);
    }
}