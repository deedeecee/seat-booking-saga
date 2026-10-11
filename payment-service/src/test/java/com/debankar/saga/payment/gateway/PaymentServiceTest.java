package com.debankar.saga.payment.gateway;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PaymentServiceTest {

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

    private PaymentService newService(Duration slowPaymentDelay) {
        return new PaymentService(
                new PaymentDao(jdbcTemplate), new DataSourceTransactionManager(dataSource), slowPaymentDelay);
    }

    private PaymentService newService() {
        return newService(Duration.ZERO);
    }

    @Test
    void authorizingAnOrdinaryBookingIdSucceeds() {
        AuthorizeResult result = newService().authorize("booking-1", 1500);

        assertThat(result).isInstanceOf(AuthorizeResult.Authorized.class);
        assertThat(((AuthorizeResult.Authorized) result).amountCents()).isEqualTo(1500);
    }

    @Test
    void authorizingADeclinePrefixedBookingIdIsDeclined() {
        AuthorizeResult result = newService().authorize("decline-booking-2", 1500);

        assertThat(result).isInstanceOf(AuthorizeResult.Declined.class);
        Payment payment = new PaymentDao(jdbcTemplate).findByBookingId("decline-booking-2").orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.DECLINED);
    }

    @Test
    void authorizingTheSameBookingTwiceReturnsTheSameOutcomeBothTimes() {
        PaymentService service = newService();

        AuthorizeResult first = service.authorize("booking-3", 2000);
        AuthorizeResult second = service.authorize("booking-3", 9999);

        assertThat(first).isInstanceOf(AuthorizeResult.Authorized.class);
        assertThat(second).isInstanceOf(AuthorizeResult.Authorized.class);
        assertThat(((AuthorizeResult.Authorized) second).amountCents())
                .as("the second call must report the first call's committed amount, not its own")
                .isEqualTo(2000);
    }

    @Test
    void slowPrefixedBookingIdActuallyDelaysBeforeAuthorizing() {
        PaymentService service = newService(Duration.ofMillis(300));

        Instant before = Instant.now();
        AuthorizeResult result = service.authorize("slow-booking-4", 1000);
        Instant after = Instant.now();

        assertThat(result).isInstanceOf(AuthorizeResult.Authorized.class);
        assertThat(Duration.between(before, after))
                .as("the injected delay should have actually elapsed")
                .isGreaterThanOrEqualTo(Duration.ofMillis(300));
    }

    @Test
    void voidingAnAuthorizedPaymentMarksItVoided() {
        PaymentService service = newService();
        service.authorize("booking-5", 3000);

        service.voidPayment("booking-5");

        Payment payment = new PaymentDao(jdbcTemplate).findByBookingId("booking-5").orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.VOIDED);
    }
}