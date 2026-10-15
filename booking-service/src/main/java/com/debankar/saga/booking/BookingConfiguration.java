package com.debankar.saga.booking;

import com.debankar.saga.booking.kafka.BookingEventListener;
import com.debankar.saga.booking.saga.BookingDao;
import com.debankar.saga.booking.saga.BookingOrchestrator;
import com.debankar.saga.booking.saga.BookingState;
import com.debankar.saga.booking.saga.SagaTimeoutSweeper;
import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.outbox.OutboxDao;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;

@Configuration(proxyBeanMethods = false)
public class BookingConfiguration {

    @Bean
    public BookingDao bookingDao(JdbcTemplate jdbcTemplate,
                                 @Qualifier("outboxObjectMapper") ObjectMapper objectMapper) {
        return new BookingDao(jdbcTemplate, objectMapper);
    }

    @Bean
    public BookingOrchestrator bookingOrchestrator(BookingDao bookingDao,
                                                   OutboxDao outboxDao,
                                                   @Qualifier("outboxObjectMapper") ObjectMapper objectMapper,
                                                   PlatformTransactionManager transactionManager) {
        return new BookingOrchestrator(bookingDao, outboxDao, objectMapper, transactionManager);
    }

    @Bean
    public BookingEventListener bookingEventListener(BookingOrchestrator bookingOrchestrator,
                                                     IdempotentConsumer idempotentConsumer,
                                                     @Qualifier("outboxObjectMapper") ObjectMapper objectMapper) {
        return new BookingEventListener(bookingOrchestrator, idempotentConsumer, objectMapper);
    }

    @Bean
    public SagaTimeoutSweeper sagaTimeoutSweeper(BookingDao bookingDao,
                                                 BookingOrchestrator bookingOrchestrator,
                                                 @Value("${booking.saga-timeout-seconds:60}") long sagaTimeoutSeconds) {
        return new SagaTimeoutSweeper(bookingDao, bookingOrchestrator, java.time.Duration.ofSeconds(sagaTimeoutSeconds));
    }

    @Bean
    public SagaTimeoutSweeperScheduler sagaTimeoutSweeperScheduler(SagaTimeoutSweeper sweeper) {
        return new SagaTimeoutSweeperScheduler(sweeper);
    }

    // One gauge per state, each queried on demand at scrape time.
    @Bean
    public List<Gauge> sagaOutcomeGauges(BookingDao bookingDao, MeterRegistry meterRegistry) {
        List<Gauge> gauges = new ArrayList<>();
        for (BookingState state : BookingState.values()) {
            gauges.add(Gauge.builder("saga.bookings.by_state", bookingDao, dao -> dao.countByState(state))
                    .tag("state", state.name())
                    .description("Current number of bookings in this state")
                    .register(meterRegistry));
        }
        return gauges;
    }

    static class SagaTimeoutSweeperScheduler {

        private final SagaTimeoutSweeper sweeper;

        SagaTimeoutSweeperScheduler(SagaTimeoutSweeper sweeper) {
            this.sweeper = sweeper;
        }

        @org.springframework.scheduling.annotation.Scheduled(fixedDelayString = "${booking.saga-timeout-sweep-delay-ms:2000}")
        void sweep() {
            sweeper.sweepStuckBookings();
        }
    }
}