package com.debankar.saga.inventory;

import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.inventory.kafka.InventoryCommandListener;
import com.debankar.saga.inventory.seat.SeatDao;
import com.debankar.saga.inventory.seat.SeatExpirySweeper;
import com.debankar.saga.inventory.seat.SeatHoldService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class InventoryConfiguration {

    @Bean
    public SeatDao seatDao(JdbcTemplate jdbcTemplate) {
        return new SeatDao(jdbcTemplate);
    }

    @Bean
    public SeatHoldService seatHoldService(JdbcTemplate jdbcTemplate,
                                           PlatformTransactionManager transactionManager,
                                           @Value("${inventory.hold-duration-seconds:300}") long holdDurationSeconds) {
        return new SeatHoldService(jdbcTemplate, transactionManager, Duration.ofSeconds(holdDurationSeconds));
    }

    @Bean
    public SeatExpirySweeper seatExpirySweeper(JdbcTemplate jdbcTemplate,
                                               OutboxDao outboxDao,
                                               @Qualifier("outboxObjectMapper") ObjectMapper objectMapper,
                                               PlatformTransactionManager transactionManager) {
        return new SeatExpirySweeper(jdbcTemplate, outboxDao, objectMapper,
                new TransactionTemplate(transactionManager));
    }

    @Bean
    public SeatExpirySweeperScheduler seatExpirySweeperScheduler(SeatExpirySweeper sweeper) {
        return new SeatExpirySweeperScheduler(sweeper);
    }

    @Bean
    public InventoryCommandListener inventoryCommandListener(SeatHoldService seatHoldService,
                                                             IdempotentConsumer idempotentConsumer,
                                                             OutboxDao outboxDao,
                                                             @Qualifier("outboxObjectMapper") ObjectMapper objectMapper) {
        return new InventoryCommandListener(seatHoldService, idempotentConsumer, outboxDao, objectMapper);
    }

    static class SeatExpirySweeperScheduler {

        private final SeatExpirySweeper sweeper;

        SeatExpirySweeperScheduler(SeatExpirySweeper sweeper) {
            this.sweeper = sweeper;
        }

        @Scheduled(fixedDelayString = "${inventory.sweep-delay-ms:1000}")
        void sweep() {
            sweeper.sweepExpiredHolds();
        }
    }
}