package com.debankar.saga.common.inbox;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration(proxyBeanMethods = false)
public class IdempotentConsumerAutoConfiguration {

    @Bean
    public InboxDao inboxDao(JdbcTemplate jdbcTemplate) {
        return new InboxDao(jdbcTemplate);
    }

    @Bean
    public IdempotentConsumer idempotentConsumer(InboxDao inboxDao,
                                                 PlatformTransactionManager transactionManager,
                                                 MeterRegistry meterRegistry) {
        return new IdempotentConsumer(inboxDao, new TransactionTemplate(transactionManager), meterRegistry);
    }
}