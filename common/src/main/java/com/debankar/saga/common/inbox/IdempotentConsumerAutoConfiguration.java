package com.debankar.saga.common.inbox;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Wires the idempotent-consumer helper into a service. Imported explicitly
 * by each service's {@code @SpringBootApplication} class, matching how
 * {@code OutboxRelayAutoConfiguration} is imported.
 */
@Configuration(proxyBeanMethods = false)
public class IdempotentConsumerAutoConfiguration {

    @Bean
    public InboxDao inboxDao(JdbcTemplate jdbcTemplate) {
        return new InboxDao(jdbcTemplate);
    }

    @Bean
    public IdempotentConsumer idempotentConsumer(InboxDao inboxDao,
                                                 PlatformTransactionManager transactionManager) {
        return new IdempotentConsumer(inboxDao, new TransactionTemplate(transactionManager));
    }
}