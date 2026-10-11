package com.debankar.saga.payment;

import com.debankar.saga.common.inbox.IdempotentConsumer;
import com.debankar.saga.common.outbox.OutboxDao;
import com.debankar.saga.payment.gateway.PaymentDao;
import com.debankar.saga.payment.gateway.PaymentService;
import com.debankar.saga.payment.kafka.PaymentCommandListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class PaymentConfiguration {

    @Bean
    public PaymentDao paymentDao(JdbcTemplate jdbcTemplate) {
        return new PaymentDao(jdbcTemplate);
    }

    @Bean
    public PaymentService paymentService(PaymentDao paymentDao,
                                         PlatformTransactionManager transactionManager,
                                         @Value("${payment.slow-payment-delay-ms:3000}") long slowPaymentDelayMs) {
        return new PaymentService(paymentDao, transactionManager, Duration.ofMillis(slowPaymentDelayMs));
    }

    @Bean
    public PaymentCommandListener paymentCommandListener(PaymentService paymentService,
                                                         IdempotentConsumer idempotentConsumer,
                                                         OutboxDao outboxDao,
                                                         @Qualifier("outboxObjectMapper") ObjectMapper objectMapper) {
        return new PaymentCommandListener(paymentService, idempotentConsumer, outboxDao, objectMapper);
    }
}