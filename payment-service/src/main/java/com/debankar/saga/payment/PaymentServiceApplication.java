package com.debankar.saga.payment;

import com.debankar.saga.common.inbox.IdempotentConsumerAutoConfiguration;
import com.debankar.saga.common.kafka.KafkaListenerAutoConfiguration;
import com.debankar.saga.common.outbox.OutboxRelayAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({
		OutboxRelayAutoConfiguration.class,
		IdempotentConsumerAutoConfiguration.class,
		KafkaListenerAutoConfiguration.class
})
public class PaymentServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentServiceApplication.class, args);
	}

}
