package com.debankar.saga.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaListenerAutoConfigurationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.0.0");

    static KafkaTemplate<String, String> kafkaTemplate;
    static KafkaConsumer<String, String> dltConsumer;

    @BeforeAll
    static void setUp() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        Map<String, Object> dltConsumerProps = new HashMap<>();
        dltConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        dltConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dltConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dltConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-reader");
        dltConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dltConsumer = new KafkaConsumer<>(dltConsumerProps);
        dltConsumer.subscribe(List.of("poison.topic.DLT"));
    }

    @AfterAll
    static void tearDown() {
        dltConsumer.close();
    }

    @Test
    void poisonMessageRetriesThenLandsInDeadLetterTopic() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        Map<String, Object> listenerConsumerProps = new HashMap<>();
        listenerConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        listenerConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        listenerConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        listenerConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "poison-listener");
        listenerConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        listenerConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        ContainerProperties containerProps = new ContainerProperties("poison.topic");
        containerProps.setMessageListener((MessageListener<String, String>) record -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("always fails: simulated poison message");
        });

        // Small, fast values so the test doesn't wait on production-sized backoff.
        DefaultErrorHandler errorHandler = KafkaListenerAutoConfiguration.buildErrorHandler(
                kafkaTemplate, 2, 20, 2.0, 100);

        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(listenerConsumerProps), containerProps);
        container.setCommonErrorHandler(errorHandler);
        container.start();
        try {
            kafkaTemplate.send("poison.topic", "poison-key", "will always fail").get();

            // A single poll() isn't guaranteed to notice a topic that gets
            // auto-created mid-call (the DLT topic doesn't exist until the
            // recoverer's first publish to it), so poll repeatedly across a
            // longer wall-clock budget instead of trusting one long call.
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(25).toMillis();
            while (received.isEmpty() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = dltConsumer.poll(Duration.ofSeconds(1));
                polled.forEach(received::add);
            }

            assertThat(received)
                    .as("expected one DLT record; listener ran %d time(s)", attempts.get())
                    .hasSize(1);

            ConsumerRecord<String, String> dltRecord = received.get(0);
            assertThat(dltRecord.key()).isEqualTo("poison-key");
            assertThat(dltRecord.value()).isEqualTo("will always fail");

            assertThat(attempts.get())
                    .as("listener should have run more than once before recovery")
                    .isGreaterThan(1);
        } finally {
            container.stop();
        }
    }
}