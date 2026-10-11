package com.debankar.saga.common.outbox;

import com.debankar.saga.common.messaging.MessageEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Publishes unpublished outbox rows to Kafka, one small batch per call.
 *
 * <p>{@link #pollAndPublish()} runs entirely inside one transaction taken
 * from the injected {@link TransactionTemplate} rather than an
 * {@code @Transactional} annotation on this method: this class is not a
 * Spring proxy target, and a self-invoked {@code @Transactional} method
 * silently skips the proxy and runs with no transaction at all. Locking the
 * rows ({@code FOR UPDATE SKIP LOCKED}), publishing them, and marking them
 * published all happen before the transaction commits, so a crash between
 * "Kafka accepted the send" and "commit" rolls the whole batch back and
 * republishes it on the next poll. Downstream consumers rely on their inbox
 * table to make that redelivery a no-op.
 */
public class OutboxRelay {

    private final OutboxDao outboxDao;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;

    public OutboxRelay(OutboxDao outboxDao,
                       KafkaTemplate<String, String> kafkaTemplate,
                       ObjectMapper objectMapper,
                       TransactionTemplate transactionTemplate,
                       int batchSize) {
        this.outboxDao = outboxDao;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
    }

    /**
     * Publishes up to {@code batchSize} unpublished rows and returns how
     * many were published. Safe to call repeatedly and concurrently: rows
     * already locked by another caller are skipped, not blocked on.
     */
    public int pollAndPublish() {
        return transactionTemplate.execute(status -> {
            List<OutboxRecord> batch = outboxDao.findUnpublished(batchSize);
            for (OutboxRecord record : batch) {
                publish(record);
                outboxDao.markPublished(record.id());
            }
            return batch.size();
        });
    }

    private void publish(OutboxRecord record) {
        MessageEnvelope envelope = record.toEnvelope();
        String json;
        try {
            json = objectMapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox record " + record.id(), e);
        }

        try {
            // .get() blocks until the broker acknowledges the send (or
            // throws), so a broker-side failure aborts this transaction and
            // the row stays unpublished for the next poll.
            kafkaTemplate.send(record.topic(), record.messageKey(), json).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish outbox record " + record.id(), e);
        }
    }
}