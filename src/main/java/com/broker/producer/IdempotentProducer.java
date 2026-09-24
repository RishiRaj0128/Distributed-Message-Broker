package com.broker.producer;

import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Topic;
import com.broker.election.MetadataService;
import com.broker.replication.AckMode;
import com.broker.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Idempotent message producer supporting at-least-once delivery with
 * exactly-once semantics at the partition log boundary.
 */
public class IdempotentProducer {
    private static final Logger log = LoggerFactory.getLogger(IdempotentProducer.class);

    private final String producerId;
    private final ClusterRegistry registry;
    private final ReplicationManager replicationManager;
    private final MetadataService metadataService;
    private final ProducerSequenceTracker sequenceTracker;
    private final int maxRetries;
    private final boolean idempotenceEnabled;

    // Topic:PartitionId -> next sequence number
    private final Map<String, AtomicLong> sequenceNumbers = new ConcurrentHashMap<>();

    public IdempotentProducer(String producerId,
                              ClusterRegistry registry,
                              ReplicationManager replicationManager,
                              MetadataService metadataService,
                              ProducerSequenceTracker sequenceTracker,
                              int maxRetries,
                              boolean idempotenceEnabled) {
        this.producerId = Objects.requireNonNullElseGet(producerId, () -> "prod-" + UUID.randomUUID().toString().substring(0, 8));
        this.registry = registry;
        this.replicationManager = replicationManager;
        this.metadataService = metadataService;
        this.sequenceTracker = sequenceTracker;
        this.maxRetries = maxRetries;
        this.idempotenceEnabled = idempotenceEnabled;
    }

    public IdempotentProducer(ClusterRegistry registry,
                              ReplicationManager replicationManager,
                              MetadataService metadataService,
                              ProducerSequenceTracker sequenceTracker) {
        this(null, registry, replicationManager, metadataService, sequenceTracker, 3, true);
    }

    public String getProducerId() {
        return producerId;
    }

    public Message send(String topicName, String key, byte[] payload, AckMode ackMode) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }

        int partitionId = topic.selectPartitionForKey(key);
        String seqKey = topicName + ":" + partitionId;
        long sequenceNumber = -1L;

        if (idempotenceEnabled) {
            sequenceNumber = sequenceNumbers.computeIfAbsent(seqKey, k -> new AtomicLong(0L)).getAndIncrement();
        }

        return sendWithRetry(topicName, partitionId, key, payload, sequenceNumber, ackMode, 0);
    }

    private Message sendWithRetry(String topicName, int partitionId, String key, byte[] payload,
                                  long sequenceNumber, AckMode ackMode, int attempt) {
        try {
            // Check broker side deduplication
            if (idempotenceEnabled) {
                var dedupCheck = sequenceTracker.checkAndValidate(producerId, topicName, partitionId, sequenceNumber);
                if (dedupCheck.isDuplicate()) {
                    log.info("Producer {}: Received duplicate ack from broker for offset {}", producerId, dedupCheck.assignedOffset());
                    return new Message(UUID.randomUUID().toString(), key, payload, Map.of(), null,
                            producerId, sequenceNumber, dedupCheck.assignedOffset());
                }
            }

            Message message = new Message(
                    UUID.randomUUID().toString(),
                    key,
                    payload,
                    Map.of(),
                    null,
                    producerId,
                    sequenceNumber,
                    -1L
            );

            Message committed = replicationManager.appendAndReplicate(topicName, partitionId, message, ackMode);

            if (idempotenceEnabled) {
                sequenceTracker.recordCommittedSequence(producerId, topicName, partitionId, sequenceNumber, committed.getOffset());
            }

            return committed;
        } catch (Exception ex) {
            if (attempt < maxRetries) {
                log.warn("Send failed for {}-p{} attempt {}/{}. Retrying with backoff. Error: {}",
                        topicName, partitionId, attempt + 1, maxRetries, ex.getMessage());
                try {
                    Thread.sleep(50L * (attempt + 1));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                return sendWithRetry(topicName, partitionId, key, payload, sequenceNumber, ackMode, attempt + 1);
            }
            throw new RuntimeException("Failed to produce message after " + maxRetries + " retries", ex);
        }
    }
}
