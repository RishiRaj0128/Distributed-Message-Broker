package com.broker.consumer;

import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Idempotent consumer illustrating effectively-exactly-once processing semantics:
 * Combines at-least-once delivery from broker partitions with local downstream
 * deduplication state (idempotency key / transaction ledger).
 *
 * This mirrors the payment webhook deduplication pattern from financial transaction systems:
 * Even if a network retry or rebalance causes duplicate message delivery, downstream state
 * mutations are executed exactly once.
 */
public class IdempotentConsumer {
    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

    private final String consumerId;
    private final String groupId;
    private final ClusterRegistry registry;
    private final Set<String> processedMessageKeys = ConcurrentHashMap.newKeySet();
    private final List<Message> processedRecords = new ArrayList<>();

    public IdempotentConsumer(String consumerId, String groupId, ClusterRegistry registry) {
        this.consumerId = Objects.requireNonNull(consumerId);
        this.groupId = Objects.requireNonNull(groupId);
        this.registry = Objects.requireNonNull(registry);
    }

    public String getConsumerId() { return consumerId; }
    public String getGroupId() { return groupId; }

    /**
     * Polls and processes messages idempotently.
     *
     * @param topicName Topic to poll
     * @param partitionId Partition to poll
     * @param maxRecords Max batch size
     * @param recordHandler Side-effect handler
     * @return Number of newly processed (non-duplicate) messages
     */
    public synchronized int pollAndProcess(String topicName, int partitionId, int maxRecords, Consumer<Message> recordHandler) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) return 0;
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) return 0;

        long currentOffset = registry.getOffsetTracker().getCommittedOffset(groupId, topicName, partitionId);
        List<Message> batch = partition.readCommitted(currentOffset, maxRecords);

        int newlyProcessedCount = 0;
        for (Message msg : batch) {
            String dedupKey = msg.getId(); // or business key msg.getKey()
            if (processedMessageKeys.add(dedupKey)) {
                // First time processing: execute business logic
                recordHandler.accept(msg);
                processedRecords.add(msg);
                newlyProcessedCount++;
            } else {
                // Duplicate delivery suppressed
                log.info("Consumer {} (group {}): Suppressed duplicate delivery for key {}",
                        consumerId, groupId, dedupKey);
            }
            // Advance offset monotonically
            registry.getOffsetTracker().commitOffset(groupId, topicName, partitionId, msg.getOffset() + 1);
        }

        return newlyProcessedCount;
    }

    public List<Message> getProcessedRecords() {
        return Collections.unmodifiableList(processedRecords);
    }

    public int getDeduplicatedCount() {
        return processedMessageKeys.size();
    }
}
