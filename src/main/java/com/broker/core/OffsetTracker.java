package com.broker.core;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe consumer offset coordinator tracking committed offsets per group-topic-partition.
 */
public class OffsetTracker {

    public record PartitionKey(String groupId, String topic, int partitionId) {
        public PartitionKey {
            Objects.requireNonNull(groupId, "groupId cannot be null");
            Objects.requireNonNull(topic, "topic cannot be null");
        }
    }

    private final Map<PartitionKey, Long> committedOffsets = new ConcurrentHashMap<>();

    public void commitOffset(String groupId, String topic, int partitionId, long offset) {
        PartitionKey key = new PartitionKey(groupId, topic, partitionId);
        committedOffsets.merge(key, offset, Math::max);
    }

    public long getCommittedOffset(String groupId, String topic, int partitionId) {
        PartitionKey key = new PartitionKey(groupId, topic, partitionId);
        return committedOffsets.getOrDefault(key, 0L);
    }

    public void resetOffset(String groupId, String topic, int partitionId, long offset) {
        PartitionKey key = new PartitionKey(groupId, topic, partitionId);
        committedOffsets.put(key, offset);
    }

    public Map<PartitionKey, Long> getAllCommittedOffsets() {
        return Map.copyOf(committedOffsets);
    }

    public void clear() {
        committedOffsets.clear();
    }
}
