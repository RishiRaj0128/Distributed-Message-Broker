package com.broker.backpressure;

import com.broker.core.ClusterRegistry;
import com.broker.core.OffsetTracker;
import com.broker.core.Partition;
import com.broker.core.Topic;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Monitors and calculates consumer lag across topic partitions and consumer groups.
 * Lag = (Partition HighWatermark - Consumer Committed Offset).
 */
public class LagTracker {
    private final ClusterRegistry registry;

    public record PartitionLag(String groupId, String topic, int partitionId, long highWatermark, long committedOffset, long lag) {}

    public LagTracker(ClusterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
    }

    public long getLag(String groupId, String topicName, int partitionId) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) return 0L;
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) return 0L;

        long hw = partition.getHighWatermark();
        long committed = registry.getOffsetTracker().getCommittedOffset(groupId, topicName, partitionId);
        return Math.max(0L, hw - committed);
    }

    /**
     * Gets maximum consumer lag across all consumer groups for a given partition.
     */
    public long getMaxLagForPartition(String topicName, int partitionId) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) return 0L;
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) return 0L;

        long hw = partition.getHighWatermark();
        long maxLag = 0L;

        for (Map.Entry<OffsetTracker.PartitionKey, Long> entry : registry.getOffsetTracker().getAllCommittedOffsets().entrySet()) {
            var key = entry.getKey();
            if (key.topic().equals(topicName) && key.partitionId() == partitionId) {
                long committed = entry.getValue();
                long lag = Math.max(0L, hw - committed);
                if (lag > maxLag) {
                    maxLag = lag;
                }
            }
        }

        return maxLag;
    }

    public Map<String, PartitionLag> getAllLags() {
        Map<String, PartitionLag> lags = new ConcurrentHashMap<>();
        for (Map.Entry<OffsetTracker.PartitionKey, Long> entry : registry.getOffsetTracker().getAllCommittedOffsets().entrySet()) {
            var key = entry.getKey();
            Topic topic = registry.getTopic(key.topic());
            if (topic != null) {
                Partition partition = topic.getPartition(key.partitionId());
                if (partition != null) {
                    long hw = partition.getHighWatermark();
                    long committed = entry.getValue();
                    long lag = Math.max(0L, hw - committed);
                    String id = key.groupId() + ":" + key.topic() + ":" + key.partitionId();
                    lags.put(id, new PartitionLag(key.groupId(), key.topic(), key.partitionId(), hw, committed, lag));
                }
            }
        }
        return lags;
    }
}
