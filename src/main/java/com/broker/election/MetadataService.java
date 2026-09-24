package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.replication.IsrTracker;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight cluster metadata coordinator allowing clients (producers & consumers)
 * to discover current partition leaders and replicas.
 */
public class MetadataService {

    public record PartitionMetadata(
            String topic,
            int partitionId,
            String leaderBrokerId,
            BrokerNode leaderEndpoint,
            List<String> allReplicas,
            Set<String> isrBrokers,
            long leaderEpoch
    ) {}

    private final ClusterRegistry registry;
    private final IsrTracker isrTracker;
    private final Map<String, AtomicLong> leaderEpochs = new ConcurrentHashMap<>();

    public MetadataService(ClusterRegistry registry, IsrTracker isrTracker) {
        this.registry = registry;
        this.isrTracker = isrTracker;
    }

    public long incrementLeaderEpoch(String topic, int partitionId) {
        String key = topic + ":" + partitionId;
        return leaderEpochs.computeIfAbsent(key, k -> new AtomicLong(0L)).incrementAndGet();
    }

    public long getLeaderEpoch(String topic, int partitionId) {
        String key = topic + ":" + partitionId;
        AtomicLong epoch = leaderEpochs.get(key);
        return epoch != null ? epoch.get() : 0L;
    }

    public PartitionMetadata getPartitionMetadata(String topicName, int partitionId) {
        var topic = registry.getTopic(topicName);
        if (topic == null) return null;
        var partition = topic.getPartition(partitionId);
        if (partition == null) return null;

        String leaderId = partition.getLeaderId();
        BrokerNode leaderNode = leaderId != null ? registry.getBroker(leaderId) : null;
        Set<String> isr = isrTracker.getIsr(topicName, partitionId);
        long epoch = getLeaderEpoch(topicName, partitionId);

        return new PartitionMetadata(
                topicName,
                partitionId,
                leaderId,
                leaderNode,
                partition.getReplicaBrokerIds(),
                isr,
                epoch
        );
    }
}
