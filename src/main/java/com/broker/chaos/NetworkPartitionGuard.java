package com.broker.chaos;

import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Enforces strict majority quorum guards to eliminate Split-Brain scenarios
 * during asymmetric network partitions.
 */
public class NetworkPartitionGuard {
    private static final Logger log = LoggerFactory.getLogger(NetworkPartitionGuard.class);

    private final ClusterRegistry registry;
    private final ChaosInjector chaosInjector;

    public NetworkPartitionGuard(ClusterRegistry registry, ChaosInjector chaosInjector) {
        this.registry = Objects.requireNonNull(registry);
        this.chaosInjector = Objects.requireNonNull(chaosInjector);
    }

    /**
     * Verifies that the specified leader maintains quorum connectivity to its partition replicas.
     * If the leader is isolated in a network partition minority, it must step down immediately.
     */
    public boolean verifyLeaderQuorum(String topicName, int partitionId) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) return false;
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) return false;

        String leaderId = partition.getLeaderId();
        if (leaderId == null) return false;

        List<String> replicas = partition.getReplicaBrokerIds();
        int reachableCount = 0;
        int requiredQuorum = (replicas.size() / 2) + 1;

        for (String replicaId : replicas) {
            if (chaosInjector.canCommunicate(leaderId, replicaId)) {
                reachableCount++;
            }
        }

        if (reachableCount < requiredQuorum) {
            log.error("SPLIT-BRAIN GUARD: Leader {} for {}-p{} has lost quorum (reachable {} < required {}). Stepping down!",
                    leaderId, topicName, partitionId, reachableCount, requiredQuorum);
            partition.setLeaderId(null);
            return false;
        }

        return true;
    }
}
