package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Controller service that monitors node heartbeats, detects leader failures,
 * and orchestrates partition failover.
 */
public class ClusterController {
    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final ClusterRegistry registry;
    private final LeaderElector leaderElector;
    private final MetadataService metadataService;
    private final long heartbeatTimeoutMs;

    public ClusterController(ClusterRegistry registry,
                             LeaderElector leaderElector,
                             MetadataService metadataService,
                             long heartbeatTimeoutMs) {
        this.registry = Objects.requireNonNull(registry);
        this.leaderElector = Objects.requireNonNull(leaderElector);
        this.metadataService = Objects.requireNonNull(metadataService);
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public ClusterController(ClusterRegistry registry,
                             LeaderElector leaderElector,
                             MetadataService metadataService) {
        this(registry, leaderElector, metadataService, 1500L); // Default: 1.5s failure detection
    }

    /**
     * Checks heartbeats of all brokers and triggers election on partitions where the leader is dead.
     */
    public void scanAndFailoverDeadLeaders() {
        long now = System.currentTimeMillis();

        for (BrokerNode node : registry.getAllBrokers()) {
            if (node.isAlive() && (now - node.getLastHeartbeatTimestamp() > heartbeatTimeoutMs)) {
                log.warn("Broker {} heartbeat expired (silence={}ms > threshold={}ms). Marking DEAD.",
                        node.getId(), (now - node.getLastHeartbeatTimestamp()), heartbeatTimeoutMs);
                node.setStatus(BrokerNode.NodeStatus.DEAD);
            }
        }

        // Check all partitions
        for (Topic topic : registry.getAllTopics()) {
            for (Partition partition : topic.getPartitions().values()) {
                String leaderId = partition.getLeaderId();
                if (leaderId == null) {
                    leaderElector.electLeader(topic.getName(), partition.getPartitionId());
                } else {
                    BrokerNode leaderNode = registry.getBroker(leaderId);
                    if (leaderNode == null || !leaderNode.isAlive()) {
                        log.warn("Leader {} for {}-p{} is offline. Triggering election.",
                                leaderId, topic.getName(), partition.getPartitionId());
                        leaderElector.electLeader(topic.getName(), partition.getPartitionId());
                    }
                }
            }
        }
    }

    /**
     * Explicitly marks a broker as failed (e.g. killed process simulation)
     * and performs immediate synchronous failover.
     */
    public void simulateNodeFailure(String brokerId) {
        BrokerNode node = registry.getBroker(brokerId);
        if (node != null) {
            node.setStatus(BrokerNode.NodeStatus.DEAD);
            log.info("Simulated hard crash for broker node: {}", brokerId);
        }
        scanAndFailoverDeadLeaders();
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public LeaderElector getLeaderElector() {
        return leaderElector;
    }
}
