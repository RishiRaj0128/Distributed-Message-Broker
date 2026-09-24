package com.broker.dsl;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Reconciler that continuously or on-demand aligns actual broker cluster state
 * with declared infrastructure DSL specifications (similar to Terraform or Kubernetes controllers).
 */
public class TopologyReconciler {
    private static final Logger log = LoggerFactory.getLogger(TopologyReconciler.class);

    private final ClusterRegistry registry;
    private final ReplicationManager replicationManager;
    private final IsrTracker isrTracker;
    private final ZoneAwarePlacementStrategy placementStrategy;
    private final ObjectMapper yamlMapper;

    public record ReconciliationReport(
            int brokersRegistered,
            int topicsCreated,
            int partitionsCreated,
            List<String> underReplicatedPartitions,
            List<String> appliedActions,
            boolean isConverged
    ) {}

    public TopologyReconciler(ClusterRegistry registry,
                              ReplicationManager replicationManager,
                              IsrTracker isrTracker,
                              ZoneAwarePlacementStrategy placementStrategy) {
        this.registry = registry;
        this.replicationManager = replicationManager;
        this.isrTracker = isrTracker;
        this.placementStrategy = placementStrategy;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    public TopologyReconciler(ClusterRegistry registry, ReplicationManager replicationManager, IsrTracker isrTracker) {
        this(registry, replicationManager, isrTracker, new ZoneAwarePlacementStrategy());
    }

    public ClusterTopologySpec parseSpec(String yamlContent) throws IOException {
        return yamlMapper.readValue(yamlContent, ClusterTopologySpec.class);
    }

    public ClusterTopologySpec parseSpec(File yamlFile) throws IOException {
        return yamlMapper.readValue(yamlFile, ClusterTopologySpec.class);
    }

    /**
     * Executes reconciliation loop: Declared State -> Actual Live State.
     */
    public synchronized ReconciliationReport reconcile(ClusterTopologySpec spec) {
        List<String> actions = new ArrayList<>();
        int brokersAdded = 0;
        int topicsCreated = 0;
        int partitionsAdded = 0;

        // 1. Reconcile Brokers
        for (ClusterTopologySpec.BrokerSpec bSpec : spec.getBrokers()) {
            BrokerNode existing = registry.getBroker(bSpec.getId());
            if (existing == null) {
                BrokerNode node = new BrokerNode(bSpec.getId(), bSpec.getHost(), bSpec.getPort(), bSpec.getZone());
                registry.registerBroker(node);
                actions.add("Registered broker node: " + bSpec.getId() + " in zone " + bSpec.getZone());
                brokersAdded++;
            }
        }

        List<BrokerNode> availableBrokers = new ArrayList<>(registry.getAllBrokers());

        // 2. Reconcile Topics and Partitions
        for (ClusterTopologySpec.TopicSpec tSpec : spec.getTopics()) {
            Topic liveTopic = registry.getTopic(tSpec.getName());
            int minZones = tSpec.getPlacementPolicy() != null ? tSpec.getPlacementPolicy().getMinDistinctZones() : 1;

            if (liveTopic == null) {
                liveTopic = new Topic(tSpec.getName(), tSpec.getPartitions(), tSpec.getReplicationFactor());
                registry.registerTopic(liveTopic);
                actions.add("Created topic: " + tSpec.getName() + " (partitions=" + tSpec.getPartitions()
                        + ", rf=" + tSpec.getReplicationFactor() + ")");
                topicsCreated++;

                // Create initial partitions
                for (int pId = 0; pId < tSpec.getPartitions(); pId++) {
                    var placement = placementStrategy.placePartition(
                            availableBrokers, pId, tSpec.getReplicationFactor(), minZones);
                    Partition p = new Partition(tSpec.getName(), pId, placement.leaderId(), placement.replicas());
                    liveTopic.addPartition(p);
                    replicationManager.setupPartition(tSpec.getName(), pId, placement.leaderId(), placement.replicas());
                    partitionsAdded++;
                    actions.add(String.format("Created partition %s-p%d with leader=%s, replicas=%s, zones=%s",
                            tSpec.getName(), pId, placement.leaderId(), placement.replicas(), placement.coveredZones()));
                }
            } else {
                // Topic exists, verify partition count expansion
                if (tSpec.getPartitions() > liveTopic.getPartitions().size()) {
                    int existingPartitions = liveTopic.getPartitions().size();
                    for (int pId = existingPartitions; pId < tSpec.getPartitions(); pId++) {
                        var placement = placementStrategy.placePartition(
                                availableBrokers, pId, tSpec.getReplicationFactor(), minZones);
                        Partition p = new Partition(tSpec.getName(), pId, placement.leaderId(), placement.replicas());
                        liveTopic.addPartition(p);
                        replicationManager.setupPartition(tSpec.getName(), pId, placement.leaderId(), placement.replicas());
                        partitionsAdded++;
                        actions.add(String.format("Expanded partition %s-p%d with leader=%s, replicas=%s",
                                tSpec.getName(), pId, placement.leaderId(), placement.replicas()));
                    }
                }
            }
        }

        // 3. Scan for Under-Replicated Partitions
        List<String> underReplicated = new ArrayList<>();
        for (Topic t : registry.getAllTopics()) {
            for (Partition p : t.getPartitions().values()) {
                if (isrTracker.isUnderReplicated(t.getName(), p.getPartitionId(), t.getReplicationFactor())) {
                    underReplicated.add(t.getName() + "-p" + p.getPartitionId());
                }
            }
        }

        boolean converged = underReplicated.isEmpty();
        log.info("Reconciliation complete. Actions: {}, UnderReplicated: {}, Converged: {}",
                actions.size(), underReplicated.size(), converged);

        return new ReconciliationReport(brokersAdded, topicsCreated, partitionsAdded, underReplicated, actions, converged);
    }
}
