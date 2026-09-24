package com.broker.topology;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;

import java.util.*;

/**
 * Validates and enforces Multi-DC / Multi-AZ fault tolerance invariants.
 * Ensures no partition has all replicas co-located in the same failure domain.
 */
public class MultiDcPlacementEngine {

    public record PlacementAuditResult(
            boolean isCompliant,
            Map<String, Set<String>> partitionZoneMap,
            List<String> violations
    ) {}

    private final ClusterRegistry registry;

    public MultiDcPlacementEngine(ClusterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
    }

    /**
     * Audits the live cluster to verify multi-zone placement invariants.
     *
     * @param minDistinctZonesPerPartition Minimum distinct zones required for each partition
     */
    public PlacementAuditResult auditClusterPlacement(int minDistinctZonesPerPartition) {
        Map<String, Set<String>> partitionZoneMap = new LinkedHashMap<>();
        List<String> violations = new ArrayList<>();

        for (Topic topic : registry.getAllTopics()) {
            if (topic.getReplicationFactor() < minDistinctZonesPerPartition) {
                // Topic replication factor cannot satisfy minDistinctZonesPerPartition
                continue;
            }

            for (Partition partition : topic.getPartitions().values()) {
                String partitionKey = topic.getName() + "-p" + partition.getPartitionId();
                Set<String> zones = new HashSet<>();

                for (String brokerId : partition.getReplicaBrokerIds()) {
                    BrokerNode node = registry.getBroker(brokerId);
                    if (node != null) {
                        zones.add(node.getZone());
                    }
                }

                partitionZoneMap.put(partitionKey, zones);

                if (zones.size() < minDistinctZonesPerPartition) {
                    violations.add(String.format(
                            "Partition %s has replicas only in %d zone(s) %s; violates minDistinctZones=%d requirement",
                            partitionKey, zones.size(), zones, minDistinctZonesPerPartition));
                }
            }
        }

        boolean compliant = violations.isEmpty();
        return new PlacementAuditResult(compliant, partitionZoneMap, violations);
    }

    /**
     * Simulates catastrophic total failure of an entire Datacenter Zone
     * and evaluates partition survivability.
     */
    public Map<String, Boolean> simulateZoneOutage(String failedZone) {
        Map<String, Boolean> partitionSurvivability = new LinkedHashMap<>();

        for (Topic topic : registry.getAllTopics()) {
            for (Partition partition : topic.getPartitions().values()) {
                String partitionKey = topic.getName() + "-p" + partition.getPartitionId();
                boolean hasSurvivingReplica = false;

                for (String brokerId : partition.getReplicaBrokerIds()) {
                    BrokerNode node = registry.getBroker(brokerId);
                    if (node != null && !node.getZone().equalsIgnoreCase(failedZone)) {
                        hasSurvivingReplica = true;
                        break;
                    }
                }

                partitionSurvivability.put(partitionKey, hasSurvivingReplica);
            }
        }

        return partitionSurvivability;
    }
}
