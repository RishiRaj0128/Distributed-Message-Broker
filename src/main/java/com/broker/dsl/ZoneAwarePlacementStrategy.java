package com.broker.dsl;

import com.broker.core.BrokerNode;

import java.util.*;

/**
 * Computes broker-to-partition replica assignments enforcing multi-DC zone spread rules.
 */
public class ZoneAwarePlacementStrategy {

    public record PlacementDecision(String leaderId, List<String> replicas, Set<String> coveredZones) {}

    /**
     * Selects replicas for a partition satisfying minDistinctZones.
     */
    public PlacementDecision placePartition(List<BrokerNode> availableBrokers, int partitionId,
                                            int replicationFactor, int minDistinctZones) {
        if (availableBrokers.isEmpty()) {
            throw new IllegalStateException("No available brokers for partition placement");
        }
        if (replicationFactor > availableBrokers.size()) {
            throw new IllegalArgumentException(String.format(
                    "Replication factor %d exceeds available broker count %d",
                    replicationFactor, availableBrokers.size()));
        }

        // Group brokers by zone
        Map<String, List<BrokerNode>> brokersByZone = new LinkedHashMap<>();
        for (BrokerNode b : availableBrokers) {
            brokersByZone.computeIfAbsent(b.getZone(), z -> new ArrayList<>()).add(b);
        }

        if (brokersByZone.size() < minDistinctZones) {
            throw new IllegalStateException(String.format(
                    "Cluster has %d zones (%s), which cannot satisfy minDistinctZones=%d requirement",
                    brokersByZone.size(), brokersByZone.keySet(), minDistinctZones));
        }

        List<String> chosenReplicas = new ArrayList<>();
        Set<String> chosenZones = new HashSet<>();

        // 1. Pick 1 broker from each distinct zone first until minDistinctZones is satisfied
        List<String> zoneKeys = new ArrayList<>(brokersByZone.keySet());
        // Rotate start zone by partitionId to distribute leaders evenly across zones
        int startZoneIdx = partitionId % zoneKeys.size();

        for (int i = 0; i < zoneKeys.size() && chosenReplicas.size() < replicationFactor; i++) {
            String zone = zoneKeys.get((startZoneIdx + i) % zoneKeys.size());
            List<BrokerNode> zoneNodes = brokersByZone.get(zone);
            // Select node in zone using partitionId offset
            BrokerNode picked = zoneNodes.get(partitionId % zoneNodes.size());
            if (!chosenReplicas.contains(picked.getId())) {
                chosenReplicas.add(picked.getId());
                chosenZones.add(zone);
            }
        }

        // 2. If replicationFactor > distinct zones, fill remaining replicas from remaining available brokers
        for (BrokerNode b : availableBrokers) {
            if (chosenReplicas.size() >= replicationFactor) break;
            if (!chosenReplicas.contains(b.getId())) {
                chosenReplicas.add(b.getId());
                chosenZones.add(b.getZone());
            }
        }

        // First chosen replica is designated as leader
        String leaderId = chosenReplicas.get(0);
        return new PlacementDecision(leaderId, Collections.unmodifiableList(chosenReplicas), Collections.unmodifiableSet(chosenZones));
    }
}
