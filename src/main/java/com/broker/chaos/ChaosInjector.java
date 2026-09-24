package com.broker.chaos;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Chaos engineering fault injector for distributed network partitions,
 * broker crashes, and latency degradation.
 */
public class ChaosInjector {
    private static final Logger log = LoggerFactory.getLogger(ChaosInjector.class);

    private final ClusterRegistry registry;

    // Disallowed broker communication pairs (simulated network partition boundary)
    private final Set<String> partitionedPairs = ConcurrentHashMap.newKeySet();

    public ChaosInjector(ClusterRegistry registry) {
        this.registry = Objects.requireNonNull(registry);
    }

    private String pairKey(String b1, String b2) {
        return b1.compareTo(b2) < 0 ? b1 + "<->" + b2 : b2 + "<->" + b1;
    }

    /**
     * Creates an artificial network partition between two sets of brokers.
     * Members of sideA cannot communicate with members of sideB.
     */
    public void createNetworkPartition(Set<String> sideA, Set<String> sideB) {
        for (String a : sideA) {
            for (String b : sideB) {
                partitionedPairs.add(pairKey(a, b));
            }
        }
        log.warn("CHAOS: Network partition injected between sideA={} and sideB={}", sideA, sideB);
    }

    /**
     * Heals all network partitions.
     */
    public void healNetworkPartition() {
        partitionedPairs.clear();
        log.info("CHAOS: Network partition healed. Full connectivity restored.");
    }

    public boolean canCommunicate(String b1, String b2) {
        if (b1.equals(b2)) return true;
        BrokerNode node1 = registry.getBroker(b1);
        BrokerNode node2 = registry.getBroker(b2);
        if (node1 == null || !node1.isAlive() || node2 == null || !node2.isAlive()) {
            return false;
        }
        return !partitionedPairs.contains(pairKey(b1, b2));
    }

    /**
     * Checks if a candidate broker can reach a strict quorum (majority) of the cluster.
     */
    public boolean hasQuorumReachability(String candidateBrokerId, Collection<String> allClusterBrokers) {
        int reachableCount = 0;
        int totalNodes = allClusterBrokers.size();
        int majorityThreshold = (totalNodes / 2) + 1;

        for (String other : allClusterBrokers) {
            if (canCommunicate(candidateBrokerId, other)) {
                reachableCount++;
            }
        }

        boolean hasQuorum = reachableCount >= majorityThreshold;
        log.info("Broker {} quorum check: reachable={}/{}, majorityThreshold={}, quorumReached={}",
                candidateBrokerId, reachableCount, totalNodes, majorityThreshold, hasQuorum);
        return hasQuorum;
    }
}
