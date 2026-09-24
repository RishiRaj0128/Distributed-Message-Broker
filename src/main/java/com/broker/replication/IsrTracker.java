package com.broker.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Tracks follower replication progress, calculates In-Sync Replica (ISR) membership,
 * and maintains partition high watermarks.
 */
public class IsrTracker {
    private static final Logger log = LoggerFactory.getLogger(IsrTracker.class);

    public record PartitionKey(String topic, int partitionId) {}

    public static class ReplicaState {
        private final String brokerId;
        private volatile long offset;
        private volatile long lastFetchTimestamp;

        public ReplicaState(String brokerId, long initialOffset) {
            this.brokerId = brokerId;
            this.offset = initialOffset;
            this.lastFetchTimestamp = System.currentTimeMillis();
        }

        public String getBrokerId() { return brokerId; }
        public long getOffset() { return offset; }
        public void setOffset(long offset) {
            this.offset = offset;
            this.lastFetchTimestamp = System.currentTimeMillis();
        }
        public long getLastFetchTimestamp() { return lastFetchTimestamp; }
    }

    private final long maxLagOffsets;
    private final long maxLagMs;
    private final Map<PartitionKey, Map<String, ReplicaState>> replicaStates = new ConcurrentHashMap<>();
    private final Map<PartitionKey, Set<String>> isrMembers = new ConcurrentHashMap<>();

    public IsrTracker(long maxLagOffsets, long maxLagMs) {
        this.maxLagOffsets = maxLagOffsets;
        this.maxLagMs = maxLagMs;
    }

    public IsrTracker() {
        this(5L, 2000L); // Default: 5 offsets lag or 2 seconds silence
    }

    public void initPartition(String topic, int partitionId, String leaderId, List<String> allReplicas) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<String, ReplicaState> states = replicaStates.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        Set<String> isr = isrMembers.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>());

        for (String replicaId : allReplicas) {
            states.putIfAbsent(replicaId, new ReplicaState(replicaId, 0L));
            isr.add(replicaId);
        }
        if (leaderId != null) {
            states.putIfAbsent(leaderId, new ReplicaState(leaderId, 0L));
            isr.add(leaderId);
        }
    }

    public void recordReplicaFetch(String topic, int partitionId, String replicaId, long replicatedOffset) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<String, ReplicaState> states = replicaStates.get(key);
        if (states != null) {
            ReplicaState state = states.computeIfAbsent(replicaId, id -> new ReplicaState(id, replicatedOffset));
            state.setOffset(replicatedOffset);
        }
    }

    /**
     * Evaluates ISR membership based on current leader LEO and heartbeat freshness.
     * Follower is expelled if lag > maxLagOffsets OR elapsed time since fetch > maxLagMs.
     */
    public synchronized Set<String> evaluateIsr(String topic, int partitionId, String leaderId, long leaderLeo) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<String, ReplicaState> states = replicaStates.get(key);
        Set<String> isr = isrMembers.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>());

        if (states == null) {
            return Collections.emptySet();
        }

        long now = System.currentTimeMillis();
        // Leader is always in ISR if defined
        if (leaderId != null) {
            isr.add(leaderId);
            ReplicaState leaderState = states.get(leaderId);
            if (leaderState != null) {
                leaderState.setOffset(leaderLeo);
            }
        }

        for (Map.Entry<String, ReplicaState> entry : states.entrySet()) {
            String brokerId = entry.getKey();
            if (brokerId.equals(leaderId)) continue;

            ReplicaState state = entry.getValue();
            long offsetLag = Math.max(0L, leaderLeo - state.getOffset());
            long timeLag = now - state.getLastFetchTimestamp();

            boolean isHealthy = (offsetLag <= maxLagOffsets) && (timeLag <= maxLagMs);

            if (isHealthy) {
                if (!isr.contains(brokerId)) {
                    log.info("Replica {} caught up on {}-p{}. Re-adding to ISR.", brokerId, topic, partitionId);
                    isr.add(brokerId);
                }
            } else {
                if (isr.contains(brokerId)) {
                    log.warn("Replica {} lagged out on {}-p{} (offsetLag={}, timeLag={}ms). Expelling from ISR.",
                            brokerId, topic, partitionId, offsetLag, timeLag);
                    isr.remove(brokerId);
                }
            }
        }

        return Collections.unmodifiableSet(new HashSet<>(isr));
    }

    public Set<String> getIsr(String topic, int partitionId) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Set<String> isr = isrMembers.get(key);
        return isr != null ? Collections.unmodifiableSet(new HashSet<>(isr)) : Collections.emptySet();
    }

    public long getReplicaOffset(String topic, int partitionId, String replicaId) {
        PartitionKey key = new PartitionKey(topic, partitionId);
        Map<String, ReplicaState> states = replicaStates.get(key);
        if (states != null) {
            ReplicaState state = states.get(replicaId);
            if (state != null) {
                return state.getOffset();
            }
        }
        return -1L;
    }

    public boolean isUnderReplicated(String topic, int partitionId, int configuredReplicationFactor) {
        Set<String> isr = getIsr(topic, partitionId);
        return isr.size() < configuredReplicationFactor;
    }

    public void removeBrokerFromAllIsr(String brokerId) {
        for (Map.Entry<PartitionKey, Set<String>> entry : isrMembers.entrySet()) {
            entry.getValue().remove(brokerId);
        }
    }
}
