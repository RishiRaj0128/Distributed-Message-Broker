package com.broker.replication;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.CommitLog;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages partition replication, follower log sync, write quorum verification,
 * and high-watermark progression across broker nodes.
 */
public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final ClusterRegistry registry;
    private final IsrTracker isrTracker;

    // Broker ID -> Topic -> Partition ID -> CommitLog (Broker's local replica storage)
    private final Map<String, Map<String, Map<Integer, CommitLog>>> brokerStorage = new ConcurrentHashMap<>();

    // Listeners for replication events (for metrics and failure testing)
    private final List<ReplicationListener> listeners = new CopyOnWriteArrayList<>();

    public interface ReplicationListener {
        void onMessageReplicated(String topic, int partitionId, String replicaId, long offset);
        void onHighWatermarkAdvanced(String topic, int partitionId, long newHw);
    }

    public ReplicationManager(ClusterRegistry registry, IsrTracker isrTracker) {
        this.registry = Objects.requireNonNull(registry);
        this.isrTracker = Objects.requireNonNull(isrTracker);
    }

    public void addListener(ReplicationListener listener) {
        listeners.add(listener);
    }

    public CommitLog getBrokerPartitionLog(String brokerId, String topic, int partitionId) {
        return brokerStorage
                .computeIfAbsent(brokerId, b -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(partitionId, p -> new CommitLog());
    }

    /**
     * Initializes partition replication across assigned replicas.
     */
    public void setupPartition(String topic, int partitionId, String leaderId, List<String> replicas) {
        for (String replicaId : replicas) {
            getBrokerPartitionLog(replicaId, topic, partitionId);
        }
        if (leaderId != null) {
            getBrokerPartitionLog(leaderId, topic, partitionId);
        }
        isrTracker.initPartition(topic, partitionId, leaderId, replicas);
    }

    /**
     * Writes a message to the partition leader and replicates to followers based on AckMode.
     *
     * @return The committed Message with assigned offset and high watermark.
     * @throws IllegalStateException if leader is down or quorum cannot be reached.
     */
    public Message appendAndReplicate(String topicName, int partitionId, Message message, AckMode ackMode) {
        Topic topic = registry.getTopic(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Partition does not exist: " + partitionId);
        }

        String leaderId = partition.getLeaderId();
        if (leaderId == null) {
            throw new IllegalStateException("No leader currently elected for " + topicName + "-p" + partitionId);
        }

        BrokerNode leaderNode = registry.getBroker(leaderId);
        if (leaderNode == null || !leaderNode.isAlive()) {
            throw new IllegalStateException("Leader node " + leaderId + " is not ALIVE");
        }

        // 1. Leader appends to its local commit log
        CommitLog leaderLog = getBrokerPartitionLog(leaderId, topicName, partitionId);
        Message leaderCommitted = leaderLog.append(message);
        long messageOffset = leaderCommitted.getOffset();

        // Also reflect into Partition representation
        partition.getCommitLog().append(leaderCommitted);

        long leaderLeo = leaderLog.getLogEndOffset();
        Set<String> currentIsr = isrTracker.evaluateIsr(topicName, partitionId, leaderId, leaderLeo);

        // 2. Handle LEADER_ONLY (ack=1)
        if (ackMode == AckMode.LEADER_ONLY) {
            long newHw = messageOffset + 1;
            partition.setHighWatermark(newHw);
            notifyHwAdvanced(topicName, partitionId, newHw);
            // Async replication to followers without blocking
            replicateToFollowersAsync(topicName, partitionId, leaderId, leaderCommitted, currentIsr);
            return leaderCommitted;
        }

        // 3. Replicate synchronously to available ISR followers
        int ackCount = 1; // Leader already persisted
        List<String> followersInIsr = currentIsr.stream()
                .filter(b -> !b.equals(leaderId))
                .toList();

        for (String followerId : followersInIsr) {
            BrokerNode followerNode = registry.getBroker(followerId);
            if (followerNode != null && followerNode.isAlive()) {
                CommitLog followerLog = getBrokerPartitionLog(followerId, topicName, partitionId);
                followerLog.append(leaderCommitted);
                isrTracker.recordReplicaFetch(topicName, partitionId, followerId, followerLog.getLogEndOffset());
                ackCount++;

                for (ReplicationListener listener : listeners) {
                    listener.onMessageReplicated(topicName, partitionId, followerId, messageOffset);
                }
            }
        }

        // Re-evaluate ISR after replication pass
        currentIsr = isrTracker.evaluateIsr(topicName, partitionId, leaderId, leaderLeo);

        // 4. Verify quorum satisfaction
        int isrSize = currentIsr.size();
        int requiredAcks = switch (ackMode) {
            case LEADER_ONLY -> 1;
            case QUORUM_ISR -> (isrSize / 2) + 1;
            case ALL_ISR -> isrSize;
        };

        if (ackCount < requiredAcks) {
            throw new IllegalStateException(String.format(
                    "Replication quorum failed for %s-p%d with AckMode=%s. Required %d acks, received %d (ISR size: %d)",
                    topicName, partitionId, ackMode, requiredAcks, ackCount, isrSize));
        }

        // Advance High Watermark (HW)
        long newHw = messageOffset + 1;
        partition.setHighWatermark(newHw);
        notifyHwAdvanced(topicName, partitionId, newHw);

        return leaderCommitted;
    }

    private void replicateToFollowersAsync(String topic, int partitionId, String leaderId, Message msg, Set<String> currentIsr) {
        // In local simulation, propagate to alive followers
        for (String followerId : currentIsr) {
            if (followerId.equals(leaderId)) continue;
            BrokerNode node = registry.getBroker(followerId);
            if (node != null && node.isAlive()) {
                CommitLog followerLog = getBrokerPartitionLog(followerId, topic, partitionId);
                followerLog.append(msg);
                isrTracker.recordReplicaFetch(topic, partitionId, followerId, followerLog.getLogEndOffset());
            }
        }
    }

    private void notifyHwAdvanced(String topic, int partitionId, long newHw) {
        for (ReplicationListener listener : listeners) {
            listener.onHighWatermarkAdvanced(topic, partitionId, newHw);
        }
    }

    public IsrTracker getIsrTracker() {
        return isrTracker;
    }
}
