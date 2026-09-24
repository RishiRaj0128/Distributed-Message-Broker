package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes partition leader election during node failure or failover.
 *
 * Election Rule:
 * 1. Only brokers currently in the In-Sync Replica (ISR) set are eligible for election.
 * 2. Among eligible ISR candidates, the broker with the highest replicated log offset (LEO) is elected.
 * 3. Ties are deterministically broken by broker ID.
 * 4. This guarantees that un-replicated or lagging followers never overwrite committed data.
 *
 * Simplification note: In production Apache Kafka (KRaft / ZooKeeper), leader elections are either
 * coordinated by the active cluster Controller using metadata records or via Raft quorum votes.
 * Here, LeaderElector runs a centralized atomic election state machine with identical ISR/LEO criteria.
 */
public class LeaderElector {
    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);

    private final ClusterRegistry registry;
    private final IsrTracker isrTracker;
    private final ReplicationManager replicationManager;
    private final MetadataService metadataService;
    private final ReentrantLock electionLock = new ReentrantLock();

    public interface ElectionListener {
        void onLeaderElected(String topic, int partitionId, String previousLeader, String newLeader, long epoch);
    }

    private final List<ElectionListener> listeners = new CopyOnWriteArrayList<>();

    public LeaderElector(ClusterRegistry registry,
                         IsrTracker isrTracker,
                         ReplicationManager replicationManager,
                         MetadataService metadataService) {
        this.registry = registry;
        this.isrTracker = isrTracker;
        this.replicationManager = replicationManager;
        this.metadataService = metadataService;
    }

    public void addListener(ElectionListener listener) {
        listeners.add(listener);
    }

    /**
     * Triggers leader election for a specified partition.
     *
     * @return The newly elected leader broker ID, or null if no eligible ISR candidate is available.
     */
    public String electLeader(String topicName, int partitionId) {
        electionLock.lock();
        try {
            Topic topic = registry.getTopic(topicName);
            if (topic == null) {
                throw new IllegalArgumentException("Unknown topic: " + topicName);
            }
            Partition partition = topic.getPartition(partitionId);
            if (partition == null) {
                throw new IllegalArgumentException("Unknown partition: " + partitionId);
            }

            String currentLeader = partition.getLeaderId();
            Set<String> isr = isrTracker.getIsr(topicName, partitionId);

            log.info("Initiating leader election for {}-p{}. Current leader: {}, Current ISR: {}",
                    topicName, partitionId, currentLeader, isr);

            // Filter for alive ISR members excluding dead leader
            List<String> eligibleCandidates = isr.stream()
                    .filter(brokerId -> !brokerId.equals(currentLeader))
                    .filter(brokerId -> {
                        BrokerNode node = registry.getBroker(brokerId);
                        return node != null && node.isAlive();
                    })
                    .toList();

            if (eligibleCandidates.isEmpty()) {
                log.error("CRITICAL: No alive ISR candidates found for {}-p{}. Partition offline to prevent data loss!",
                        topicName, partitionId);
                partition.setLeaderId(null);
                return null;
            }

            // Find candidate with highest replicated log offset (LEO)
            String bestCandidate = null;
            long highestOffset = -1L;

            for (String candidateId : eligibleCandidates) {
                var candidateLog = replicationManager.getBrokerPartitionLog(candidateId, topicName, partitionId);
                long leo = candidateLog.getLogEndOffset();
                if (leo > highestOffset) {
                    highestOffset = leo;
                    bestCandidate = candidateId;
                } else if (leo == highestOffset && bestCandidate != null) {
                    // Tie-breaker: deterministic alphabetical comparison
                    if (candidateId.compareTo(bestCandidate) < 0) {
                        bestCandidate = candidateId;
                    }
                }
            }

            // Promote candidate
            long newEpoch = metadataService.incrementLeaderEpoch(topicName, partitionId);
            partition.setLeaderId(bestCandidate);

            // Synchronize partition commit log with the new leader's log
            var newLeaderLog = replicationManager.getBrokerPartitionLog(bestCandidate, topicName, partitionId);
            long hw = newLeaderLog.getLogEndOffset();
            partition.setHighWatermark(hw);

            // Evaluate ISR with new leader
            isrTracker.evaluateIsr(topicName, partitionId, bestCandidate, hw);

            log.info("SUCCESS: Elected new leader '{}' for {}-p{} at epoch {} (replicated offset={})",
                    bestCandidate, topicName, partitionId, newEpoch, highestOffset);

            for (ElectionListener listener : listeners) {
                listener.onLeaderElected(topicName, partitionId, currentLeader, bestCandidate, newEpoch);
            }

            return bestCandidate;
        } finally {
            electionLock.unlock();
        }
    }
}
