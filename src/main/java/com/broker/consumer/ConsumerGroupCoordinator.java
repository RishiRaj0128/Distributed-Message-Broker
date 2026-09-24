package com.broker.consumer;

import com.broker.core.ClusterRegistry;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates consumer group membership, failure detection, and partition rebalancing.
 */
public class ConsumerGroupCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupCoordinator.class);

    public record MemberInfo(
            String memberId,
            String groupId,
            Set<String> subscribedTopics,
            long lastHeartbeatTime
    ) {}

    public interface RebalanceCallback {
        void onPartitionsRevoked(String groupId, String memberId, List<PartitionAssignor.TopicPartition> revoked);
        void onPartitionsAssigned(String groupId, String memberId, List<PartitionAssignor.TopicPartition> assigned);
    }

    private final ClusterRegistry registry;
    private final long sessionTimeoutMs;
    private final ReentrantLock coordinatorLock = new ReentrantLock();

    // Group ID -> (Member ID -> MemberInfo)
    private final Map<String, Map<String, MemberInfo>> groupMembers = new ConcurrentHashMap<>();
    // Group ID -> (Member ID -> Assigned Partitions)
    private final Map<String, Map<String, List<PartitionAssignor.TopicPartition>>> groupAssignments = new ConcurrentHashMap<>();
    private final List<RebalanceCallback> callbacks = new CopyOnWriteArrayList<>();

    public ConsumerGroupCoordinator(ClusterRegistry registry, long sessionTimeoutMs) {
        this.registry = Objects.requireNonNull(registry);
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public ConsumerGroupCoordinator(ClusterRegistry registry) {
        this(registry, 3000L); // 3 seconds session timeout
    }

    public void addCallback(RebalanceCallback callback) {
        callbacks.add(callback);
    }

    public void joinGroup(String groupId, String memberId, Set<String> subscribedTopics) {
        coordinatorLock.lock();
        try {
            Map<String, MemberInfo> members = groupMembers.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());
            members.put(memberId, new MemberInfo(memberId, groupId, Set.copyOf(subscribedTopics), System.currentTimeMillis()));
            log.info("Consumer {} joined group {}. Triggering rebalance.", memberId, groupId);
            rebalanceGroup(groupId);
        } finally {
            coordinatorLock.unlock();
        }
    }

    public void leaveGroup(String groupId, String memberId) {
        coordinatorLock.lock();
        try {
            Map<String, MemberInfo> members = groupMembers.get(groupId);
            if (members != null && members.remove(memberId) != null) {
                log.info("Consumer {} left group {}. Triggering rebalance.", memberId, groupId);
                Map<String, List<PartitionAssignor.TopicPartition>> currentAssigns = groupAssignments.get(groupId);
                if (currentAssigns != null) {
                    List<PartitionAssignor.TopicPartition> revoked = currentAssigns.remove(memberId);
                    if (revoked != null) {
                        notifyRevoked(groupId, memberId, revoked);
                    }
                }
                rebalanceGroup(groupId);
            }
        } finally {
            coordinatorLock.unlock();
        }
    }

    public void heartbeat(String groupId, String memberId) {
        Map<String, MemberInfo> members = groupMembers.get(groupId);
        if (members != null) {
            MemberInfo info = members.get(memberId);
            if (info != null) {
                members.put(memberId, new MemberInfo(memberId, groupId, info.subscribedTopics(), System.currentTimeMillis()));
            }
        }
    }

    public void checkAndEvictDeadMembers() {
        coordinatorLock.lock();
        try {
            long now = System.currentTimeMillis();
            for (String groupId : groupMembers.keySet()) {
                Map<String, MemberInfo> members = groupMembers.get(groupId);
                List<String> deadMembers = new ArrayList<>();
                for (MemberInfo m : members.values()) {
                    if (now - m.lastHeartbeatTime() > sessionTimeoutMs) {
                        deadMembers.add(m.memberId());
                    }
                }
                if (!deadMembers.isEmpty()) {
                    for (String deadId : deadMembers) {
                        log.warn("Evicting dead consumer {} from group {} due to heartbeat timeout", deadId, groupId);
                        members.remove(deadId);
                    }
                    rebalanceGroup(groupId);
                }
            }
        } finally {
            coordinatorLock.unlock();
        }
    }

    public synchronized void rebalanceGroup(String groupId) {
        Map<String, MemberInfo> members = groupMembers.get(groupId);
        if (members == null || members.isEmpty()) {
            groupAssignments.remove(groupId);
            return;
        }

        // Collect all distinct topics subscribed by members of this group
        Set<String> topics = new HashSet<>();
        for (MemberInfo m : members.values()) {
            topics.addAll(m.subscribedTopics());
        }

        // Gather all partition references
        List<PartitionAssignor.TopicPartition> allPartitions = new ArrayList<>();
        for (String topicName : topics) {
            Topic topic = registry.getTopic(topicName);
            if (topic != null) {
                for (int pId : topic.getPartitions().keySet()) {
                    allPartitions.add(new PartitionAssignor.TopicPartition(topicName, pId));
                }
            }
        }

        List<String> memberIds = new ArrayList<>(members.keySet());
        Map<String, List<PartitionAssignor.TopicPartition>> newAssignments =
                PartitionAssignor.assignRoundRobin(memberIds, allPartitions);

        Map<String, List<PartitionAssignor.TopicPartition>> oldAssignments =
                groupAssignments.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>());

        // Notify revocations
        for (Map.Entry<String, List<PartitionAssignor.TopicPartition>> entry : oldAssignments.entrySet()) {
            notifyRevoked(groupId, entry.getKey(), entry.getValue());
        }

        // Update with new assignments
        oldAssignments.clear();
        oldAssignments.putAll(newAssignments);

        // Notify assignments
        for (Map.Entry<String, List<PartitionAssignor.TopicPartition>> entry : newAssignments.entrySet()) {
            notifyAssigned(groupId, entry.getKey(), entry.getValue());
        }

        log.info("Rebalanced group {}. Assignments: {}", groupId, newAssignments);
    }

    public List<PartitionAssignor.TopicPartition> getAssignment(String groupId, String memberId) {
        Map<String, List<PartitionAssignor.TopicPartition>> assigns = groupAssignments.get(groupId);
        if (assigns != null) {
            List<PartitionAssignor.TopicPartition> list = assigns.get(memberId);
            if (list != null) return Collections.unmodifiableList(list);
        }
        return Collections.emptyList();
    }

    private void notifyRevoked(String groupId, String memberId, List<PartitionAssignor.TopicPartition> partitions) {
        for (RebalanceCallback cb : callbacks) {
            cb.onPartitionsRevoked(groupId, memberId, partitions);
        }
    }

    private void notifyAssigned(String groupId, String memberId, List<PartitionAssignor.TopicPartition> partitions) {
        for (RebalanceCallback cb : callbacks) {
            cb.onPartitionsAssigned(groupId, memberId, partitions);
        }
    }
}
