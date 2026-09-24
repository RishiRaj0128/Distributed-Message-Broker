package com.broker.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An individual partition of a Topic, containing its commit log, leader, and replicas.
 */
public class Partition {
    private final String topic;
    private final int partitionId;
    private final CommitLog commitLog;
    private volatile String leaderId;
    private final List<String> replicaBrokerIds;
    private final AtomicLong highWatermark;

    public Partition(String topic, int partitionId, String leaderId, List<String> replicaBrokerIds) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.commitLog = new CommitLog();
        this.leaderId = leaderId;
        this.replicaBrokerIds = new CopyOnWriteArrayList<>(replicaBrokerIds != null ? replicaBrokerIds : Collections.emptyList());
        this.highWatermark = new AtomicLong(0L);
    }

    public String getTopic() { return topic; }
    public int getPartitionId() { return partitionId; }
    public CommitLog getCommitLog() { return commitLog; }
    public String getLeaderId() { return leaderId; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public List<String> getReplicaBrokerIds() { return Collections.unmodifiableList(replicaBrokerIds); }

    public void addReplica(String brokerId) {
        if (!replicaBrokerIds.contains(brokerId)) {
            replicaBrokerIds.add(brokerId);
        }
    }

    public void removeReplica(String brokerId) {
        replicaBrokerIds.remove(brokerId);
    }

    public long getHighWatermark() {
        return highWatermark.get();
    }

    public void setHighWatermark(long hw) {
        this.highWatermark.set(hw);
    }

    public Message append(Message message) {
        return commitLog.append(message);
    }

    public List<Message> readCommitted(long startOffset, int maxCount) {
        long currentHw = highWatermark.get();
        if (startOffset >= currentHw) {
            return Collections.emptyList();
        }
        int fetchCount = (int) Math.min(maxCount, currentHw - startOffset);
        return commitLog.read(startOffset, fetchCount);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "topic='" + topic + '\'' +
                ", partitionId=" + partitionId +
                ", leaderId='" + leaderId + '\'' +
                ", replicas=" + replicaBrokerIds +
                ", hw=" + highWatermark.get() +
                ", leo=" + commitLog.getLogEndOffset() +
                '}';
    }
}
