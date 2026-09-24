package com.broker.core;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Topic representing a logical channel split across multiple partitions.
 */
public class Topic {
    private final String name;
    private final int partitionCount;
    private final int replicationFactor;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    public Topic(String name, int partitionCount, int replicationFactor) {
        this.name = name;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
    }

    public String getName() { return name; }
    public int getPartitionCount() { return partitionCount; }
    public int getReplicationFactor() { return replicationFactor; }

    public void addPartition(Partition partition) {
        partitions.put(partition.getPartitionId(), partition);
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    public Map<Integer, Partition> getPartitions() {
        return Collections.unmodifiableMap(partitions);
    }

    /**
     * Standard Murmur2 / Hash partitioner based on message key.
     */
    public int selectPartitionForKey(String key) {
        if (key == null || partitionCount <= 1) {
            return 0;
        }
        int hash = key.hashCode();
        return Math.abs(hash) % partitionCount;
    }

    @Override
    public String toString() {
        return "Topic{" +
                "name='" + name + '\'' +
                ", partitions=" + partitionCount +
                ", replicationFactor=" + replicationFactor +
                '}';
    }
}
