package com.broker.consumer;

import java.util.*;

/**
 * Deterministic partition assignor for consumer groups.
 * Ensures partitions are distributed evenly across group members
 * with the strict invariant that no two consumers in the same group read the same partition.
 */
public class PartitionAssignor {

    public record TopicPartition(String topic, int partitionId) implements Comparable<TopicPartition> {
        @Override
        public int compareTo(TopicPartition o) {
            int c = topic.compareTo(o.topic);
            return c != 0 ? c : Integer.compare(partitionId, o.partitionId);
        }
    }

    /**
     * Round-robin partition assignment across sorted member IDs.
     */
    public static Map<String, List<TopicPartition>> assignRoundRobin(
            List<String> memberIds,
            List<TopicPartition> partitions) {

        Map<String, List<TopicPartition>> assignment = new TreeMap<>();
        if (memberIds.isEmpty()) {
            return assignment;
        }

        List<String> sortedMembers = new ArrayList<>(memberIds);
        Collections.sort(sortedMembers);

        List<TopicPartition> sortedPartitions = new ArrayList<>(partitions);
        Collections.sort(sortedPartitions);

        for (String m : sortedMembers) {
            assignment.put(m, new ArrayList<>());
        }

        int memberIndex = 0;
        for (TopicPartition partition : sortedPartitions) {
            String member = sortedMembers.get(memberIndex % sortedMembers.size());
            assignment.get(member).add(partition);
            memberIndex++;
        }

        return assignment;
    }
}
