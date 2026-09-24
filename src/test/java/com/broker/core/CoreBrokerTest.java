package com.broker.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CoreBrokerTest {

    private ClusterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
    }

    @Test
    @DisplayName("CommitLog appends monotonically and respects offsets")
    void testCommitLogAppendAndRead() {
        CommitLog log = new CommitLog();
        assertThat(log.getLogEndOffset()).isZero();

        Message m1 = log.append(new Message("key-1", "payload-1".getBytes()));
        Message m2 = log.append(new Message("key-2", "payload-2".getBytes()));

        assertThat(m1.getOffset()).isEqualTo(0L);
        assertThat(m2.getOffset()).isEqualTo(1L);
        assertThat(log.getLogEndOffset()).isEqualTo(2L);

        List<Message> read = log.read(0, 10);
        assertThat(read).hasSize(2);
        assertThat(read.get(0).getPayloadAsString()).isEqualTo("payload-1");
        assertThat(read.get(1).getPayloadAsString()).isEqualTo("payload-2");
    }

    @Test
    @DisplayName("Topic partitions correctly route keys deterministically")
    void testTopicKeyPartitioning() {
        Topic topic = new Topic("payments-stream", 3, 2);
        Partition p0 = new Partition("payments-stream", 0, "broker-1", List.of("broker-1", "broker-2"));
        Partition p1 = new Partition("payments-stream", 1, "broker-2", List.of("broker-2", "broker-3"));
        Partition p2 = new Partition("payments-stream", 2, "broker-3", List.of("broker-3", "broker-1"));
        topic.addPartition(p0);
        topic.addPartition(p1);
        topic.addPartition(p2);

        int partA1 = topic.selectPartitionForKey("order-user-1234");
        int partA2 = topic.selectPartitionForKey("order-user-1234");
        int partB = topic.selectPartitionForKey("order-user-9999");

        assertThat(partA1).isEqualTo(partA2);
        assertThat(partA1).isBetween(0, 2);
        assertThat(partB).isBetween(0, 2);
    }

    @Test
    @DisplayName("OffsetTracker persists and retrieves committed offsets per consumer group")
    void testOffsetTracking() {
        OffsetTracker tracker = registry.getOffsetTracker();
        tracker.commitOffset("analytics-group", "payments-stream", 0, 42L);
        tracker.commitOffset("analytics-group", "payments-stream", 1, 108L);

        assertThat(tracker.getCommittedOffset("analytics-group", "payments-stream", 0)).isEqualTo(42L);
        assertThat(tracker.getCommittedOffset("analytics-group", "payments-stream", 1)).isEqualTo(108L);
        assertThat(tracker.getCommittedOffset("analytics-group", "payments-stream", 2)).isZero();
    }
}
