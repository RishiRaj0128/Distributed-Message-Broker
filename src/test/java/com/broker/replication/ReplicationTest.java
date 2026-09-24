package com.broker.replication;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicationTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private ReplicationManager replicationManager;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker(3L, 1000L); // Max 3 offsets lag or 1 second
        replicationManager = new ReplicationManager(registry, isrTracker);

        // Register 3 brokers across 3 zones
        registry.registerBroker(new BrokerNode("b-1", "localhost", 9091, "zone-a"));
        registry.registerBroker(new BrokerNode("b-2", "localhost", 9092, "zone-b"));
        registry.registerBroker(new BrokerNode("b-3", "localhost", 9093, "zone-c"));

        Topic topic = new Topic("payments-events", 1, 3);
        Partition p0 = new Partition("payments-events", 0, "b-1", List.of("b-1", "b-2", "b-3"));
        topic.addPartition(p0);
        registry.registerTopic(topic);

        replicationManager.setupPartition("payments-events", 0, "b-1", List.of("b-1", "b-2", "b-3"));
    }

    @Test
    @DisplayName("Phase 1: Messages replicate across all 3 ISR brokers with ALL_ISR ack")
    void testReplicationAcrossAllBrokers() {
        Message msg = new Message("tx-1", "PAYMENT_INITIATED".getBytes());
        Message committed = replicationManager.appendAndReplicate("payments-events", 0, msg, AckMode.ALL_ISR);

        assertThat(committed.getOffset()).isEqualTo(0L);

        // Verify leader log
        assertThat(replicationManager.getBrokerPartitionLog("b-1", "payments-events", 0).size()).isEqualTo(1);
        // Verify followers replicated
        assertThat(replicationManager.getBrokerPartitionLog("b-2", "payments-events", 0).size()).isEqualTo(1);
        assertThat(replicationManager.getBrokerPartitionLog("b-3", "payments-events", 0).size()).isEqualTo(1);

        Partition p0 = registry.getTopic("payments-events").getPartition(0);
        assertThat(p0.getHighWatermark()).isEqualTo(1L);

        Set<String> isr = isrTracker.getIsr("payments-events", 0);
        assertThat(isr).containsExactlyInAnyOrder("b-1", "b-2", "b-3");
    }

    @Test
    @DisplayName("Phase 1: QUORUM_ISR succeeds when 1 follower is dead (2 of 3 ack)")
    void testQuorumIsrSurvivesOneFollowerFailure() {
        // Mark b-3 as dead
        registry.getBroker("b-3").setStatus(BrokerNode.NodeStatus.DEAD);

        Message msg = new Message("tx-2", "PAYMENT_PROCESSING".getBytes());
        Message committed = replicationManager.appendAndReplicate("payments-events", 0, msg, AckMode.QUORUM_ISR);

        assertThat(committed.getOffset()).isEqualTo(0L);
        assertThat(replicationManager.getBrokerPartitionLog("b-1", "payments-events", 0).size()).isEqualTo(1);
        assertThat(replicationManager.getBrokerPartitionLog("b-2", "payments-events", 0).size()).isEqualTo(1);
        assertThat(replicationManager.getBrokerPartitionLog("b-3", "payments-events", 0).size()).isZero();
    }

    @Test
    @DisplayName("Phase 1: ALL_ISR fails when an in-sync follower is dead, preventing silent un-replicated write")
    void testAllIsrFailsWhenFollowerCannotAck() {
        // Mark b-3 as dead, but it was previously in ISR
        registry.getBroker("b-3").setStatus(BrokerNode.NodeStatus.DEAD);

        Message msg = new Message("tx-3", "PAYMENT_COMMITTED".getBytes());
        assertThatThrownBy(() -> replicationManager.appendAndReplicate("payments-events", 0, msg, AckMode.ALL_ISR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Replication quorum failed");
    }

    @Test
    @DisplayName("Phase 1: Lagging follower is expelled from ISR and under-replicated partition flag is raised")
    void testLaggingFollowerExpulsionFromIsr() {
        // Leader has 10 messages, follower b-2 only replicated up to 2
        isrTracker.recordReplicaFetch("payments-events", 0, "b-2", 2L);
        isrTracker.recordReplicaFetch("payments-events", 0, "b-3", 10L);

        Set<String> updatedIsr = isrTracker.evaluateIsr("payments-events", 0, "b-1", 10L);
        assertThat(updatedIsr).doesNotContain("b-2");
        assertThat(updatedIsr).contains("b-1", "b-3");

        assertThat(isrTracker.isUnderReplicated("payments-events", 0, 3)).isTrue();

        // When b-2 catches up to offset 9 (lag = 1 <= maxLag 3)
        isrTracker.recordReplicaFetch("payments-events", 0, "b-2", 9L);
        Set<String> recoveredIsr = isrTracker.evaluateIsr("payments-events", 0, "b-1", 10L);
        assertThat(recoveredIsr).contains("b-2");
    }
}
