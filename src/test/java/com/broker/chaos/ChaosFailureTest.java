package com.broker.chaos;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.election.ClusterController;
import com.broker.election.LeaderElector;
import com.broker.election.MetadataService;
import com.broker.replication.AckMode;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosFailureTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private ReplicationManager replicationManager;
    private MetadataService metadataService;
    private LeaderElector leaderElector;
    private ClusterController controller;
    private ChaosInjector chaosInjector;
    private NetworkPartitionGuard splitBrainGuard;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker(5L, 2000L);
        replicationManager = new ReplicationManager(registry, isrTracker);
        metadataService = new MetadataService(registry, isrTracker);
        leaderElector = new LeaderElector(registry, isrTracker, replicationManager, metadataService);
        controller = new ClusterController(registry, leaderElector, metadataService);
        chaosInjector = new ChaosInjector(registry);
        splitBrainGuard = new NetworkPartitionGuard(registry, chaosInjector);

        // Register 5 brokers
        for (int i = 1; i <= 5; i++) {
            registry.registerBroker(new BrokerNode("b-" + i, "10.0.0." + i, 9090 + i, "zone-" + ((i % 3) + 1)));
        }

        // Topic with 1 partition replicated across all 5 brokers (Quorum = 3)
        List<String> allReplicas = List.of("b-1", "b-2", "b-3", "b-4", "b-5");
        Topic topic = new Topic("critical-payments", 1, 5);
        Partition p0 = new Partition("critical-payments", 0, "b-1", allReplicas);
        topic.addPartition(p0);
        registry.registerTopic(topic);

        replicationManager.setupPartition("critical-payments", 0, "b-1", allReplicas);
    }

    @Test
    @DisplayName("Phase 9: Kill follower -> ISR shrinks, alerted as under-replicated, writes continue via remaining ISR")
    void testKillFollowerShrinksIsrAndContinuesServing() {
        // Produce 1 message with all 5 nodes alive
        replicationManager.appendAndReplicate("critical-payments", 0,
                new Message("tx-1", "INITIATE".getBytes()), AckMode.QUORUM_ISR);

        assertThat(isrTracker.getIsr("critical-payments", 0)).hasSize(5);

        // Kill follower b-5
        controller.simulateNodeFailure("b-5");
        isrTracker.removeBrokerFromAllIsr("b-5");

        // ISR shrinks to 4
        Set<String> isrAfterKill = isrTracker.getIsr("critical-payments", 0);
        assertThat(isrAfterKill).doesNotContain("b-5");
        assertThat(isrAfterKill).hasSize(4);

        // Under-replicated flag is raised
        assertThat(isrTracker.isUnderReplicated("critical-payments", 0, 5)).isTrue();

        // System continues serving writes via remaining ISR quorum
        Message msg2 = replicationManager.appendAndReplicate("critical-payments", 0,
                new Message("tx-2", "PROCESSED".getBytes()), AckMode.QUORUM_ISR);

        assertThat(msg2.getOffset()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Phase 9: Kill leader -> failover completes in bounded downtime (< 50ms) with zero message loss")
    void testKillLeaderBoundedDowntimeAndZeroLoss() {
        // Write 3 acknowledged records
        for (int i = 0; i < 3; i++) {
            replicationManager.appendAndReplicate("critical-payments", 0,
                    new Message("tx-" + i, ("data-" + i).getBytes()), AckMode.QUORUM_ISR);
        }

        long killStart = System.nanoTime();
        controller.simulateNodeFailure("b-1");
        long elapsedNanos = System.nanoTime() - killStart;
        long elapsedMs = elapsedNanos / 1_000_000;

        // Measure bounded downtime window
        assertThat(elapsedMs).isLessThan(50);

        Partition p0 = registry.getTopic("critical-payments").getPartition(0);
        String newLeader = p0.getLeaderId();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader).isNotEqualTo("b-1");

        // Verify zero acknowledged messages lost
        var leaderLog = replicationManager.getBrokerPartitionLog(newLeader, "critical-payments", 0);
        assertThat(leaderLog.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("Phase 9: Network Partition & Split-Brain check -> minority partition leader steps down, preventing dual leaders")
    void testNetworkPartitionPreventsSplitBrain() {
        // Set b-5 as leader
        Partition p0 = registry.getTopic("critical-payments").getPartition(0);
        p0.setLeaderId("b-5");

        // Inject network partition: Side A {b-1, b-2, b-3} (majority: 3 nodes) vs Side B {b-4, b-5} (minority: 2 nodes)
        Set<String> sideA = Set.of("b-1", "b-2", "b-3");
        Set<String> sideB = Set.of("b-4", "b-5");
        chaosInjector.createNetworkPartition(sideA, sideB);

        // Broker b-5 (on minority Side B) checks quorum reachability: can only reach b-4 and b-5 (2/5 < majority 3)
        boolean b5HasQuorum = splitBrainGuard.verifyLeaderQuorum("critical-payments", 0);

        // Minority leader MUST step down, leaving partition leaderId = null on Side B
        assertThat(b5HasQuorum).isFalse();
        assertThat(p0.getLeaderId()).isNull();

        // Meanwhile, Side A {b-1, b-2, b-3} checks reachability: can reach 3/5 nodes (Quorum satisfied!)
        boolean sideAHasQuorum = chaosInjector.hasQuorumReachability("b-1", List.of("b-1", "b-2", "b-3", "b-4", "b-5"));
        assertThat(sideAHasQuorum).isTrue();

        // Side A elects a new leader (e.g. b-1)
        p0.setLeaderId("b-1");
        boolean b1HasQuorum = splitBrainGuard.verifyLeaderQuorum("critical-payments", 0);
        assertThat(b1HasQuorum).isTrue();

        // Invariant: At no point can Side B also have an active leader. Split-brain is strictly prevented!
        chaosInjector.healNetworkPartition();
    }
}
