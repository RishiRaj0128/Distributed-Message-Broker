package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.replication.AckMode;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderElectionTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private ReplicationManager replicationManager;
    private MetadataService metadataService;
    private LeaderElector leaderElector;
    private ClusterController controller;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker(5L, 2000L);
        replicationManager = new ReplicationManager(registry, isrTracker);
        metadataService = new MetadataService(registry, isrTracker);
        leaderElector = new LeaderElector(registry, isrTracker, replicationManager, metadataService);
        controller = new ClusterController(registry, leaderElector, metadataService);

        // 3 brokers
        registry.registerBroker(new BrokerNode("b-1", "10.0.1.1", 9091, "zone-1"));
        registry.registerBroker(new BrokerNode("b-2", "10.0.1.2", 9092, "zone-2"));
        registry.registerBroker(new BrokerNode("b-3", "10.0.1.3", 9093, "zone-3"));

        Topic topic = new Topic("juspay-transactions", 1, 3);
        Partition p0 = new Partition("juspay-transactions", 0, "b-1", List.of("b-1", "b-2", "b-3"));
        topic.addPartition(p0);
        registry.registerTopic(topic);

        replicationManager.setupPartition("juspay-transactions", 0, "b-1", List.of("b-1", "b-2", "b-3"));
    }

    @Test
    @DisplayName("Phase 2: Highest-offset ISR replica wins leader election when leader crashes")
    void testHighestOffsetIsrReplicaWinsElection() {
        // Produce 3 messages replicated across all 3
        replicationManager.appendAndReplicate("juspay-transactions", 0, new Message("k1", "tx-1".getBytes()), AckMode.ALL_ISR);
        replicationManager.appendAndReplicate("juspay-transactions", 0, new Message("k2", "tx-2".getBytes()), AckMode.ALL_ISR);
        replicationManager.appendAndReplicate("juspay-transactions", 0, new Message("k3", "tx-3".getBytes()), AckMode.ALL_ISR);

        // Simulate b-2 replicated an additional message while b-3 was slower
        var b2Log = replicationManager.getBrokerPartitionLog("b-2", "juspay-transactions", 0);
        b2Log.append(new Message("k4", "tx-4".getBytes()).withOffset(3L));
        isrTracker.recordReplicaFetch("juspay-transactions", 0, "b-2", 4L);
        isrTracker.recordReplicaFetch("juspay-transactions", 0, "b-3", 3L);

        // Crash current leader b-1
        controller.simulateNodeFailure("b-1");

        Partition p0 = registry.getTopic("juspay-transactions").getPartition(0);
        // b-2 must win because it has LEO=4 while b-3 has LEO=3
        assertThat(p0.getLeaderId()).isEqualTo("b-2");

        var meta = metadataService.getPartitionMetadata("juspay-transactions", 0);
        assertThat(meta.leaderBrokerId()).isEqualTo("b-2");
        assertThat(meta.leaderEpoch()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Phase 2: Mid-stream leader crash failover causes zero acknowledged message loss")
    void testMidStreamLeaderCrashWithZeroAcknowledgedMessageLoss() {
        List<Message> acknowledged = new ArrayList<>();
        AtomicInteger electionCount = new AtomicInteger();
        leaderElector.addListener((t, p, oldL, newL, epoch) -> electionCount.incrementAndGet());

        // Stream 5 acknowledged messages to b-1
        for (int i = 0; i < 5; i++) {
            Message m = replicationManager.appendAndReplicate(
                    "juspay-transactions", 0,
                    new Message("order-" + i, ("payload-" + i).getBytes()),
                    AckMode.ALL_ISR
            );
            acknowledged.add(m);
        }

        // Mid-stream kill of leader b-1
        long killStartTime = System.currentTimeMillis();
        controller.simulateNodeFailure("b-1");
        long electionDurationMs = System.currentTimeMillis() - killStartTime;

        // Election must complete quickly (bounded failover window < 100ms in simulation)
        assertThat(electionDurationMs).isLessThan(200);
        assertThat(electionCount.get()).isEqualTo(1);

        Partition p0 = registry.getTopic("juspay-transactions").getPartition(0);
        String newLeader = p0.getLeaderId();
        assertThat(newLeader).isIn("b-2", "b-3");

        // Producer writes next 5 messages successfully to new leader
        for (int i = 5; i < 10; i++) {
            Message m = replicationManager.appendAndReplicate(
                    "juspay-transactions", 0,
                    new Message("order-" + i, ("payload-" + i).getBytes()),
                    AckMode.QUORUM_ISR // since b-1 is dead, majority of remaining ISR (2) is 2
            );
            acknowledged.add(m);
        }

        // Verify that all 10 messages exist without loss in the new leader's log
        var newLeaderLog = replicationManager.getBrokerPartitionLog(newLeader, "juspay-transactions", 0);
        List<Message> finalLog = newLeaderLog.read(0, 20);

        assertThat(finalLog).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(finalLog.get(i).getPayloadAsString()).isEqualTo("payload-" + i);
            assertThat(finalLog.get(i).getOffset()).isEqualTo((long) i);
        }
    }
}
