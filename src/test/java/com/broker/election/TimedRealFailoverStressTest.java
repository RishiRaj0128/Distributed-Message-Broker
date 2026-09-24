package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.producer.IdempotentProducer;
import com.broker.producer.ProducerSequenceTracker;
import com.broker.replication.AckMode;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class TimedRealFailoverStressTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private ReplicationManager replicationManager;
    private MetadataService metadataService;
    private LeaderElector leaderElector;
    private ClusterController controller;
    private ProducerSequenceTracker sequenceTracker;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker(10L, 2000L);
        replicationManager = new ReplicationManager(registry, isrTracker);
        metadataService = new MetadataService(registry, isrTracker);
        leaderElector = new LeaderElector(registry, isrTracker, replicationManager, metadataService);
        sequenceTracker = new ProducerSequenceTracker();
    }

    @AfterEach
    void tearDown() {
        if (controller != null) {
            controller.stopHeartbeatMonitoring();
        }
    }

    @Test
    @DisplayName("Real Background Clock: True wall-clock failover measured across genuine heartbeat timeout")
    void testRealClockHeartbeatFailoverTiming() throws Exception {
        // Heartbeat timeout set to 500ms, checked every 50ms by real ScheduledExecutorService
        controller = new ClusterController(registry, leaderElector, metadataService, 500L);

        BrokerNode b1 = new BrokerNode("b-1", "127.0.0.1", 9091, "zone-1");
        BrokerNode b2 = new BrokerNode("b-2", "127.0.0.1", 9092, "zone-2");
        registry.registerBroker(b1);
        registry.registerBroker(b2);

        Topic topic = new Topic("timed-stream", 1, 2);
        Partition p0 = new Partition("timed-stream", 0, "b-1", List.of("b-1", "b-2"));
        topic.addPartition(p0);
        registry.registerTopic(topic);
        replicationManager.setupPartition("timed-stream", 0, "b-1", List.of("b-1", "b-2"));

        // Produce initial message
        replicationManager.appendAndReplicate("timed-stream", 0,
                new Message("k0", "init-msg".getBytes()), AckMode.ALL_ISR);

        // Start real background ScheduledExecutorService
        controller.startHeartbeatMonitoring(50L);
        assertThat(controller.isMonitoringActive()).isTrue();

        // Synchronize heartbeat baseline right before failure
        b1.recordHeartbeat();
        b2.recordHeartbeat();

        // b-1 stops heartbeating at this exact instant (keep b-2 heartbeating)
        Thread heartbeatKeeper = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                b2.recordHeartbeat();
                try {
                    Thread.sleep(30L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatKeeper.setDaemon(true);
        heartbeatKeeper.start();

        // Await failover driven solely by the background ScheduledExecutorService
        long elapsedMs = controller.awaitFailover("timed-stream", 0, "b-1", 3000L);
        heartbeatKeeper.interrupt();

        System.out.println("\n=======================================================");
        System.out.println(">>> MEASURED REAL CLOCK FAILOVER TIME: " + elapsedMs + " ms <<<");
        System.out.println("=======================================================\n");

        Partition liveP0 = registry.getTopic("timed-stream").getPartition(0);
        assertThat(liveP0.getLeaderId()).isEqualTo("b-2");

        // The failover MUST wait for the real heartbeat timeout (500ms) plus sweep resolution (50ms)
        // With thread scheduling resolution, this typically lands in the 500ms - 750ms range.
        assertThat(elapsedMs).isGreaterThanOrEqualTo(450L);
        assertThat(elapsedMs).isLessThan(1500L);
    }

    @Test
    @DisplayName("Stress Test: Sustaining zero acknowledged message loss across 50+ simulated broker-failure cycles")
    void test50PlusSimulatedBrokerFailureCyclesZeroLoss() {
        controller = new ClusterController(registry, leaderElector, metadataService, 1000L);

        // 3-node cluster
        List<String> brokerIds = List.of("node-1", "node-2", "node-3");
        for (int i = 0; i < brokerIds.size(); i++) {
            registry.registerBroker(new BrokerNode(brokerIds.get(i), "127.0.0.1", 9100 + i, "zone-" + (i + 1)));
        }

        Topic topic = new Topic("stress-payments", 1, 3);
        Partition p0 = new Partition("stress-payments", 0, "node-1", brokerIds);
        topic.addPartition(p0);
        registry.registerTopic(topic);
        replicationManager.setupPartition("stress-payments", 0, "node-1", brokerIds);

        IdempotentProducer producer = new IdempotentProducer(
                "prod-stress", registry, replicationManager, metadataService, sequenceTracker, 3, true);

        List<String> acknowledgedPayloads = new ArrayList<>();
        Random rng = new Random(42);

        // Execute 50 failure cycles
        for (int cycle = 0; cycle < 50; cycle++) {
            // 1. Write an acknowledged record to current leader
            String payload = "PAYMENT_TX_CYCLE_" + cycle;
            Message committed = producer.send("stress-payments", "key-" + cycle, payload.getBytes(), AckMode.QUORUM_ISR);
            acknowledgedPayloads.add(payload);

            // 2. Select a broker to fail (alternate between current leader and followers)
            Partition currentP0 = registry.getTopic("stress-payments").getPartition(0);
            String currentLeader = currentP0.getLeaderId();

            String victimBrokerId = (cycle % 2 == 0) ? currentLeader :
                    brokerIds.stream().filter(id -> !id.equals(currentLeader)).findFirst().get();

            // 3. Crash victim node
            controller.simulateNodeFailure(victimBrokerId);

            // 4. If leader was killed, election promoted a new leader from ISR
            String newLeader = registry.getTopic("stress-payments").getPartition(0).getLeaderId();
            assertThat(newLeader).isNotNull();
            assertThat(newLeader).isNotEqualTo(victimBrokerId);

            // 5. Recover the dead node back into the cluster and catch up
            BrokerNode victimNode = registry.getBroker(victimBrokerId);
            victimNode.setStatus(BrokerNode.NodeStatus.ALIVE);
            victimNode.recordHeartbeat();

            // Follower catches up with new leader's log
            var newLeaderLog = replicationManager.getBrokerPartitionLog(newLeader, "stress-payments", 0);
            var victimLog = replicationManager.getBrokerPartitionLog(victimBrokerId, "stress-payments", 0);
            for (Message m : newLeaderLog.read(victimLog.getLogEndOffset(), 100)) {
                victimLog.append(m);
            }
            isrTracker.recordReplicaFetch("stress-payments", 0, victimBrokerId, victimLog.getLogEndOffset());
            isrTracker.evaluateIsr("stress-payments", 0, newLeader, newLeaderLog.getLogEndOffset());
        }

        // Verify across all 50 failure cycles:
        // ZERO acknowledged messages lost, sequence perfectly intact
        String finalLeader = registry.getTopic("stress-payments").getPartition(0).getLeaderId();
        var finalLog = replicationManager.getBrokerPartitionLog(finalLeader, "stress-payments", 0);

        List<Message> allCommittedMessages = finalLog.read(0, 100);
        assertThat(allCommittedMessages).hasSize(50);

        for (int i = 0; i < 50; i++) {
            assertThat(allCommittedMessages.get(i).getPayloadAsString()).isEqualTo(acknowledgedPayloads.get(i));
            assertThat(allCommittedMessages.get(i).getOffset()).isEqualTo((long) i);
        }
    }
}
