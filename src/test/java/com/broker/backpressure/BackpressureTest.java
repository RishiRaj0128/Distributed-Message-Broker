package com.broker.backpressure;

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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackpressureTest {

    private ClusterRegistry registry;
    private ReplicationManager replicationManager;
    private LagTracker lagTracker;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        replicationManager = new ReplicationManager(registry, isrTracker);
        lagTracker = new LagTracker(registry);

        registry.registerBroker(new BrokerNode("b-1", "localhost", 9091, "zone-1"));

        Topic topic = new Topic("payments-webhook-stream", 1, 1);
        Partition p0 = new Partition("payments-webhook-stream", 0, "b-1", List.of("b-1"));
        topic.addPartition(p0);
        registry.registerTopic(topic);
        replicationManager.setupPartition("payments-webhook-stream", 0, "b-1", List.of("b-1"));
    }

    @Test
    @DisplayName("Phase 5: Accurate calculation of consumer lag per partition")
    void testLagCalculation() {
        // Produce 10 messages
        for (int i = 0; i < 10; i++) {
            replicationManager.appendAndReplicate("payments-webhook-stream", 0,
                    new Message("k" + i, ("val" + i).getBytes()), AckMode.LEADER_ONLY);
        }

        // Consumer group only committed offset 4
        registry.getOffsetTracker().commitOffset("webhook-worker-group", "payments-webhook-stream", 0, 4L);

        long lag = lagTracker.getLag("webhook-worker-group", "payments-webhook-stream", 0);
        assertThat(lag).isEqualTo(6L); // 10 HW - 4 committed = 6 lag
    }

    @Test
    @DisplayName("Phase 5: REJECT_WITH_BUSY throws BrokerBusyException when lag exceeds threshold")
    void testFlowControllerRejectsWhenLagExceeded() {
        FlowController flowController = new FlowController(lagTracker, 5L, FlowController.BackpressurePolicy.REJECT_WITH_BUSY);

        // Produce 8 messages while consumer committed only 2 (lag = 6 > maxTolerable 5)
        for (int i = 0; i < 8; i++) {
            replicationManager.appendAndReplicate("payments-webhook-stream", 0,
                    new Message("k" + i, ("val" + i).getBytes()), AckMode.LEADER_ONLY);
        }
        registry.getOffsetTracker().commitOffset("webhook-worker-group", "payments-webhook-stream", 0, 2L);

        assertThatThrownBy(() -> flowController.checkIngress("payments-webhook-stream", 0))
                .isInstanceOf(BrokerBusyException.class)
                .hasMessageContaining("Consumer lag 6 exceeds safety threshold 5");

        // Consumer catches up to offset 7 (lag = 1 < 5)
        registry.getOffsetTracker().commitOffset("webhook-worker-group", "payments-webhook-stream", 0, 7L);

        // Now ingress should succeed without exception
        flowController.checkIngress("payments-webhook-stream", 0);
        assertThat(lagTracker.getLag("webhook-worker-group", "payments-webhook-stream", 0)).isEqualTo(1L);
    }

    @Test
    @DisplayName("Phase 5: THROTTLE_PRODUCER introduces measured latency delay when consumer lags")
    void testFlowControllerThrottlesProducer() {
        FlowController flowController = new FlowController(lagTracker, 3L, FlowController.BackpressurePolicy.THROTTLE_PRODUCER);

        for (int i = 0; i < 6; i++) {
            replicationManager.appendAndReplicate("payments-webhook-stream", 0,
                    new Message("k" + i, ("val" + i).getBytes()), AckMode.LEADER_ONLY);
        }
        registry.getOffsetTracker().commitOffset("webhook-worker-group", "payments-webhook-stream", 0, 1L);
        // Lag = 5 > 3

        long start = System.currentTimeMillis();
        flowController.checkIngress("payments-webhook-stream", 0);
        long elapsed = System.currentTimeMillis() - start;

        // Verify delay was injected
        assertThat(elapsed).isGreaterThanOrEqualTo(20L);
    }
}
