package com.broker.consumer;

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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupRebalanceTest {

    private ClusterRegistry registry;
    private ReplicationManager replicationManager;
    private ConsumerGroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        replicationManager = new ReplicationManager(registry, isrTracker);
        coordinator = new ConsumerGroupCoordinator(registry, 1000L);

        registry.registerBroker(new BrokerNode("b-1", "localhost", 9091, "zone-1"));

        // Topic with 3 partitions
        Topic topic = new Topic("order-events", 3, 1);
        for (int p = 0; p < 3; p++) {
            Partition partition = new Partition("order-events", p, "b-1", List.of("b-1"));
            topic.addPartition(partition);
            replicationManager.setupPartition("order-events", p, "b-1", List.of("b-1"));
        }
        registry.registerTopic(topic);
    }

    @Test
    @DisplayName("Phase 4: Partitions are fairly and disjointly assigned across 3 consumers")
    void testConsumerGroupDisjointAssignment() {
        coordinator.joinGroup("order-processors", "c-1", Set.of("order-events"));
        coordinator.joinGroup("order-processors", "c-2", Set.of("order-events"));
        coordinator.joinGroup("order-processors", "c-3", Set.of("order-events"));

        var a1 = coordinator.getAssignment("order-processors", "c-1");
        var a2 = coordinator.getAssignment("order-processors", "c-2");
        var a3 = coordinator.getAssignment("order-processors", "c-3");

        assertThat(a1).hasSize(1);
        assertThat(a2).hasSize(1);
        assertThat(a3).hasSize(1);

        // Strict Invariant: No two consumers share a partition
        Set<Integer> allAssignedPartitions = new HashSet<>();
        allAssignedPartitions.add(a1.get(0).partitionId());
        allAssignedPartitions.add(a2.get(0).partitionId());
        allAssignedPartitions.add(a3.get(0).partitionId());

        assertThat(allAssignedPartitions).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    @DisplayName("Phase 4: Mid-stream death of 1 consumer in a 3-consumer group rebalances without data loss")
    void testMidStreamConsumerDeathAndRebalance() {
        // Produce 4 messages on each partition (total 12 messages)
        for (int p = 0; p < 3; p++) {
            for (int i = 0; i < 4; i++) {
                replicationManager.appendAndReplicate("order-events", p,
                        new Message("key-" + p + "-" + i, ("val-" + p + "-" + i).getBytes()),
                        AckMode.LEADER_ONLY);
            }
        }

        // Join 3 consumers
        coordinator.joinGroup("order-processors", "c-1", Set.of("order-events"));
        coordinator.joinGroup("order-processors", "c-2", Set.of("order-events"));
        coordinator.joinGroup("order-processors", "c-3", Set.of("order-events"));

        // Instantiate IdempotentConsumers
        IdempotentConsumer cons1 = new IdempotentConsumer("c-1", "order-processors", registry);
        IdempotentConsumer cons2 = new IdempotentConsumer("c-2", "order-processors", registry);
        IdempotentConsumer cons3 = new IdempotentConsumer("c-3", "order-processors", registry);

        // Each consumer reads 2 messages from their assigned partition
        for (var tp : coordinator.getAssignment("order-processors", "c-1")) {
            cons1.pollAndProcess(tp.topic(), tp.partitionId(), 2, m -> {});
        }
        for (var tp : coordinator.getAssignment("order-processors", "c-2")) {
            cons2.pollAndProcess(tp.topic(), tp.partitionId(), 2, m -> {});
        }
        for (var tp : coordinator.getAssignment("order-processors", "c-3")) {
            cons3.pollAndProcess(tp.topic(), tp.partitionId(), 2, m -> {});
        }

        // Mid-stream: Consumer 2 crashes/leaves
        coordinator.leaveGroup("order-processors", "c-2");

        // Remaining consumers: c-1 and c-3
        var c1AssignmentAfter = coordinator.getAssignment("order-processors", "c-1");
        var c3AssignmentAfter = coordinator.getAssignment("order-processors", "c-3");

        // Total 3 partitions now split between 2 consumers (1 gets 2, 1 gets 1)
        assertThat(c1AssignmentAfter.size() + c3AssignmentAfter.size()).isEqualTo(3);

        // Surviving consumers consume remaining messages
        List<String> survivingConsumed = new ArrayList<>();
        for (var tp : c1AssignmentAfter) {
            cons1.pollAndProcess(tp.topic(), tp.partitionId(), 10, m -> survivingConsumed.add(m.getPayloadAsString()));
        }
        for (var tp : c3AssignmentAfter) {
            cons3.pollAndProcess(tp.topic(), tp.partitionId(), 10, m -> survivingConsumed.add(m.getPayloadAsString()));
        }

        // In total, across initial and surviving reads, all 12 messages are read completely
        // Check offsets committed for all 3 partitions are at LEO = 4
        for (int p = 0; p < 3; p++) {
            long committed = registry.getOffsetTracker().getCommittedOffset("order-processors", "order-events", p);
            assertThat(committed).isEqualTo(4L);
        }
    }
}
