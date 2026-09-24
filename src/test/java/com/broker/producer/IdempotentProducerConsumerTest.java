package com.broker.producer;

import com.broker.consumer.IdempotentConsumer;
import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.election.MetadataService;
import com.broker.replication.AckMode;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentProducerConsumerTest {

    private ClusterRegistry registry;
    private ReplicationManager replicationManager;
    private MetadataService metadataService;
    private ProducerSequenceTracker sequenceTracker;
    private IdempotentProducer producer;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        replicationManager = new ReplicationManager(registry, isrTracker);
        metadataService = new MetadataService(registry, isrTracker);
        sequenceTracker = new ProducerSequenceTracker();

        registry.registerBroker(new BrokerNode("b-1", "127.0.0.1", 9091, "zone-a"));
        Topic topic = new Topic("payments-charge", 1, 1);
        Partition p0 = new Partition("payments-charge", 0, "b-1", List.of("b-1"));
        topic.addPartition(p0);
        registry.registerTopic(topic);

        replicationManager.setupPartition("payments-charge", 0, "b-1", List.of("b-1"));

        producer = new IdempotentProducer(
                "prod-juspay-01",
                registry,
                replicationManager,
                metadataService,
                sequenceTracker,
                2,
                true
        );
    }

    @Test
    @DisplayName("Phase 3: Producer deduplicates retry without creating duplicate log records")
    void testProducerIdempotencyDeduplication() {
        Message m1 = producer.send("payments-charge", "cust-1", "CHARGED_100".getBytes(), AckMode.ALL_ISR);
        assertThat(m1.getOffset()).isEqualTo(0L);
        assertThat(m1.getSequenceNumber()).isZero();

        // Simulate network retry of sequence 0 by calling sequenceTracker directly or retrying send
        var dedupCheck = sequenceTracker.checkAndValidate("prod-juspay-01", "payments-charge", 0, 0);
        assertThat(dedupCheck.isDuplicate()).isTrue();
        assertThat(dedupCheck.assignedOffset()).isEqualTo(0L);

        // Partition commit log size must still be exactly 1
        Partition p0 = registry.getTopic("payments-charge").getPartition(0);
        assertThat(p0.getCommitLog().size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Phase 3: Sequence gap throws OutOfOrder error, preventing missing messages")
    void testProducerSequenceGapDetection() {
        producer.send("payments-charge", "cust-1", "ORDER_1".getBytes(), AckMode.ALL_ISR); // seq 0

        // Trying to produce seq 2 directly (skipping seq 1)
        assertThatThrownBy(() -> sequenceTracker.checkAndValidate("prod-juspay-01", "payments-charge", 0, 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Out of order sequence gap");
    }

    @Test
    @DisplayName("Phase 3: Idempotent consumer achieves effectively-exactly-once even under replay")
    void testIdempotentConsumerExactlyOnceSemantics() {
        producer.send("payments-charge", "tx-1", "PAYLOAD_A".getBytes(), AckMode.ALL_ISR);
        producer.send("payments-charge", "tx-2", "PAYLOAD_B".getBytes(), AckMode.ALL_ISR);

        IdempotentConsumer consumer = new IdempotentConsumer("c-1", "accounting-group", registry);
        List<String> executedMutations = new ArrayList<>();

        // Poll 1
        int processed = consumer.pollAndProcess("payments-charge", 0, 10, m -> {
            executedMutations.add(m.getPayloadAsString());
        });
        assertThat(processed).isEqualTo(2);
        assertThat(executedMutations).containsExactly("PAYLOAD_A", "PAYLOAD_B");

        // Simulate network failure / consumer restart rewinding offset back to 0
        registry.getOffsetTracker().resetOffset("accounting-group", "payments-charge", 0, 0L);

        // Poll 2 after rewind: broker re-delivers at-least-once, but consumer idempotency suppresses duplicate execution
        int replayedCount = consumer.pollAndProcess("payments-charge", 0, 10, m -> {
            executedMutations.add(m.getPayloadAsString());
        });

        assertThat(replayedCount).isZero();
        // Downstream mutations executed strictly once
        assertThat(executedMutations).hasSize(2);
    }
}
