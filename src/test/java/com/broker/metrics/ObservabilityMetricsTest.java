package com.broker.metrics;

import com.broker.backpressure.LagTracker;
import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.replication.IsrTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ObservabilityMetricsTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private LagTracker lagTracker;
    private BrokerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker();
        lagTracker = new LagTracker(registry);
        metrics = new BrokerMetrics(registry, isrTracker, lagTracker);

        registry.registerBroker(new BrokerNode("b-1", "localhost", 9091, "zone-1"));
        registry.registerBroker(new BrokerNode("b-2", "localhost", 9092, "zone-2"));

        Topic topic = new Topic("payments-audit", 1, 2);
        Partition p0 = new Partition("payments-audit", 0, "b-1", List.of("b-1", "b-2"));
        topic.addPartition(p0);
        registry.registerTopic(topic);

        isrTracker.initPartition("payments-audit", 0, "b-1", List.of("b-1", "b-2"));
    }

    @Test
    @DisplayName("Phase 7: Prometheus metrics export valid format and counters increment correctly")
    void testMetricsInstrumentationAndScrape() {
        metrics.markMessageIngested("payments-audit", 0);
        metrics.markMessageIngested("payments-audit", 0);
        metrics.markLeaderElection("payments-audit", 0);
        metrics.markDuplicateSuppressed("payments-audit", 0);
        metrics.markBackpressure("payments-audit", 0);
        metrics.recordReplicationLatency(12);

        assertThat(metrics.getMessagesInCount()).isEqualTo(2.0);
        assertThat(metrics.getLeaderElectionsCount()).isEqualTo(1.0);

        String prometheusOutput = metrics.scrapePrometheus();
        assertThat(prometheusOutput)
                .contains("broker_messages_in_total 2.0")
                .contains("broker_leader_elections_total 1.0")
                .contains("broker_producer_duplicates_suppressed_total 1.0")
                .contains("broker_backpressure_events_total 1.0")
                .contains("broker_active_nodes_count 2.0");
    }

    @Test
    @DisplayName("Phase 7: Under-replicated partitions gauge accurately detects ISR degradation")
    void testUnderReplicatedPartitionsGauge() {
        assertThat(metrics.getUnderReplicatedCount()).isZero();

        // Expel b-2 from ISR
        isrTracker.recordReplicaFetch("payments-audit", 0, "b-2", 0L);
        isrTracker.evaluateIsr("payments-audit", 0, "b-1", 100L); // lag = 100 > threshold

        assertThat(metrics.getUnderReplicatedCount()).isEqualTo(1.0);

        String scraped = metrics.scrapePrometheus();
        assertThat(scraped).contains("broker_under_replicated_partitions 1.0");
    }
}
