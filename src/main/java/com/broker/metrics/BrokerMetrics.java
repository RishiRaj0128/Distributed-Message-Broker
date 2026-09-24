package com.broker.metrics;

import com.broker.backpressure.LagTracker;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.replication.IsrTracker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enterprise Prometheus metrics instrumentation for distributed broker cluster.
 * Tracks throughput, consumer lag, ISR health, and failover events.
 */
public class BrokerMetrics {

    private final PrometheusMeterRegistry registry;
    private final ClusterRegistry clusterRegistry;
    private final IsrTracker isrTracker;
    private final LagTracker lagTracker;

    private final Counter messagesInCounter;
    private final Counter leaderElectionsCounter;
    private final Counter backpressureCounter;
    private final Counter duplicatesSuppressedCounter;
    private final Timer replicationTimer;

    public BrokerMetrics(ClusterRegistry clusterRegistry, IsrTracker isrTracker, LagTracker lagTracker) {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.clusterRegistry = Objects.requireNonNull(clusterRegistry);
        this.isrTracker = Objects.requireNonNull(isrTracker);
        this.lagTracker = Objects.requireNonNull(lagTracker);

        this.messagesInCounter = Counter.builder("broker_messages_in_total")
                .description("Total number of successfully ingested messages")
                .register(registry);

        this.leaderElectionsCounter = Counter.builder("broker_leader_elections_total")
                .description("Total number of partition leader election events")
                .register(registry);

        this.backpressureCounter = Counter.builder("broker_backpressure_events_total")
                .description("Total number of backpressure throttle or rejection events")
                .register(registry);

        this.duplicatesSuppressedCounter = Counter.builder("broker_producer_duplicates_suppressed_total")
                .description("Total idempotent duplicate messages detected and suppressed")
                .register(registry);

        this.replicationTimer = Timer.builder("broker_replication_latency_ms")
                .description("Replication latency from leader to quorum ISR")
                .register(registry);

        registerGauges();
    }

    private void registerGauges() {
        // Under-replicated partitions gauge
        Gauge.builder("broker_under_replicated_partitions", this, BrokerMetrics::computeUnderReplicatedCount)
                .description("Number of partitions with ISR size less than configured replication factor")
                .register(registry);

        // Active broker nodes count
        Gauge.builder("broker_active_nodes_count", clusterRegistry, r -> r.getAllBrokers().stream().filter(com.broker.core.BrokerNode::isAlive).count())
                .description("Number of alive broker nodes in the cluster")
                .register(registry);
    }

    private double computeUnderReplicatedCount() {
        long count = 0;
        for (Topic t : clusterRegistry.getAllTopics()) {
            for (Partition p : t.getPartitions().values()) {
                if (isrTracker.isUnderReplicated(t.getName(), p.getPartitionId(), t.getReplicationFactor())) {
                    count++;
                }
            }
        }
        return count;
    }

    public void markMessageIngested(String topic, int partitionId) {
        messagesInCounter.increment();
    }

    public void markLeaderElection(String topic, int partitionId) {
        leaderElectionsCounter.increment();
    }

    public void markBackpressure(String topic, int partitionId) {
        backpressureCounter.increment();
    }

    public void markDuplicateSuppressed(String topic, int partitionId) {
        duplicatesSuppressedCounter.increment();
    }

    public void recordReplicationLatency(long durationMs) {
        replicationTimer.record(durationMs, TimeUnit.MILLISECONDS);
    }

    public double getMessagesInCount() {
        return messagesInCounter.count();
    }

    public double getLeaderElectionsCount() {
        return leaderElectionsCounter.count();
    }

    public double getUnderReplicatedCount() {
        return computeUnderReplicatedCount();
    }

    public String scrapePrometheus() {
        return registry.scrape();
    }

    public PrometheusMeterRegistry getRegistry() {
        return registry;
    }
}
