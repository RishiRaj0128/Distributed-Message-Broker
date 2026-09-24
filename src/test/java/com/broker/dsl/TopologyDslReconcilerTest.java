package com.broker.dsl;

import com.broker.core.ClusterRegistry;
import com.broker.core.Topic;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyDslReconcilerTest {

    private ClusterRegistry registry;
    private IsrTracker isrTracker;
    private ReplicationManager replicationManager;
    private TopologyReconciler reconciler;

    private static final String SAMPLE_YAML = """
            version: v1alpha1
            brokers:
              - id: broker-01
                host: 10.0.1.10
                port: 9092
                zone: dc-mumbai-zone-1
              - id: broker-02
                host: 10.0.2.10
                port: 9092
                zone: dc-mumbai-zone-2
              - id: broker-03
                host: 10.0.3.10
                port: 9092
                zone: dc-bangalore-zone-1
            topics:
              - name: payment-authorizations
                partitions: 2
                replicationFactor: 2
                placementPolicy:
                  minDistinctZones: 2
              - name: audit-ledger
                partitions: 1
                replicationFactor: 3
                placementPolicy:
                  minDistinctZones: 3
            """;

    @BeforeEach
    void setUp() {
        registry = new ClusterRegistry();
        isrTracker = new IsrTracker();
        replicationManager = new ReplicationManager(registry, isrTracker);
        reconciler = new TopologyReconciler(registry, replicationManager, isrTracker);
    }

    @Test
    @DisplayName("Phase 6: Reconciler translates declarative YAML DSL into live cluster state with multi-zone placement")
    void testDeclarativeReconciliationFromYaml() throws Exception {
        ClusterTopologySpec spec = reconciler.parseSpec(SAMPLE_YAML);
        assertThat(spec.getBrokers()).hasSize(3);
        assertThat(spec.getTopics()).hasSize(2);

        var report = reconciler.reconcile(spec);

        assertThat(report.brokersRegistered()).isEqualTo(3);
        assertThat(report.topicsCreated()).isEqualTo(2);
        assertThat(report.partitionsCreated()).isEqualTo(3); // 2 on payment-authorizations + 1 on audit-ledger
        assertThat(report.isConverged()).isTrue();

        // Verify topic 1 placement (min 2 distinct zones)
        Topic paymentTopic = registry.getTopic("payment-authorizations");
        assertThat(paymentTopic).isNotNull();
        var p0 = paymentTopic.getPartition(0);
        assertThat(p0.getReplicaBrokerIds()).hasSize(2);

        var bLeader = registry.getBroker(p0.getLeaderId());
        var bFollower = registry.getBroker(p0.getReplicaBrokerIds().stream()
                .filter(id -> !id.equals(p0.getLeaderId()))
                .findFirst().orElseThrow());

        // Must be in distinct zones
        assertThat(bLeader.getZone()).isNotEqualTo(bFollower.getZone());

        // Verify audit-ledger has 3 replicas across 3 distinct zones
        Topic auditTopic = registry.getTopic("audit-ledger");
        var auditP0 = auditTopic.getPartition(0);
        assertThat(auditP0.getReplicaBrokerIds()).containsExactlyInAnyOrder("broker-01", "broker-02", "broker-03");
    }

    @Test
    @DisplayName("Phase 6: Reconciler is idempotent on subsequent runs without state drift")
    void testReconciliationIdempotency() throws Exception {
        ClusterTopologySpec spec = reconciler.parseSpec(SAMPLE_YAML);
        reconciler.reconcile(spec);

        // Second reconcile should detect zero new objects
        var secondReport = reconciler.reconcile(spec);
        assertThat(secondReport.brokersRegistered()).isZero();
        assertThat(secondReport.topicsCreated()).isZero();
        assertThat(secondReport.partitionsCreated()).isZero();
        assertThat(secondReport.isConverged()).isTrue();
    }

    @Test
    @DisplayName("Phase 6: Reconciler dynamically provisions expanded partition count")
    void testReconcilePartitionExpansion() throws Exception {
        ClusterTopologySpec spec = reconciler.parseSpec(SAMPLE_YAML);
        reconciler.reconcile(spec);

        // Mutate spec to expand payment-authorizations from 2 to 3 partitions
        String updatedYaml = SAMPLE_YAML.replace("partitions: 2", "partitions: 3");
        ClusterTopologySpec expandedSpec = reconciler.parseSpec(updatedYaml);

        var expandReport = reconciler.reconcile(expandedSpec);
        assertThat(expandReport.partitionsCreated()).isEqualTo(1);

        Topic paymentTopic = registry.getTopic("payment-authorizations");
        assertThat(paymentTopic.getPartitions()).containsKey(2);
    }
}
