package com.broker.topology;

import com.broker.core.ClusterRegistry;
import com.broker.dsl.ClusterTopologySpec;
import com.broker.dsl.TopologyReconciler;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MultiDcPlacementTest {

    private ClusterRegistry registry;
    private MultiDcPlacementEngine placementEngine;
    private InterZoneLatencySimulator latencySimulator;
    private TopologyReconciler reconciler;

    private static final String MULTI_DC_YAML = """
            version: v1alpha1
            brokers:
              - id: node-mum-1
                host: 10.1.1.1
                port: 9092
                zone: dc-mumbai-1
              - id: node-mum-2
                host: 10.1.1.2
                port: 9092
                zone: dc-mumbai-1
              - id: node-blr-1
                host: 10.2.1.1
                port: 9092
                zone: dc-bangalore-1
              - id: node-del-1
                host: 10.3.1.1
                port: 9092
                zone: edge-delhi-1
            topics:
              - name: payment-settlement
                partitions: 2
                replicationFactor: 3
                placementPolicy:
                  minDistinctZones: 2
            """;

    @BeforeEach
    void setUp() throws Exception {
        registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        ReplicationManager replicationManager = new ReplicationManager(registry, isrTracker);
        reconciler = new TopologyReconciler(registry, replicationManager, isrTracker);

        ClusterTopologySpec spec = reconciler.parseSpec(MULTI_DC_YAML);
        reconciler.reconcile(spec);

        placementEngine = new MultiDcPlacementEngine(registry);
        latencySimulator = new InterZoneLatencySimulator(registry, 1L, 20L);
    }

    @Test
    @DisplayName("Phase 8: Multi-DC placement audit confirms replicas span multiple failure zones")
    void testAuditMultiDcPlacement() {
        var audit = placementEngine.auditClusterPlacement(2);

        assertThat(audit.isCompliant()).isTrue();
        assertThat(audit.violations()).isEmpty();
        assertThat(audit.partitionZoneMap().get("payment-settlement-p0")).hasSizeGreaterThanOrEqualTo(2);
        assertThat(audit.partitionZoneMap().get("payment-settlement-p1")).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Phase 8: Catastrophic total outage of dc-mumbai-1 leaves all partitions surviving in blr / del")
    void testZoneOutageSurvivability() {
        Map<String, Boolean> survivability = placementEngine.simulateZoneOutage("dc-mumbai-1");

        assertThat(survivability).containsEntry("payment-settlement-p0", true);
        assertThat(survivability).containsEntry("payment-settlement-p1", true);
    }

    @Test
    @DisplayName("Phase 8: InterZoneLatencySimulator measures differentiated intra-zone vs cross-DC latency")
    void testLatencyDifferential() {
        long intraZoneDelay = latencySimulator.getSimulatedDelayMs("node-mum-1", "node-mum-2");
        long crossDcDelay = latencySimulator.getSimulatedDelayMs("node-mum-1", "node-blr-1");

        assertThat(intraZoneDelay).isEqualTo(1L);
        assertThat(crossDcDelay).isEqualTo(20L);
    }
}
