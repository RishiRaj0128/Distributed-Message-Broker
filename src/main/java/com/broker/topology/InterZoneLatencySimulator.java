package com.broker.topology;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Simulates cross-datacenter and edge latency profiles between broker nodes.
 */
public class InterZoneLatencySimulator {
    private static final Logger log = LoggerFactory.getLogger(InterZoneLatencySimulator.class);

    private final ClusterRegistry registry;
    private final long intraZoneLatencyMs;
    private final long interZoneLatencyMs;

    public InterZoneLatencySimulator(ClusterRegistry registry, long intraZoneLatencyMs, long interZoneLatencyMs) {
        this.registry = Objects.requireNonNull(registry);
        this.intraZoneLatencyMs = intraZoneLatencyMs;
        this.interZoneLatencyMs = interZoneLatencyMs;
    }

    public InterZoneLatencySimulator(ClusterRegistry registry) {
        this(registry, 1L, 25L); // 1ms intra-zone, 25ms cross-DC
    }

    /**
     * Calculates simulated network roundtrip delay between two broker nodes.
     */
    public long getSimulatedDelayMs(String brokerId1, String brokerId2) {
        if (brokerId1.equals(brokerId2)) return 0L;

        BrokerNode b1 = registry.getBroker(brokerId1);
        BrokerNode b2 = registry.getBroker(brokerId2);

        if (b1 == null || b2 == null) return 0L;

        if (b1.getZone().equalsIgnoreCase(b2.getZone())) {
            return intraZoneLatencyMs;
        } else {
            return interZoneLatencyMs;
        }
    }

    /**
     * Injects the realistic delay synchronously for benchmarking or chaos tests.
     */
    public void injectDelay(String brokerId1, String brokerId2) {
        long delay = getSimulatedDelayMs(brokerId1, brokerId2);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
