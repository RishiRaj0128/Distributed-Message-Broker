package com.broker.backpressure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Applies dynamic backpressure and flow control to prevent broker memory saturation
 * when producer ingress rate persistently outpaces consumer consumption rate.
 */
public class FlowController {
    private static final Logger log = LoggerFactory.getLogger(FlowController.class);

    public enum BackpressurePolicy {
        REJECT_WITH_BUSY,
        THROTTLE_PRODUCER
    }

    private final LagTracker lagTracker;
    private final long maxTolerableLag;
    private final BackpressurePolicy policy;

    public FlowController(LagTracker lagTracker, long maxTolerableLag, BackpressurePolicy policy) {
        this.lagTracker = Objects.requireNonNull(lagTracker);
        this.maxTolerableLag = maxTolerableLag;
        this.policy = Objects.requireNonNull(policy);
    }

    public FlowController(LagTracker lagTracker, long maxTolerableLag) {
        this(lagTracker, maxTolerableLag, BackpressurePolicy.REJECT_WITH_BUSY);
    }

    /**
     * Checks if the partition can accept new writes, or applies backpressure.
     *
     * @throws BrokerBusyException if policy is REJECT_WITH_BUSY and lag > maxTolerableLag
     */
    public void checkIngress(String topic, int partitionId) {
        long currentLag = lagTracker.getMaxLagForPartition(topic, partitionId);
        if (currentLag >= maxTolerableLag) {
            log.warn("Backpressure triggered on {}-p{}: current lag {} >= threshold {}",
                    topic, partitionId, currentLag, maxTolerableLag);

            if (policy == BackpressurePolicy.REJECT_WITH_BUSY) {
                throw new BrokerBusyException(topic, partitionId, currentLag, maxTolerableLag);
            } else if (policy == BackpressurePolicy.THROTTLE_PRODUCER) {
                try {
                    // Dynamic throttle proportional to lag overshoot
                    long throttleDelayMs = Math.min(500L, (currentLag - maxTolerableLag + 1) * 10L);
                    Thread.sleep(throttleDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public LagTracker getLagTracker() {
        return lagTracker;
    }

    public long getMaxTolerableLag() {
        return maxTolerableLag;
    }

    public BackpressurePolicy getPolicy() {
        return policy;
    }
}
