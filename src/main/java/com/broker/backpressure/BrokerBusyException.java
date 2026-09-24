package com.broker.backpressure;

/**
 * Thrown when broker applies backpressure due to excessive consumer lag,
 * signaling the producer to back off and retry.
 */
public class BrokerBusyException extends RuntimeException {
    private final String topic;
    private final int partitionId;
    private final long currentLag;
    private final long maxThreshold;

    public BrokerBusyException(String topic, int partitionId, long currentLag, long maxThreshold) {
        super(String.format("Broker busy on %s-p%d: Consumer lag %d exceeds safety threshold %d. Backing off.",
                topic, partitionId, currentLag, maxThreshold));
        this.topic = topic;
        this.partitionId = partitionId;
        this.currentLag = currentLag;
        this.maxThreshold = maxThreshold;
    }

    public String getTopic() { return topic; }
    public int getPartitionId() { return partitionId; }
    public long getCurrentLag() { return currentLag; }
    public long getMaxThreshold() { return maxThreshold; }
}
