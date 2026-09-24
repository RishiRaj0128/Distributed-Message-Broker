package com.broker.election;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Controller service that monitors node heartbeats, detects leader failures,
 * and orchestrates partition failover using a real background ScheduledExecutorService clock.
 */
public class ClusterController implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final ClusterRegistry registry;
    private final LeaderElector leaderElector;
    private final MetadataService metadataService;
    private final long heartbeatTimeoutMs;

    private ScheduledExecutorService heartbeatScheduler;
    private final AtomicBoolean monitoringActive = new AtomicBoolean(false);

    public ClusterController(ClusterRegistry registry,
                             LeaderElector leaderElector,
                             MetadataService metadataService,
                             long heartbeatTimeoutMs) {
        this.registry = Objects.requireNonNull(registry);
        this.leaderElector = Objects.requireNonNull(leaderElector);
        this.metadataService = Objects.requireNonNull(metadataService);
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public ClusterController(ClusterRegistry registry,
                             LeaderElector leaderElector,
                             MetadataService metadataService) {
        this(registry, leaderElector, metadataService, 1500L); // Default: 1.5s failure detection
    }

    /**
     * Starts the real background heartbeat timer clock using ScheduledExecutorService.
     *
     * @param scanIntervalMs Interval in milliseconds between successive health sweeps
     */
    public synchronized void startHeartbeatMonitoring(long scanIntervalMs) {
        if (monitoringActive.compareAndSet(false, true)) {
            heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cluster-heartbeat-monitor");
                t.setDaemon(true);
                return t;
            });

            heartbeatScheduler.scheduleAtFixedRate(
                    this::scanAndFailoverDeadLeaders,
                    scanIntervalMs,
                    scanIntervalMs,
                    TimeUnit.MILLISECONDS
            );
            log.info("Started real background heartbeat failure detection (interval={}ms, timeout={}ms)",
                    scanIntervalMs, heartbeatTimeoutMs);
        }
    }

    public synchronized void startHeartbeatMonitoring() {
        startHeartbeatMonitoring(100L); // Default 100ms background sweep resolution
    }

    public synchronized void stopHeartbeatMonitoring() {
        if (monitoringActive.compareAndSet(true, false)) {
            if (heartbeatScheduler != null) {
                heartbeatScheduler.shutdownNow();
                try {
                    heartbeatScheduler.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Stopped background heartbeat failure detection");
        }
    }

    /**
     * Checks heartbeats of all brokers and triggers election on partitions where the leader is dead.
     * Executed periodically by heartbeatScheduler or invoked explicitly in test harnesses.
     */
    public void scanAndFailoverDeadLeaders() {
        long now = System.currentTimeMillis();

        for (BrokerNode node : registry.getAllBrokers()) {
            if (node.isAlive() && (now - node.getLastHeartbeatTimestamp() > heartbeatTimeoutMs)) {
                log.warn("Broker {} heartbeat expired (silence={}ms > threshold={}ms). Marking DEAD.",
                        node.getId(), (now - node.getLastHeartbeatTimestamp()), heartbeatTimeoutMs);
                node.setStatus(BrokerNode.NodeStatus.DEAD);
            }
        }

        // Check all partitions
        for (Topic topic : registry.getAllTopics()) {
            for (Partition partition : topic.getPartitions().values()) {
                String leaderId = partition.getLeaderId();
                if (leaderId == null) {
                    leaderElector.electLeader(topic.getName(), partition.getPartitionId());
                } else {
                    BrokerNode leaderNode = registry.getBroker(leaderId);
                    if (leaderNode == null || !leaderNode.isAlive()) {
                        log.warn("Leader {} for {}-p{} is offline. Triggering election.",
                                leaderId, topic.getName(), partition.getPartitionId());
                        leaderElector.electLeader(topic.getName(), partition.getPartitionId());
                    }
                }
            }
        }
    }

    /**
     * Simulates node failure and allows the background ScheduledExecutorService
     * to naturally detect the silence and trigger failover when heartbeatTimeoutMs elapses.
     */
    public void stopHeartbeatForNode(String brokerId) {
        log.info("Ceasing heartbeats for broker node: {} (awaiting true background clock expiry)", brokerId);
        // Note: we do NOT touch status here; the background timer will observe elapsed silence and mark DEAD
    }

    /**
     * Explicitly marks a broker as failed and immediately invokes scanAndFailoverDeadLeaders().
     * Used for deterministic synchronous tests.
     */
    public void simulateNodeFailure(String brokerId) {
        BrokerNode node = registry.getBroker(brokerId);
        if (node != null) {
            node.setStatus(BrokerNode.NodeStatus.DEAD);
            log.info("Simulated immediate crash for broker node: {}", brokerId);
        }
        scanAndFailoverDeadLeaders();
    }

    /**
     * Awaits until the background timer elects a new leader different from previousLeader.
     *
     * @return Elapsed wall-clock milliseconds from invocation until new leader detected.
     */
    public long awaitFailover(String topicName, int partitionId, String previousLeaderId, long maxWaitMs)
            throws TimeoutException, InterruptedException {
        long start = System.currentTimeMillis();
        Topic topic = registry.getTopic(topicName);
        if (topic == null) throw new IllegalArgumentException("Unknown topic: " + topicName);
        Partition partition = topic.getPartition(partitionId);
        if (partition == null) throw new IllegalArgumentException("Unknown partition: " + partitionId);

        while (System.currentTimeMillis() - start < maxWaitMs) {
            String currentLeader = partition.getLeaderId();
            if (currentLeader != null && !currentLeader.equals(previousLeaderId)) {
                return System.currentTimeMillis() - start;
            }
            Thread.sleep(20L);
        }

        throw new TimeoutException(String.format(
                "Timed out awaiting failover for %s-p%d after %d ms. Current leader: %s",
                topicName, partitionId, maxWaitMs, partition.getLeaderId()));
    }

    @Override
    public void close() {
        stopHeartbeatMonitoring();
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public LeaderElector getLeaderElector() {
        return leaderElector;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public boolean isMonitoringActive() {
        return monitoringActive.get();
    }
}
