package com.broker;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.dsl.ClusterTopologySpec;
import com.broker.dsl.TopologyReconciler;
import com.broker.election.ClusterController;
import com.broker.election.LeaderElector;
import com.broker.election.MetadataService;
import com.broker.net.BrokerServer;
import com.broker.producer.ProducerSequenceTracker;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone executable entrypoint to run a live Broker Node process over real TCP sockets.
 *
 * Single-Node Quickstart:
 *   java -cp target/distributed-message-broker-1.0.0-SNAPSHOT.jar com.broker.BrokerNodeMain broker-01 9092 zone-1
 *
 * Multi-Broker Cluster:
 *   Terminal 1: java -cp target/... com.broker.BrokerNodeMain broker-mum-01 9091 ap-south-1a specs/topology-cluster.yaml
 *   Terminal 2: java -cp target/... com.broker.BrokerNodeMain broker-blr-01 9092 ap-south-1b specs/topology-cluster.yaml
 *   Terminal 3: java -cp target/... com.broker.BrokerNodeMain broker-del-01 9093 ap-south-1c specs/topology-cluster.yaml
 */
public class BrokerNodeMain {
    private static final Logger log = LoggerFactory.getLogger(BrokerNodeMain.class);

    public static void main(String[] args) {
        String brokerId = args.length > 0 ? args[0] : "broker-01";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9092;
        String zone = args.length > 2 ? args[2] : "ap-south-1a";
        String specPath = args.length > 3 ? args[3] : null;

        System.out.println("==================================================================");
        System.out.println(" DISTRIBUTED MESSAGE BROKER - LIVE TCP SERVER");
        System.out.println(" Node ID:         " + brokerId);
        System.out.println(" TCP Listen Port: " + port);
        System.out.println(" Datacenter Zone: " + zone);
        System.out.println(" Topology Spec:   " + (specPath != null ? specPath : "(Standalone Mode)"));
        System.out.println("==================================================================");

        ClusterRegistry registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        ReplicationManager replicationManager = new ReplicationManager(registry, isrTracker);
        MetadataService metadataService = new MetadataService(registry, isrTracker);
        LeaderElector leaderElector = new LeaderElector(registry, isrTracker, replicationManager, metadataService);
        ClusterController controller = new ClusterController(registry, leaderElector, metadataService);
        ProducerSequenceTracker sequenceTracker = new ProducerSequenceTracker();

        // Register self in registry
        BrokerNode selfNode = new BrokerNode(brokerId, "127.0.0.1", port, zone);
        registry.registerBroker(selfNode);

        if (specPath != null && new File(specPath).exists()) {
            try {
                TopologyReconciler reconciler = new TopologyReconciler(registry, replicationManager, isrTracker);
                ClusterTopologySpec spec = reconciler.parseSpec(new File(specPath));
                var report = reconciler.reconcile(spec);
                log.info("Topology DSL Reconciled: {} topics, {} partitions, converged={}",
                        report.topicsCreated(), report.partitionsCreated(), report.isConverged());
            } catch (IOException e) {
                log.warn("Could not load topology spec file {}: {}", specPath, e.getMessage());
            }
        } else {
            // Standalone mode: seed default topics with self as leader
            Topic paymentsTopic = new Topic("payments", 3, 1);
            for (int p = 0; p < 3; p++) {
                Partition partition = new Partition("payments", p, brokerId, List.of(brokerId));
                paymentsTopic.addPartition(partition);
                replicationManager.setupPartition("payments", p, brokerId, List.of(brokerId));
            }
            registry.registerTopic(paymentsTopic);

            Topic ordersTopic = new Topic("orders", 2, 1);
            for (int p = 0; p < 2; p++) {
                Partition partition = new Partition("orders", p, brokerId, List.of(brokerId));
                ordersTopic.addPartition(partition);
                replicationManager.setupPartition("orders", p, brokerId, List.of(brokerId));
            }
            registry.registerTopic(ordersTopic);

            log.info("Initialized default standalone topics: 'payments' (3 partitions), 'orders' (2 partitions)");
        }

        // Start self-heartbeat emitter to keep node marked ALIVE
        ScheduledExecutorService selfHeartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "self-heartbeat-" + brokerId);
            t.setDaemon(true);
            return t;
        });
        selfHeartbeat.scheduleAtFixedRate(selfNode::recordHeartbeat, 0, 300, java.util.concurrent.TimeUnit.MILLISECONDS);

        // Start background heartbeat monitoring
        controller.startHeartbeatMonitoring(100L);

        // Start real TCP ServerSocket listener
        BrokerServer server = new BrokerServer(
                brokerId, "127.0.0.1", port, registry, replicationManager, metadataService, sequenceTracker);

        try {
            server.start();
        } catch (IOException e) {
            log.error("Failed to start BrokerServer on port {}", port, e);
            System.exit(1);
        }

        // Register clean JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[SHUTDOWN] Intercepted termination signal. Stopping broker " + brokerId + "...");
            controller.stopHeartbeatMonitoring();
            server.stop();
            System.out.println("[SHUTDOWN] Broker " + brokerId + " stopped cleanly.");
        }, "broker-shutdown-hook"));

        System.out.println("\n>>> Broker node [" + brokerId + "] is LIVE and accepting TCP connections on port " + port + " <<<");
        System.out.println("Available Topics: " + registry.getAllTopics().stream().map(Topic::getName).toList());
        System.out.println("Interact using BrokerClientMain or press Ctrl+C to terminate.\n");

        // Keep process alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
