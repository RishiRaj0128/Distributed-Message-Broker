package com.broker.net;

import com.broker.core.ClusterRegistry;
import com.broker.core.Message;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.election.MetadataService;
import com.broker.producer.ProducerSequenceTracker;
import com.broker.replication.AckMode;
import com.broker.replication.ReplicationManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real TCP ServerSocket host for a broker node.
 * Listens on physical network interfaces, accepts incoming producer/consumer/replica sockets,
 * and processes requests concurrently.
 */
public class BrokerServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String brokerId;
    private final String host;
    private int port;
    private final ClusterRegistry registry;
    private final ReplicationManager replicationManager;
    private final MetadataService metadataService;
    private final ProducerSequenceTracker sequenceTracker;

    private ServerSocket serverSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService serverExecutor;
    private ExecutorService clientHandlerPool;
    private final Set<Socket> activeClientSockets = ConcurrentHashMap.newKeySet();

    public BrokerServer(String brokerId,
                        String host,
                        int port,
                        ClusterRegistry registry,
                        ReplicationManager replicationManager,
                        MetadataService metadataService,
                        ProducerSequenceTracker sequenceTracker) {
        this.brokerId = brokerId;
        this.host = host != null ? host : "127.0.0.1";
        this.port = port;
        this.registry = registry;
        this.replicationManager = replicationManager;
        this.metadataService = metadataService;
        this.sequenceTracker = sequenceTracker;
    }

    public synchronized void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(host, port));
            this.port = serverSocket.getLocalPort();

            serverExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "broker-listener-" + brokerId));
            clientHandlerPool = Executors.newCachedThreadPool(r -> new Thread(r, "broker-worker-" + brokerId));

            serverExecutor.submit(this::acceptConnectionsLoop);
            log.info("BrokerServer [{}] online and listening on TCP {}:{}", brokerId, host, this.port);
        }
    }

    private void acceptConnectionsLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                activeClientSockets.add(clientSocket);
                clientHandlerPool.submit(() -> handleClient(clientSocket));
            } catch (SocketException se) {
                if (!running.get()) break;
                log.debug("ServerSocket closed on broker {}", brokerId);
            } catch (IOException e) {
                if (running.get()) {
                    log.error("Error accepting socket on broker {}", brokerId, e);
                }
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (clientSocket;
             DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {

            while (running.get() && !clientSocket.isClosed()) {
                String reqJson;
                try {
                    reqJson = NetworkProtocol.readFrame(in);
                } catch (EOFException | SocketException e) {
                    break; // Client closed connection cleanly
                }

                NetworkProtocol.Request req = mapper.readValue(reqJson, NetworkProtocol.Request.class);
                NetworkProtocol.Response resp = processRequest(req);

                String respJson = mapper.writeValueAsString(resp);
                NetworkProtocol.writeFrame(out, respJson);
            }
        } catch (Exception e) {
            if (running.get()) {
                log.debug("Client socket session ended on {}: {}", brokerId, e.getMessage());
            }
        } finally {
            activeClientSockets.remove(clientSocket);
        }
    }

    private NetworkProtocol.Response processRequest(NetworkProtocol.Request req) {
        try {
            switch (req.getType()) {
                case PRODUCE -> {
                    // Check deduplication
                    if (req.getSenderId() != null && req.getSequenceNumber() >= 0) {
                        var check = sequenceTracker.checkAndValidate(
                                req.getSenderId(), req.getTopic(), req.getPartitionId(), req.getSequenceNumber());
                        if (check.isDuplicate()) {
                            return NetworkProtocol.Response.ok(check.assignedOffset(), "DUPLICATE_SUPPRESSED");
                        }
                    }

                    AckMode ack = req.getAckMode() != null ? AckMode.valueOf(req.getAckMode()) : AckMode.QUORUM_ISR;
                    Message msg = new Message(
                            UUID.randomUUID().toString(),
                            "key",
                            req.getPayload().getBytes(),
                            Collections.emptyMap(),
                            null,
                            req.getSenderId(),
                            req.getSequenceNumber(),
                            -1L
                    );

                    Message committed = replicationManager.appendAndReplicate(req.getTopic(), req.getPartitionId(), msg, ack);

                    if (req.getSenderId() != null && req.getSequenceNumber() >= 0) {
                        sequenceTracker.recordCommittedSequence(
                                req.getSenderId(), req.getTopic(), req.getPartitionId(), req.getSequenceNumber(), committed.getOffset());
                    }

                    return NetworkProtocol.Response.ok(committed.getOffset(), "COMMITTED");
                }

                case FETCH -> {
                    Topic topic = registry.getTopic(req.getTopic());
                    if (topic == null) {
                        return NetworkProtocol.Response.error("Topic not found: " + req.getTopic());
                    }
                    Partition partition = topic.getPartition(req.getPartitionId());
                    if (partition == null) {
                        return NetworkProtocol.Response.error("Partition not found: " + req.getPartitionId());
                    }

                    List<Message> messages = partition.readCommitted(req.getOffset(), 100);
                    List<String> payloads = messages.stream().map(Message::getPayloadAsString).toList();
                    String serialized = mapper.writeValueAsString(payloads);
                    return NetworkProtocol.Response.ok(partition.getHighWatermark(), serialized);
                }

                case METADATA -> {
                    var meta = metadataService.getPartitionMetadata(req.getTopic(), req.getPartitionId());
                    if (meta == null || meta.leaderEndpoint() == null) {
                        return NetworkProtocol.Response.error("Metadata not found or no leader elected");
                    }
                    NetworkProtocol.Response resp = NetworkProtocol.Response.ok(meta.leaderEpoch(), "METADATA");
                    resp.setLeaderId(meta.leaderBrokerId());
                    resp.setLeaderPort(meta.leaderEndpoint().getPort());
                    return resp;
                }

                case HEARTBEAT -> {
                    var node = registry.getBroker(req.getSenderId());
                    if (node != null) {
                        node.recordHeartbeat();
                    }
                    return NetworkProtocol.Response.ok(0, "PONG");
                }

                case REPLICATE -> {
                    // Follower replica write pass
                    var log = replicationManager.getBrokerPartitionLog(brokerId, req.getTopic(), req.getPartitionId());
                    Message rMsg = new Message("rep-key", req.getPayload().getBytes()).withOffset(req.getOffset());
                    log.append(rMsg);
                    return NetworkProtocol.Response.ok(req.getOffset(), "REPLICATED");
                }

                default -> {
                    return NetworkProtocol.Response.error("Unsupported request type: " + req.getType());
                }
            }
        } catch (Exception ex) {
            log.warn("Error processing socket request on broker {}: {}", brokerId, ex.getMessage());
            return NetworkProtocol.Response.error(ex.getMessage());
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping BrokerServer [{}] TCP {}:{}", brokerId, host, port);

            // Close all active client connections
            for (Socket s : activeClientSockets) {
                try {
                    s.close();
                } catch (IOException ignored) {}
            }
            activeClientSockets.clear();

            // Close server socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException ignored) {}
            }

            if (clientHandlerPool != null) {
                clientHandlerPool.shutdownNow();
            }
            if (serverExecutor != null) {
                serverExecutor.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    public String getBrokerId() { return brokerId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public boolean isRunning() { return running.get(); }
}
