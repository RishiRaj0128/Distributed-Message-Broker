package com.broker.net;

import com.broker.core.BrokerNode;
import com.broker.core.ClusterRegistry;
import com.broker.core.Partition;
import com.broker.core.Topic;
import com.broker.election.LeaderElector;
import com.broker.election.MetadataService;
import com.broker.producer.ProducerSequenceTracker;
import com.broker.replication.IsrTracker;
import com.broker.replication.ReplicationManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RealSocketNetworkingTest {

    private ClusterRegistry registry;
    private ReplicationManager replicationManager;
    private MetadataService metadataService;
    private ProducerSequenceTracker sequenceTracker;
    private BrokerServer leaderServer;
    private BrokerServer followerServer;
    private final ObjectMapper mapper = new ObjectMapper();

    private int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        registry = new ClusterRegistry();
        IsrTracker isrTracker = new IsrTracker();
        replicationManager = new ReplicationManager(registry, isrTracker);
        metadataService = new MetadataService(registry, isrTracker);
        sequenceTracker = new ProducerSequenceTracker();

        int leaderPort = findFreePort();
        int followerPort = findFreePort();

        BrokerNode bLeader = new BrokerNode("b-lead", "127.0.0.1", leaderPort, "zone-1");
        BrokerNode bFollow = new BrokerNode("b-follow", "127.0.0.1", followerPort, "zone-2");
        registry.registerBroker(bLeader);
        registry.registerBroker(bFollow);

        Topic topic = new Topic("net-orders", 1, 2);
        Partition p0 = new Partition("net-orders", 0, "b-lead", List.of("b-lead", "b-follow"));
        topic.addPartition(p0);
        registry.registerTopic(topic);
        replicationManager.setupPartition("net-orders", 0, "b-lead", List.of("b-lead", "b-follow"));

        leaderServer = new BrokerServer("b-lead", "127.0.0.1", leaderPort, registry, replicationManager, metadataService, sequenceTracker);
        followerServer = new BrokerServer("b-follow", "127.0.0.1", followerPort, registry, replicationManager, metadataService, sequenceTracker);

        leaderServer.start();
        followerServer.start();
    }

    @AfterEach
    void tearDown() {
        if (leaderServer != null) leaderServer.stop();
        if (followerServer != null) followerServer.stop();
    }

    @Test
    @DisplayName("Real Socket: Producer sends PRODUCE request over TCP socket and receives committed ACK")
    void testRealSocketProduceAndFetch() throws Exception {
        try (BrokerClient client = new BrokerClient("127.0.0.1", leaderServer.getPort(), 5000)) {
            client.connect();
            assertThat(client.isConnected()).isTrue();

            // 1. Send metadata query over socket
            NetworkProtocol.Request metaReq = new NetworkProtocol.Request(
                    NetworkProtocol.RequestType.METADATA, "net-orders", 0, "client-1", null);
            NetworkProtocol.Response metaResp = client.send(metaReq);

            assertThat(metaResp.isSuccess()).isTrue();
            assertThat(metaResp.getLeaderId()).isEqualTo("b-lead");
            assertThat(metaResp.getLeaderPort()).isEqualTo(leaderServer.getPort());

            // 2. Send real message over TCP socket
            NetworkProtocol.Request produceReq = new NetworkProtocol.Request(
                    NetworkProtocol.RequestType.PRODUCE, "net-orders", 0, "prod-sock-1", "PAYMENT_SETTLED_INR_5000");
            produceReq.setSequenceNumber(0);
            produceReq.setAckMode("LEADER_ONLY");

            NetworkProtocol.Response produceResp = client.send(produceReq);

            assertThat(produceResp.isSuccess()).isTrue();
            assertThat(produceResp.getAssignedOffset()).isEqualTo(0L);

            // 3. Fetch message back over TCP socket
            NetworkProtocol.Request fetchReq = new NetworkProtocol.Request(
                    NetworkProtocol.RequestType.FETCH, "net-orders", 0, "client-1", null);
            fetchReq.setOffset(0L);

            NetworkProtocol.Response fetchResp = client.send(fetchReq);
            assertThat(fetchResp.isSuccess()).isTrue();

            List<String> messages = mapper.readValue(fetchResp.getPayload(), new TypeReference<List<String>>() {});
            assertThat(messages).containsExactly("PAYMENT_SETTLED_INR_5000");
        }
    }

    @Test
    @DisplayName("Real Socket: Idempotent sequence deduplication operates over TCP wire protocol")
    void testRealSocketIdempotencyDeduplication() throws Exception {
        try (BrokerClient client = new BrokerClient("127.0.0.1", leaderServer.getPort(), 5000)) {
            // First produce seq 0
            NetworkProtocol.Request req0 = new NetworkProtocol.Request(
                    NetworkProtocol.RequestType.PRODUCE, "net-orders", 0, "prod-idemp", "ORDER_MSG_1");
            req0.setSequenceNumber(0);
            req0.setAckMode("LEADER_ONLY");
            NetworkProtocol.Response r0 = client.send(req0);
            assertThat(r0.isSuccess()).isTrue();
            assertThat(r0.getAssignedOffset()).isEqualTo(0L);

            // Resend seq 0 over TCP socket simulating network retry
            NetworkProtocol.Response retryResp = client.send(req0);
            assertThat(retryResp.isSuccess()).isTrue();
            assertThat(retryResp.getPayload()).isEqualTo("DUPLICATE_SUPPRESSED");
            assertThat(retryResp.getAssignedOffset()).isEqualTo(0L);

            // Verify commit log has only 1 record
            Partition p = registry.getTopic("net-orders").getPartition(0);
            assertThat(p.getCommitLog().size()).isEqualTo(1);
        }
    }
}
