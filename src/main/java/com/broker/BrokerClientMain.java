package com.broker;

import com.broker.net.BrokerClient;
import com.broker.net.NetworkProtocol;

/**
 * Standalone executable CLI client to interact with live BrokerNode processes over TCP sockets.
 *
 * Usage:
 *   # Query partition metadata
 *   java -cp target/... com.broker.BrokerClientMain 127.0.0.1 9092 metadata <topic> <partitionId>
 *
 *   # Produce a message over TCP
 *   java -cp target/... com.broker.BrokerClientMain 127.0.0.1 9092 produce <topic> <partitionId> <producerId> <payload>
 *
 *   # Fetch messages over TCP
 *   java -cp target/... com.broker.BrokerClientMain 127.0.0.1 9092 fetch <topic> <partitionId> <startOffset>
 */
public class BrokerClientMain {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: BrokerClientMain <host> <port> <command> [args...]");
            System.out.println("Commands:");
            System.out.println("  metadata <topic> <partitionId>");
            System.out.println("  produce  <topic> <partitionId> <producerId> <payload>");
            System.out.println("  fetch    <topic> <partitionId> <startOffset>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String command = args[2].toLowerCase();

        try (BrokerClient client = new BrokerClient(host, port, 5000)) {
            client.connect();

            switch (command) {
                case "metadata" -> {
                    String topic = args.length > 3 ? args[3] : "juspay-payment-authorizations";
                    int partition = args.length > 4 ? Integer.parseInt(args[4]) : 0;

                    NetworkProtocol.Request req = new NetworkProtocol.Request(
                            NetworkProtocol.RequestType.METADATA, topic, partition, "cli-client", null);
                    NetworkProtocol.Response resp = client.send(req);

                    System.out.println("\n--- Cluster Partition Metadata ---");
                    System.out.println("Status:       " + (resp.isSuccess() ? "SUCCESS" : "ERROR"));
                    System.out.println("Leader Node:  " + resp.getLeaderId());
                    System.out.println("Leader Port:  " + resp.getLeaderPort());
                    System.out.println("Leader Epoch: " + resp.getAssignedOffset());
                }

                case "produce" -> {
                    String topic = args.length > 3 ? args[3] : "juspay-payment-authorizations";
                    int partition = args.length > 4 ? Integer.parseInt(args[4]) : 0;
                    String producerId = args.length > 5 ? args[5] : "cli-producer";
                    String payload = args.length > 6 ? args[6] : "TEST_PAYLOAD_" + System.currentTimeMillis();

                    NetworkProtocol.Request req = new NetworkProtocol.Request(
                            NetworkProtocol.RequestType.PRODUCE, topic, partition, producerId, payload);
                    req.setSequenceNumber(0);
                    req.setAckMode("QUORUM_ISR");

                    NetworkProtocol.Response resp = client.send(req);
                    System.out.println("\n--- Message Produce Response ---");
                    System.out.println("Status:           " + (resp.isSuccess() ? "SUCCESS" : "ERROR"));
                    if (resp.isSuccess()) {
                        System.out.println("Committed Offset: " + resp.getAssignedOffset());
                        System.out.println("Response Payload: " + resp.getPayload());
                    } else {
                        System.out.println("Error Message:    " + resp.getErrorMessage());
                    }
                }

                case "fetch" -> {
                    String topic = args.length > 3 ? args[3] : "juspay-payment-authorizations";
                    int partition = args.length > 4 ? Integer.parseInt(args[4]) : 0;
                    long offset = args.length > 5 ? Long.parseLong(args[5]) : 0L;

                    NetworkProtocol.Request req = new NetworkProtocol.Request(
                            NetworkProtocol.RequestType.FETCH, topic, partition, "cli-consumer", null);
                    req.setOffset(offset);

                    NetworkProtocol.Response resp = client.send(req);
                    System.out.println("\n--- Partition Fetch Response ---");
                    System.out.println("Status:         " + (resp.isSuccess() ? "SUCCESS" : "ERROR"));
                    System.out.println("High Watermark: " + resp.getAssignedOffset());
                    System.out.println("Records JSON:   " + resp.getPayload());
                }

                default -> System.out.println("Unknown command: " + command);
            }
        } catch (Exception e) {
            System.err.println("TCP Connection failed to " + host + ":" + port + " -> " + e.getMessage());
            System.exit(1);
        }
    }
}
