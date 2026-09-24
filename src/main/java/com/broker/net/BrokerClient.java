package com.broker.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Real TCP socket client communicating with BrokerServer nodes.
 */
public class BrokerClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String host;
    private final int port;
    private final int timeoutMs;
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;

    public BrokerClient(String host, int port, int timeoutMs) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
    }

    public BrokerClient(String host, int port) {
        this(host, port, 3000);
    }

    public synchronized void connect() throws IOException {
        if (socket == null || socket.isClosed()) {
            socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            out = new DataOutputStream(socket.getOutputStream());
            in = new DataInputStream(socket.getInputStream());
        }
    }

    public synchronized NetworkProtocol.Response send(NetworkProtocol.Request request) throws IOException {
        connect();
        String jsonReq = mapper.writeValueAsString(request);
        NetworkProtocol.writeFrame(out, jsonReq);

        String jsonResp = NetworkProtocol.readFrame(in);
        return mapper.readValue(jsonResp, NetworkProtocol.Response.class);
    }

    @Override
    public synchronized void close() {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }
}
