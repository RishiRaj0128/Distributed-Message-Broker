package com.broker.core;

import java.util.Objects;

/**
 * Broker node representation in a multi-DC / simulated cluster environment.
 */
public class BrokerNode {
    public enum NodeStatus {
        ALIVE,
        DEGRADED,
        DEAD
    }

    private final String id;
    private final String host;
    private final int port;
    private final String zone; // Simulated Datacenter / Availability Zone
    private volatile NodeStatus status;
    private volatile long lastHeartbeatTimestamp;

    public BrokerNode(String id, String host, int port, String zone) {
        this.id = Objects.requireNonNull(id, "broker id cannot be null");
        this.host = Objects.requireNonNull(host, "host cannot be null");
        this.port = port;
        this.zone = zone != null ? zone : "default-zone";
        this.status = NodeStatus.ALIVE;
        this.lastHeartbeatTimestamp = System.currentTimeMillis();
    }

    public String getId() { return id; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getZone() { return zone; }
    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }

    public long getLastHeartbeatTimestamp() { return lastHeartbeatTimestamp; }
    public void recordHeartbeat() { this.lastHeartbeatTimestamp = System.currentTimeMillis(); }

    public boolean isAlive() {
        return this.status == NodeStatus.ALIVE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerNode that = (BrokerNode) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "BrokerNode{" +
                "id='" + id + '\'' +
                ", endpoint='" + host + ":" + port + '\'' +
                ", zone='" + zone + '\'' +
                ", status=" + status +
                '}';
    }
}
