package com.broker.net;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Length-prefixed framing and JSON payload protocol over TCP sockets.
 * Format on wire:
 * [4 bytes integer: length N][N bytes UTF-8 JSON payload]
 */
public class NetworkProtocol {

    public enum RequestType {
        PRODUCE,
        FETCH,
        HEARTBEAT,
        METADATA,
        REPLICATE
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Request {
        @JsonProperty("type")
        private RequestType type;

        @JsonProperty("topic")
        private String topic;

        @JsonProperty("partitionId")
        private int partitionId;

        @JsonProperty("senderId")
        private String senderId;

        @JsonProperty("payload")
        private String payload;

        @JsonProperty("offset")
        private long offset;

        @JsonProperty("sequenceNumber")
        private long sequenceNumber;

        @JsonProperty("ackMode")
        private String ackMode;

        public Request() {}

        public Request(RequestType type, String topic, int partitionId, String senderId, String payload) {
            this.type = type;
            this.topic = topic;
            this.partitionId = partitionId;
            this.senderId = senderId;
            this.payload = payload;
        }

        public RequestType getType() { return type; }
        public void setType(RequestType type) { this.type = type; }

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }

        public int getPartitionId() { return partitionId; }
        public void setPartitionId(int partitionId) { this.partitionId = partitionId; }

        public String getSenderId() { return senderId; }
        public void setSenderId(String senderId) { this.senderId = senderId; }

        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }

        public long getOffset() { return offset; }
        public void setOffset(long offset) { this.offset = offset; }

        public long getSequenceNumber() { return sequenceNumber; }
        public void setSequenceNumber(long sequenceNumber) { this.sequenceNumber = sequenceNumber; }

        public String getAckMode() { return ackMode; }
        public void setAckMode(String ackMode) { this.ackMode = ackMode; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Response {
        @JsonProperty("success")
        private boolean success;

        @JsonProperty("errorMessage")
        private String errorMessage;

        @JsonProperty("assignedOffset")
        private long assignedOffset;

        @JsonProperty("payload")
        private String payload;

        @JsonProperty("leaderId")
        private String leaderId;

        @JsonProperty("leaderPort")
        private int leaderPort;

        public Response() {}

        public static Response ok(long assignedOffset, String payload) {
            Response r = new Response();
            r.success = true;
            r.assignedOffset = assignedOffset;
            r.payload = payload;
            return r;
        }

        public static Response error(String errorMessage) {
            Response r = new Response();
            r.success = false;
            r.errorMessage = errorMessage;
            return r;
        }

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getErrorMessage() { return errorMessage; }
        public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

        public long getAssignedOffset() { return assignedOffset; }
        public void setAssignedOffset(long assignedOffset) { this.assignedOffset = assignedOffset; }

        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }

        public String getLeaderId() { return leaderId; }
        public void setLeaderId(String leaderId) { this.leaderId = leaderId; }

        public int getLeaderPort() { return leaderPort; }
        public void setLeaderPort(int leaderPort) { this.leaderPort = leaderPort; }
    }

    public static void writeFrame(DataOutputStream out, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    public static String readFrame(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 10_000_000) {
            throw new IOException("Frame size exceeds maximum threshold: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
