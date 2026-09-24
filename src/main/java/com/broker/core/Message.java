package com.broker.core;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable log record within a partition commit log.
 */
public final class Message {
    private final String id;
    private final String key;
    private final byte[] payload;
    private final Map<String, String> headers;
    private final Instant timestamp;
    private final String producerId;
    private final long sequenceNumber;
    private final long offset;

    public Message(String key, byte[] payload) {
        this(UUID.randomUUID().toString(), key, payload, Collections.emptyMap(), Instant.now(), null, -1, -1);
    }

    public Message(String id, String key, byte[] payload, Map<String, String> headers,
                   Instant timestamp, String producerId, long sequenceNumber, long offset) {
        this.id = Objects.requireNonNullElseGet(id, () -> UUID.randomUUID().toString());
        this.key = key;
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.headers = headers != null ? Map.copyOf(headers) : Collections.emptyMap();
        this.timestamp = timestamp != null ? timestamp : Instant.now();
        this.producerId = producerId;
        this.sequenceNumber = sequenceNumber;
        this.offset = offset;
    }

    public Message withOffset(long assignedOffset) {
        return new Message(this.id, this.key, this.payload, this.headers, this.timestamp,
                this.producerId, this.sequenceNumber, assignedOffset);
    }

    public String getId() { return id; }
    public String getKey() { return key; }
    public byte[] getPayload() { return payload.clone(); }
    public String getPayloadAsString() { return new String(payload); }
    public Map<String, String> getHeaders() { return headers; }
    public Instant getTimestamp() { return timestamp; }
    public String getProducerId() { return producerId; }
    public long getSequenceNumber() { return sequenceNumber; }
    public long getOffset() { return offset; }

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", key='" + key + '\'' +
                ", offset=" + offset +
                ", producerId='" + producerId + '\'' +
                ", seq=" + sequenceNumber +
                ", ts=" + timestamp +
                '}';
    }
}
