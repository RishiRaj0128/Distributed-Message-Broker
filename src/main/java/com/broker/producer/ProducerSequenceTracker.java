package com.broker.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks producer IDs and sequence numbers per partition to enforce idempotent writes.
 * Rejects duplicate sequence numbers on retry and detects out-of-order sequence gaps.
 */
public class ProducerSequenceTracker {
    private static final Logger log = LoggerFactory.getLogger(ProducerSequenceTracker.class);

    public record SequenceKey(String producerId, String topic, int partitionId) {
        public SequenceKey {
            Objects.requireNonNull(producerId, "producerId cannot be null");
            Objects.requireNonNull(topic, "topic cannot be null");
        }
    }

    public record DedupResult(boolean isDuplicate, long assignedOffset) {}

    // Key -> last committed sequence number
    private final Map<SequenceKey, Long> lastCommittedSequence = new ConcurrentHashMap<>();
    // Key + Sequence -> committed offset (so retried produce requests get the original offset)
    private final Map<String, Long> committedOffsetsBySeq = new ConcurrentHashMap<>();

    /**
     * Checks sequence validity before writing.
     *
     * @return DedupResult. If isDuplicate=true, caller must return assignedOffset and skip log append.
     * @throws IllegalStateException if sequence gap is detected.
     */
    public synchronized DedupResult checkAndValidate(String producerId, String topic, int partitionId, long sequenceNumber) {
        if (producerId == null || sequenceNumber < 0) {
            // Non-idempotent write, pass through
            return new DedupResult(false, -1L);
        }

        SequenceKey key = new SequenceKey(producerId, topic, partitionId);
        Long lastSeq = lastCommittedSequence.get(key);

        if (lastSeq == null) {
            // First message from this producer for this partition
            if (sequenceNumber != 0) {
                throw new IllegalStateException(String.format(
                        "Out of order sequence: Producer %s started with seq %d, expected 0 on %s-p%d",
                        producerId, sequenceNumber, topic, partitionId));
            }
            return new DedupResult(false, -1L);
        }

        if (sequenceNumber <= lastSeq) {
            // Duplicate detected (retry after ack timeout)
            String dedupKey = key + "#" + sequenceNumber;
            long originalOffset = committedOffsetsBySeq.getOrDefault(dedupKey, -1L);
            log.warn("Duplicate message suppressed for producer {} on {}-p{} (seq={}, lastCommitted={})",
                    producerId, topic, partitionId, sequenceNumber, lastSeq);
            return new DedupResult(true, originalOffset);
        }

        if (sequenceNumber > lastSeq + 1) {
            // Sequence gap: missing previous message
            throw new IllegalStateException(String.format(
                    "Out of order sequence gap: Producer %s sent seq %d, expected %d on %s-p%d",
                    producerId, sequenceNumber, lastSeq + 1, topic, partitionId));
        }

        return new DedupResult(false, -1L);
    }

    /**
     * Commits the sequence number and associates it with the assigned partition offset.
     */
    public synchronized void recordCommittedSequence(String producerId, String topic, int partitionId,
                                                     long sequenceNumber, long assignedOffset) {
        if (producerId == null || sequenceNumber < 0) return;

        SequenceKey key = new SequenceKey(producerId, topic, partitionId);
        lastCommittedSequence.put(key, sequenceNumber);
        String dedupKey = key + "#" + sequenceNumber;
        committedOffsetsBySeq.put(dedupKey, assignedOffset);
    }

    public Long getLastSequence(String producerId, String topic, int partitionId) {
        return lastCommittedSequence.get(new SequenceKey(producerId, topic, partitionId));
    }

    public void clear() {
        lastCommittedSequence.clear();
        committedOffsetsBySeq.clear();
    }
}
