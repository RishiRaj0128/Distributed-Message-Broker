package com.broker.replication;

/**
 * Acknowledgment levels for partition writes.
 *
 * Tradeoff Matrix:
 * - LEADER_ONLY (ack=1): Lowest write latency. Only the partition leader logs the record.
 *   Risk: If the leader crashes before followers fetch, the un-replicated record is permanently lost.
 *
 * - QUORUM_ISR (ack=majority): Balanced latency and safety. Requires a strict majority of the
 *   current In-Sync Replica (ISR) set to persist the record before acknowledging to producer.
 *   Resilience: Survives loss of minor partition replicas without data loss.
 *
 * - ALL_ISR (ack=-1 / all): Highest durability guarantee. Requires 100% of current ISR replicas
 *   to confirm write. Write latency is governed by the slowest replica (tail latency sensitivity).
 */
public enum AckMode {
    LEADER_ONLY,
    QUORUM_ISR,
    ALL_ISR
}
