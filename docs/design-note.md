# Design Note: Distributed Message Broker Architecture

**Author**: Rishi Raj  
**Target Profile**: Platform Engineer / Site Reliability Engineer (SRE)  
**Date**: November 2026  
**Status**: Architecture & Implementation Design Document  

---

## 1. Problem Statement

Financial transactions and event-driven payment pipelines (such as payment switches and settlement orchestrators) require event streaming infrastructure with strict ordering, high availability, and zero data loss under crash failures. When broker nodes crash, networks partition, or replicas experience garbage collection (GC) pauses, standard message queues risk split-brain leadership, uncommitted record overwrites, or cascading producer timeouts.

This project implements an event streaming broker designed to solve these failure modes through quorum-based partition replication, dynamic In-Sync Replica (ISR) tracking, and zero-loss leader election.

---

## 2. System Requirements

- **Durability**: Configurable acknowledgment guarantees (`LEADER_ONLY`, `QUORUM_ISR`, `ALL_ISR`) ensuring messages are committed before acknowledging producers.
- **High Availability**: Sub-second failover when partition leaders crash, bounding downtime while maintaining continuous ingress.
- **Replication Freshness**: Automatic detection and expulsion of degraded followers to prevent slow replicas from stalling write pipelines.
- **Split-Brain Immunity**: Prevention of concurrent active leaders during asymmetric network partitions.
- **Hermetic Testability**: Reproducible automated verification of resilience invariants under simulated network chaos and true wall-clock heartbeat failures.

---

## 3. Options Considered & Architectural Tradeoffs

### 3.1 Replication Acknowledgment: ISR Quorum vs. All-Replica Acks
- **All-Replica ACKs (`ALL_ISR`)**: Requires 100% of replicas to acknowledge every message. While offering maximal durability, write latency degrades to the slowest follower. A single lagging node stalls all incoming writes.
- **Quorum-Based ISR (`QUORUM_ISR`)**: Requires acknowledgment from a strict majority ($\lfloor N_{\text{ISR}} / 2 \rfloor + 1$) of the current In-Sync Replica set. Replicas lagging behind by $> 10$ offsets or $> 2000\,\text{ms}$ are expelled by `IsrTracker`.
- **Decision**: Adopt `QUORUM_ISR` as the primary operational mode. This decouples producer throughput from transient node jitter while mathematically guaranteeing that any subsequent leader elected from the ISR contains all acknowledged messages.

### 3.2 Leader Election: Highest Log End Offset (Max-LEO) vs. Arbitrary Promotion
- **Arbitrary / Round-Robin Election**: Promotes any surviving replica upon leader failure. This introduces silent data truncation if the elected node lagged behind the dead leader.
- **Highest-LEO Election (`LeaderElector`)**: Filters candidate replicas strictly to alive members of the ISR set, evaluating each candidate's replicated Log End Offset (LEO). The candidate with the highest LEO is elected, advancing the leader epoch.
- **Decision**: Adopt Max-LEO leader election. Combined with quorum write ACKs, this ensures that mid-stream leader crashes yield zero acknowledged message loss in the simulated cluster.

### 3.3 Coordination & Metadata: In-Process Controller vs. Distributed Raft Quorum
- **Distributed Raft Quorum (KRaft / etcd)**: Multi-node distributed state machine where controllers vote on metadata mutations. Requires separate consensus networking, term elections, and snapshotting.
- **In-Process Controller (`ClusterController`)**: Centralized controller monitoring node heartbeats via a `ScheduledExecutorService` (100 ms sweep interval) and triggering elections in memory.
- **Decision for Current Scope**: Implement the centralized in-process controller to cleanly isolate consensus algorithms and provide deterministic test execution without the operational overhead of managing external consensus clusters.

---

## 4. Architectural Decisions

1. **Two-Tier Model**:
   - *Tier 1 (In-Process Consensus)*: Fast, deterministic simulation of ISR shrinkage, leader failover, and split-brain fencing executing in $< 7\,\text{seconds}$ across 32 unit tests.
   - *Tier 2 (Wire Networking)*: Real TCP `ServerSocket` listeners (`BrokerServer`) and clients (`BrokerClient`) handling length-prefixed JSON frames (`[4-byte length][payload]`) over loopback or container networks.
2. **Split-Brain Fencing**: Leaders evaluate quorum reachability ($\lfloor N / 2 \rfloor + 1$) before accepting writes. In an asymmetric partition, an isolated leader detects quorum loss and immediately yields leadership (`leaderId = null`).
3. **Producer & Consumer Idempotency**: Sequence numbering on producers suppresses retried duplicates at the broker commit log; message key tracking on consumers prevents duplicate downstream processing.

---

## 5. Architectural Risks

- **GC Pause Sensitivity**: Under severe JVM stop-the-world pauses, background heartbeat sweeps may falsely detect broker failure, triggering unnecessary leader elections.
- **Blocking Socket Exhaustion**: Standard thread-per-connection `ServerSocket` pools (`Executors.newCachedThreadPool`) can exhaust OS file descriptors or thread stacks under thousands of concurrent client connections.
- **Heap Bloat Under Consumer Lag**: In-memory message buffers can saturate JVM heap if consumers fall behind producers without active flow control backpressure.

---

## 6. Honest Known Limitations

1. **In-Memory Commit Log**: `CommitLog.java` stores entries in an in-memory `ArrayList<Message>` protected by `ReentrantReadWriteLock`. There is no persistent disk storage or Write-Ahead Log (WAL). An abrupt process termination loses un-replicated messages.
2. **Single In-Process Controller & Registry**: `ClusterController` and `ClusterRegistry` run inside a single JVM. Separate `BrokerNodeMain` OS processes do **not** replicate to each other over TCP or form a multi-node cluster across process boundaries. Each process maintains its own independent in-process state.
3. **Blocking Socket I/O**: Networking uses standard blocking `ServerSocket` with worker thread pools rather than non-blocking Java NIO (`Selector`) or Netty epoll.
4. **Metrics Exposure**: `BrokerMetrics.java` defines Prometheus instrumentation (counters, timers, gauges) verified in `ObservabilityMetricsTest`, but is **not** wired into `BrokerNodeMain` or exposed via an HTTP `/metrics` endpoint.

---

## 7. What I Would Change for Production

1. **Persistent Write-Ahead Log (WAL)**: Implement memory-mapped append-only segment files (`FileChannel` and OS page cache) with index files and zero-copy network streaming (`sendfile`).
2. **Raft-Based Distributed Metadata Quorum**: Replace the single in-process controller with a distributed Raft consensus group (KRaft architecture) where metadata logs are replicated across controller nodes.
3. **Non-Blocking Network Layer (Java NIO / Netty)**: Transition from blocking sockets to an event-driven Netty architecture with zero-copy buffer pooling and epoll transport.
4. **Inter-Broker Wire Replication**: Implement TCP socket replication streams between independent broker container instances across Docker / Kubernetes networks.
5. **Prometheus HTTP Scrape Endpoint**: Wire `BrokerMetrics` into an embedded lightweight HTTP server (e.g., `com.sun.net.httpserver.HttpServer`) exposing `/metrics` on port 9090.
