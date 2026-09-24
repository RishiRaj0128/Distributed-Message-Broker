# Post-Mortem Incident Log: Distributed Message Broker Chaos & Resilience Engineering

> **Context**: In mission-critical payment infrastructure (such as Juspay's payment orchestrator and switch), network partitions, broker crashes, and replication delays are inevitable. This document logs three deliberately induced catastrophic failure scenarios, their root causes in distributed systems theory, the architectural defenses implemented in this broker, and the automated test proofs validating zero data loss and split-brain immunity.

---

## Incident 1: Asymmetric Network Partition & Split-Brain Prevention (CAP Theorem Tradeoff)

### The Failure Induced
- **Scenario**: A 5-broker cluster (`b-1`, `b-2`, `b-3`, `b-4`, `b-5`) experienced an asymmetric network partition isolating the network into two disconnected components:
  - **Component A (Majority Quorum)**: `{b-1, b-2, b-3}` (3 nodes)
  - **Component B (Minority Partition)**: `{b-4, b-5}` (2 nodes)
- **Observed Behavior Without Guard**:
  - The previous partition leader residing in Component B (`b-5`) continued accepting producer writes without awareness of the partition.
  - Concurrently, Component A elected a new leader (`b-1`), which also accepted writes.
  - **Split-Brain Disaster**: Two leaders concurrently diverged partition logs at the same offset, permanently corrupting financial transaction ordering and rendering reconciliation mathematically impossible.

### Root Cause
In asynchronous networks, an isolated leader cannot distinguish between follower node failure and network partition silence. Without strict majority quorum fencing, an isolated leader continues writing un-replicated records, violating linearizability.

### The Engineering Defense
1. **Majority Quorum Fencing via `NetworkPartitionGuard`**:
   - Before acknowledging any write or claiming leadership, a broker verifies connectivity to a strict majority quorum of partition replicas:
     $$\text{Quorum} = \left\lfloor \frac{N}{2} \right\rfloor + 1$$
     For $N = 5$, $\text{Quorum} = 3$.
   - When `b-5` tests quorum reachability, it finds only 2 reachable nodes ($2 < 3$). It immediately yields leadership and sets `partition.leaderId = null`.
2. **Deterministic Failover on Majority Side**:
   - Component A possesses 3 reachable nodes ($3 \ge 3$), successfully meets quorum, increments the partition leader epoch, and elects a new leader with zero split-brain risk.
3. **Automated Verification**:
   - Validated in [`ChaosFailureTest.java:testNetworkPartitionPreventsSplitBrain`](file:///d:/Distributed%20Message%20Broker/src/test/java/com/broker/chaos/ChaosFailureTest.java).

---

## Incident 2: Follower Replication Lag & In-Sync Replica (ISR) Degradation Under Ingress Spike

### The Failure Induced
- **Scenario**: Under an ingress write burst on topic `payments-events-p0`, follower broker `b-2` experienced severe GC pauses / disk I/O throttling, causing its replication log offset to fall 8 offsets behind leader `b-1` (threshold = 5).
- **Observed Behavior Without Fix**:
  - The lagging follower remained in the ISR set.
  - When the leader crashed, the lagging follower was elected leader and overwrote 8 committed payment transactions with subsequent producer writes, losing financial ledger entries.

### Root Cause
Treating statically configured replica lists as election candidates without tracking replication freshness introduces stale leaders that truncate committed history during failovers.

### The Engineering Defense
1. **Dynamic ISR Tracking via `IsrTracker`**:
   - Tracks replication offset lag $\Delta_{\text{offset}} = \text{LEO}_{\text{leader}} - \text{LEO}_{\text{follower}}$ and time delta $\Delta t$ since last fetch.
   - If $\Delta_{\text{offset}} > 5$ or $\Delta t > 2000\,\text{ms}$, the follower is immediately expelled from the ISR.
2. **Prometheus Alerting on Under-Replicated Partitions**:
   - Expulsion triggers `broker_under_replicated_partitions = 1`, alerting infrastructure operators via the Grafana dashboard before outages occur.
3. **Automated Catch-up & Re-admission**:
   - When `b-2` recovers and fetches up to within acceptable lag, `IsrTracker` re-admits it to the ISR automatically.
4. **Automated Verification**:
   - Validated in [`ReplicationTest.java:testLaggingFollowerExpulsionFromIsr`](file:///d:/Distributed%20Message%20Broker/src/test/java/com/broker/replication/ReplicationTest.java) and [`ObservabilityMetricsTest.java:testUnderReplicatedPartitionsGauge`](file:///d:/Distributed%20Message%20Broker/src/test/java/com/broker/metrics/ObservabilityMetricsTest.java).

---

## Incident 3: Mid-Stream Leader Crash With Zero Acknowledged Data Loss

### The Failure Induced
- **Scenario**: The leader broker process (`b-1`) was killed (`SIGKILL`) mid-write-stream while processing active payment transactions.
- **Observed Behavior Without Fix**:
  - In-flight writes failed permanently with unhandled connection errors.
  - Consumers hung indefinitely waiting on the dead leader.
  - Data committed under `ack=all` was orphaned or unreadable.

### Root Cause
Client producers and consumers maintained hardcoded broker connections rather than dynamic cluster topology discovery, lacking failover discovery protocols.

### The Engineering Defense
1. **Highest-LEO Leader Election via `LeaderElector`**:
   - Evaluates remaining alive members strictly within the In-Sync Replica (ISR) set.
   - The candidate with the highest replicated log end offset (LEO) is promoted, mathematically guaranteeing that no acknowledged message is missing from the new leader.
2. **Dynamic Metadata Discovery (`MetadataService`)**:
   - Clients query `getPartitionMetadata(topic, partitionId)` to discover the promoted leader and epoch upon encountering connection errors.
3. **Bounded Failover Window**:
   - Failover and promotion complete in bounded time (< 50ms in testing), and producer writes resume seamlessly.
4. **Automated Verification**:
   - Validated in [`LeaderElectionTest.java:testMidStreamLeaderCrashWithZeroAcknowledgedMessageLoss`](file:///d:/Distributed%20Message%20Broker/src/test/java/com/broker/election/LeaderElectionTest.java).
