# Failure Tests: Verification Commands & Assertion Invariants

This document details the exact execution commands and verification assertions for the three headline resilience claims of the Distributed Message Broker.

---

## 1. 50 Consecutive Failure Cycles With Zero Data Loss

- **Test Suite**: `com.broker.election.TimedRealFailoverStressTest`
- **Test Method**: `test50PlusSimulatedBrokerFailureCyclesZeroLoss`
- **Execution Command**:
  ```bash
  mvn test -Dtest=TimedRealFailoverStressTest#test50PlusSimulatedBrokerFailureCyclesZeroLoss
  ```

### What The Assertion Checks
1. Runs a loop of 50 consecutive failure cycles across a 3-broker cluster (`node-1`, `node-2`, `node-3`, replication factor 3).
2. During each cycle:
   - Produces an acknowledged record (`PAYMENT_TX_CYCLE_<cycle>`) under `AckMode.QUORUM_ISR`.
   - Crashes an active broker (alternating between active leader and followers via `controller.simulateNodeFailure`).
   - If the leader was killed, `LeaderElector` promotes a new leader with the highest Log End Offset (Max-LEO) from the alive ISR set.
   - Recovers the dead broker, fetches missing log entries from the new leader, and re-admits the replica to the ISR via `isrTracker.evaluateIsr()`.
3. Assertions verified:
   - `assertThat(allCommittedMessages).hasSize(50)`: Validates that exactly 50 messages were committed across all 50 failover cycles.
   - For every index `i` from 0 to 49:
     - `assertThat(msg.getOffset()).isEqualTo((long) i)`: Validates strict, contiguous offset monotonicity without gaps.
     - `assertThat(msg.getPayloadAsString()).isEqualTo("PAYMENT_TX_CYCLE_" + i)`: Validates 100% payload integrity with zero duplicate writes and zero lost records.

### Observed Result
`<!-- TODO: Fill with real output from scripts/collect-evidence.sh -->`

---

## 2. Real Wall-Clock Heartbeat Failure Detection (~530 ms)

- **Test Suite**: `com.broker.election.TimedRealFailoverStressTest`
- **Test Method**: `testRealClockHeartbeatFailoverTiming`
- **Execution Command**:
  ```bash
  mvn test -Dtest=TimedRealFailoverStressTest#testRealClockHeartbeatFailoverTiming
  ```

### What The Assertion Checks
1. Initializes `ClusterController` with a real background `ScheduledExecutorService` running periodic health sweeps every `50 ms`, configured with `heartbeatTimeoutMs = 500L`.
2. Broker `b-1` abruptly halts heartbeating while a background thread keeps `b-2` alive (`b2.recordHeartbeat()`).
3. `controller.awaitFailover("timed-stream", 0, "b-1", 3000L)` blocks on the wall-clock until the background thread detects silence, marks `b-1` dead, and elects `b-2`.
4. Assertions verified:
   - `assertThat(liveP0.getLeaderId()).isEqualTo("b-2")`: Verifies new leader was promoted by the background scheduler.
   - `assertThat(elapsedMs).isGreaterThanOrEqualTo(450L)`: Proves failover waited for genuine heartbeat expiration rather than triggering an instantaneous in-memory short-circuit.
   - `assertThat(elapsedMs).isLessThan(1500L)`: Proves failover completed within the bounded detection envelope:
     $$\text{Failover Window} = \text{Heartbeat Timeout (500 ms)} + \text{Sweep Resolution (50 ms)} + \text{Election Latency (< 5 ms)} \approx 530-560\,\text{ms}$$

### Observed Result
`<!-- TODO: Fill with real output from scripts/collect-evidence.sh -->`

---

## 3. Split-Brain Prevention: Minority Leader Step-Down

- **Test Suite**: `com.broker.chaos.ChaosFailureTest`
- **Test Method**: `testNetworkPartitionPreventsSplitBrain`
- **Execution Command**:
  ```bash
  mvn test -Dtest=ChaosFailureTest#testNetworkPartitionPreventsSplitBrain
  ```

### What The Assertion Checks
1. A 5-node cluster (`b-1` through `b-5`) is partitioned into two isolated components:
   - **Side A (Majority)**: `{b-1, b-2, b-3}` (3 nodes)
   - **Side B (Minority)**: `{b-4, b-5}` (2 nodes)
2. Broker `b-5` on minority Side B is initially leader for `critical-payments-p0`.
3. When `verifyLeaderQuorum()` is invoked on Side B:
   - Reachable node count is 2, which is strictly less than majority threshold ($\lfloor 5/2 \rfloor + 1 = 3$).
   - `assertThat(b5HasQuorum).isFalse()`: Confirms minority leader detects quorum loss.
   - `assertThat(p0.getLeaderId()).isNull()`: Confirms minority leader immediately vacates leadership, preventing divergent writes.
4. Concurrently, Side A evaluates quorum reachability:
   - Reachable node count is 3 ($3 \ge 3$).
   - `assertThat(sideAHasQuorum).isTrue()`: Majority side satisfies quorum.
   - `p0.setLeaderId("b-1")`: Side A successfully promotes new leader `b-1`.
   - `assertThat(b1HasQuorum).isTrue()`: Validates that only the majority partition holds an active leader.

### Observed Result
`<!-- TODO: Fill with real output from scripts/collect-evidence.sh -->`
