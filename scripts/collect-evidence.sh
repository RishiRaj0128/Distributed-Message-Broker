#!/usr/bin/env bash
# ==============================================================================
# scripts/collect-evidence.sh
#
# Runs the full Maven test suite followed by each of the three headline failure
# tests, recording real console output into docs/evidence/ alongside the UTC
# timestamp and JDK runtime version.
#
# Usage:
#   bash scripts/collect-evidence.sh
# ==============================================================================

set -euo pipefail

EVIDENCE_DIR="docs/evidence"
mkdir -p "${EVIDENCE_DIR}"

run_and_record() {
    local file_number="$1"
    local test_name="$2"
    local mvn_cmd="$3"
    local output_file="${EVIDENCE_DIR}/${file_number}-${test_name}.txt"

    echo "================================================================="
    echo "Running [${file_number}]: ${test_name}"
    echo "Command: ${mvn_cmd}"
    echo "================================================================="

    {
        echo "=== EVIDENCE RECORD ==="
        echo "Test Name:      ${test_name}"
        echo "Command:        ${mvn_cmd}"
        echo "UTC Start Time: $(date -u '+%Y-%m-%d %H:%M:%SZ')"
        echo "JDK Version:"
        java -version 2>&1
        echo "-----------------------------------------------------------------"
        echo ""
    } > "${output_file}"

    # Execute maven command, capture exit code, and append console output
    local exit_code=0
    ${mvn_cmd} >> "${output_file}" 2>&1 || exit_code=$?

    {
        echo ""
        echo "-----------------------------------------------------------------"
        echo "UTC End Time:   $(date -u '+%Y-%m-%d %H:%M:%SZ')"
        echo "Exit Code:      ${exit_code}"
        echo "=== END OF RECORD ==="
    } >> "${output_file}"

    echo "Saved evidence to ${output_file} (exit code: ${exit_code})"
}

# 1. Full automated test suite (32 tests across 12 suites)
run_and_record "01" "full-test-suite" "mvn clean test"

# 2. 50 consecutive failure cycles with zero data loss
run_and_record "02" "50-failure-cycles" "mvn test -Dtest=TimedRealFailoverStressTest#test50PlusSimulatedBrokerFailureCyclesZeroLoss"

# 3. Real wall-clock heartbeat failure detection (~530 ms)
run_and_record "03" "wall-clock-failover" "mvn test -Dtest=TimedRealFailoverStressTest#testRealClockHeartbeatFailoverTiming"

# 4. Split-brain prevention under asymmetric network partition
run_and_record "04" "split-brain-step-down" "mvn test -Dtest=ChaosFailureTest#testNetworkPartitionPreventsSplitBrain"

echo ""
echo "Evidence collection complete. Results saved in ${EVIDENCE_DIR}/"
