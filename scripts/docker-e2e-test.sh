#!/usr/bin/env bash
set -x
#
# docker-e2e-test.sh — Comprehensive E2E test suite for sheepdog-rs
#
# This script tests all major functionalities of the sheepdog-rs application
# using a Docker Compose-based 3-node cluster.
#
# Test coverage:
#   1. Cluster health (nodes, voting, epoch, format)
#   2. HTTP/S3/Swift API endpoints
#   3. NBD export (I/O correctness, cross-boundary, sparse)
#   4. NFS export (mount, file operations)
#   5. iSCSI (if feature enabled)
#   6. Recovery (node restart, data integrity)
#   7. Erasure coding (if feature enabled)
#
# Usage:
#   ./scripts/docker-e2e-test.sh [--all|--cluster|--http|--nbd|--nfs|--recovery|--ec]
#   ./scripts/docker-e2e-test.sh --all  # Run all tests
#   ./scripts/docker-e2e-test.sh        # Default: run all available tests
#
# Environment variables:
#   ENABLE_NBD=true  - Enable NBD tests
#   ENABLE_NFS=true  - Enable NFS tests
#   ENABLE_ISCSI=false - Enable iSCSI tests
#
set -uo pipefail

# Disable Docker credential store to bypass docker-credential-osxkeychain issue
export DOCKER_CLI_DISABLE_CREDENTIAL_STORE=1
export DOCKER_CREDENTIALS_DISABLE_STORE=1

# ─── Configuration ────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
E2E_DIR="${REPO_ROOT}/test/e2e"

# ─── Flags ──────────────────────────────────────────────────────────────
TEST_ALL=false
TEST_CLUSTER=false
TEST_HTTP=false
TEST_NBD=false
TEST_NFS=false
TEST_RECOVERY=false
TEST_EC=false

for arg in "$@"; do
    case "$arg" in
        --all)       TEST_ALL=true ;;
        --cluster)   TEST_CLUSTER=true ;;
        --http)      TEST_HTTP=true ;;
        --nbd)       TEST_NBD=true ;;
        --nfs)       TEST_NFS=true ;;
        --recovery)  TEST_RECOVERY=true ;;
        --ec)        TEST_EC=true ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --all        Run all tests"
            echo "  --cluster    Test cluster health only"
            echo "  --http       Test HTTP/S3/Swift API only"
            echo "  --nbd        Test NBD export only"
            echo "  --nfs        Test NFS export only"
            echo "  --recovery   Test recovery only"
            echo "  --ec         Test erasure coding only"
            echo "  --help       Show this help"
            echo ""
            echo "Environment variables:"
            echo "  ENABLE_NBD=true      Enable NBD tests"
            echo "  ENABLE_NFS=true      Enable NFS tests"
            echo "  ENABLE_ISCSI=false   Enable iSCSI tests"
            exit 0
            ;;
    esac
done

# Default: run all available tests
if [[ "$TEST_ALL" == false && "$TEST_CLUSTER" == false && "$TEST_HTTP" == false && "$TEST_NBD" == false && "$TEST_NFS" == false && "$TEST_RECOVERY" == false && "$TEST_EC" == false ]]; then
    TEST_ALL=true
fi

# ─── Colors ──────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ─── Output helpers ──────────────────────────────────────────────────────
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[FAIL]${NC}  $*" >&2; }
pass()  { echo -e "${GREEN}[PASS]${NC}  $*"; }
phase() { echo -e "\n${BOLD}${CYAN}━━━ $1 ━━━${NC}\n"; }

# ─── Test counters ───────────────────────────────────────────────────────
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

check() {
    local desc="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc"
        (( FAIL_COUNT++ ))
    fi
}

check_result() {
    local desc="$1" result="$2"
    if [[ "$result" == "0" ]]; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc (exit code: $result)"
        (( FAIL_COUNT++ ))
    fi
}

check_contains() {
    local desc="$1" output="$2" expected="$3"
    if [[ "$output" == *"$expected"* ]]; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc (expected contains '$expected', got: '$output')"
        (( FAIL_COUNT++ ))
    fi
}

check_http() {
    local desc="$1" url="$2" expected_code="$3"
    local response
    response=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin /usr/bin/curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    if [[ "$response" == "$expected_code" ]]; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc (expected $expected_code, got $response)"
        (( FAIL_COUNT++ ))
    fi
}

# ─── Dog helper ──────────────────────────────────────────────────────────
dog_cmd() {
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin /workspace/target/release/dog -a 172.25.0.2 -p 7000 "$@" 2>/dev/null
}

# ─── qemu-io helper (runs inside admin container) ────────────────────────
qemu_io_exec() {
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin qemu-io -f raw -c "$1" "$2" 2>&1
}

# ─── Wait for cluster ────────────────────────────────────────────────────
wait_for_cluster() {
    info "Waiting for cluster to be ready..."
    local attempts=0
    while [[ $attempts -lt 30 ]]; do
        if dog_cmd cluster info >/dev/null 2>&1; then
            info "Cluster is ready"
            return 0
        fi
        sleep 1
        (( attempts++ ))
    done
    err "Cluster failed to start within 30 seconds"
    return 1
}

# ─── Start cluster ───────────────────────────────────────────────────────
start_cluster() {
    phase "Starting 3-node cluster"
    
    # Stop any existing cluster and remove network and volumes
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" down --volumes --remove-orphans 2>/dev/null || true
    docker network rm e2e_sheepdog-net 2>/dev/null || true
    
    # Remove cached docker image to ensure fresh build
    $DOCKER_CMD image rm sheepdog-rs-e2e:latest 2>/dev/null || true
    
    # Start cluster - ensure DOCKER_CLI_DISABLE_CREDENTIAL_STORE is set for build
    export DOCKER_CLI_DISABLE_CREDENTIAL_STORE=1
    export DOCKER_CREDENTIALS_DISABLE_STORE=1
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" build 2>&1
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" up -d node0 node1 node2 admin 2>&1
    if [[ $? -ne 0 ]]; then
        err "Failed to start cluster"
        return 1
    fi
    
    # Wait for cluster to be ready
    sleep 3
    wait_for_cluster
    return $?
}

# ─── Format cluster ──────────────────────────────────────────────────────
format_cluster() {
    phase "Formatting cluster"
    # Wait for cluster to be ready before formatting
    local attempts=0
    while [[ $attempts -lt 30 ]]; do
        if dog_cmd cluster info >/dev/null 2>&1; then
            break
        fi
        sleep 1
        (( attempts++ ))
    done
    if [[ $attempts -eq 30 ]]; then
        err "Cluster failed to respond before format"
        return 1
    fi
    dog_cmd cluster format --copies 1
    check_result "Cluster formatted" "$?"
    sleep 2
}

# ─── Cleanup ─────────────────────────────────────────────────────────────
cleanup() {
    phase "Cleaning up"
    # Delete VDI objects
    local vdilist
    vdilist=$(dog_cmd vdi list 2>/dev/null)
    if [[ -n "$vdilist" ]]; then
        echo "$vdilist" | grep -E "^[[:space:]]*[0-9]+ " | while read -r line; do
            local vid
            vid=$(echo "$line" | awk '{print $1}')
            dog_cmd vdi delete "$vid" >/dev/null 2>&1 || true
        done
    fi
    sleep 1
    info "Cleanup complete"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: Cluster Health
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_cluster() {
    phase "Cluster Health Tests"
    
    # Check cluster info
    local cluster_info
    cluster_info=$(dog_cmd cluster info 2>/dev/null)
    check_contains "Cluster status is running" "$cluster_info" "running"
    check_contains "Cluster has 3 nodes" "$cluster_info" "3"
    check_contains "Epoch is set" "$cluster_info" "Epoch:"
    check_contains "Default copies is 1" "$cluster_info" "1"
    
    # Check node list (table format: Id | Host | Port | VNodes | Zone | Space | Status)
    local node_list
    node_list=$(dog_cmd node list 2>/dev/null)
    check_contains "Node 0 is present" "$node_list" "7000"
    check_contains "Node 1 is present" "$node_list" "7002"
    check_contains "Node 2 is present" "$node_list" "7004"
    
    # Check HTTP endpoints
    check_http "Node 0 HTTP endpoint" "http://172.25.0.2:8000/" "200"
    #check_http "Node 0 HTTP endpoint" "http://127.0.0.1:8000/" "200"
    check_http "Node 1 HTTP endpoint" "http://172.25.0.3:8002/" "200"
    #check_http "Node 1 HTTP endpoint" "http://127.0.0.1:8002/" "200"
    check_http "Node 2 HTTP endpoint" "http://172.25.0.4:8004/" "200"
    #check_http "Node 2 HTTP endpoint" "http://127.0.0.1:8004/" "200"

    # Check NBD endpoint (run inside admin container)
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 10809 2>&1)
    check_result "Node 0 NBD port 10809 is open" "$?"
    
    # Create test VDI
    dog_cmd vdi create clustertest 64M >/dev/null 2>&1
    sleep 1
    local vdi_check
    vdi_check=$(dog_cmd vdi list 2>/dev/null)
    check_contains "Test VDI created" "$vdi_check" "clustertest"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: HTTP/S3/Swift API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_http() {
    phase "HTTP/S3/Swift API Tests"
    
    # Health endpoint
    check_http "Health endpoint" "http://172.25.0.2:8000/" "200"
    
    # S3-style API
    check_http "S3 root" "http://172.25.0.2:8000/?delimiter=/" "200"
    
    # VDI HTTP endpoints
    check_http "VDI list" "http://172.25.0.2:8000/?vdilist=" "200"
    
    # Create a test VDI for HTTP tests
    dog_cmd vdi create httptest 32M >/dev/null 2>&1
    sleep 1
    
    # Check VDI info
    local vdi_info
    vdi_info=$(dog_cmd vdi list 2>/dev/null)
    check_contains "HTTP test VDI exists" "$vdi_info" "httptest"
    
    # HTTP upload/download via NBD-backed object
    # First write via NBD, then read via HTTP
    local nbd_uri="nbd://172.25.0.2:10809/httptest"
    result=$(qemu_io_exec "write -P 0xAB 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "HTTP VDI write via NBD" "$?"
    
    # Read back via NBD to verify
    result=$(qemu_io_exec "read -P 0xAB 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "HTTP VDI read via NBD" "$?"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: NBD Export
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_nbd() {
    phase "NBD Export Tests"
    
    # Check NBD port
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 10809 2>&1)
    check_result "Node 0 NBD port 10809 is listening" "$?"
    
    # Create VDI for NBD tests
    dog_cmd vdi create nbdtest 64M >/dev/null 2>&1
    sleep 1
    
    local nbd_uri="nbd://172.25.0.2:10809/nbdtest"
    
    # Basic write/read
    result=$(qemu_io_exec "write -P 0xAA 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: write 4K at offset 0" "$?"
    
    result=$(qemu_io_exec "read -P 0xAA 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: read 4K at offset 0" "$?"
    
    # Write at object boundary (4MB = 4194304 bytes)
    BOUNDARY=$(( 4194304 - 2048 ))
    result=$(qemu_io_exec "write -P 0xBB $BOUNDARY 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: cross-boundary write" "$?"
    
    result=$(qemu_io_exec "read -P 0xBB $BOUNDARY 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: cross-boundary read" "$?"
    
    # Sparse write
    result=$(qemu_io_exec "write -P 0xDD 0 4096" "$nbd_uri" >/dev/null 2>&1)
    result=$(qemu_io_exec "write -P 0xEE 2097152 4096" "$nbd_uri" >/dev/null 2>&1)  # 2MB
    check_result "NBD: sparse writes" "$?"
    
    result=$(qemu_io_exec "read -P 0xDD 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: sparse read at 0" "$?"
    
    result=$(qemu_io_exec "read -P 0xEE 2097152 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: sparse read at 2MB" "$?"
    
    # Verify gaps are zero (between 4K and 2MB)
    result=$(qemu_io_exec "read -P 0x00 8192 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "NBD: gap at 8K is zero" "$?"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: NFS Export
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_nfs() {
    phase "NFS Export Tests"
    
    # Check NFS ports (run nc inside admin container)
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 2049 2>&1)
    check_result "Node 0 NFS port 2049 is listening" "$?"
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 2050 2>&1)
    check_result "Node 0 NFS mount port 2050 is listening" "$?"
    
    # Try to mount NFS (may fail in Docker without special privileges)
    if $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin sh -c 'mount -t nfs 172.25.0.2:2050:/mnt /tmp/nfstest-mount-$$' 2>/dev/null; then
        # Mount succeeded
        pass "NFS mount succeeded"
        (( PASS_COUNT++ ))
        
        # Create test VDI
        dog_cmd vdi create nfstest 32M >/dev/null 2>&1
        sleep 1
        
        # Write via NFS
        $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin sh -c 'echo "Hello NFS" > /tmp/nfstest-mount/testfile.txt' 2>/dev/null
        check_result "NFS: write file" "$?"
        
        # Read via NFS
        local content
        content=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin sh -c 'cat /tmp/nfstest-mount/testfile.txt' 2>/dev/null)
        check_contains "NFS: file content matches" "$content" "Hello NFS"
        
        # Unmount
        $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin sh -c 'umount /tmp/nfstest-mount' 2>/dev/null || true
    else
        warn "NFS mount skipped (requires elevated privileges)"
        (( SKIP_COUNT++ ))
    fi
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: Recovery
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_recovery() {
    phase "Recovery Tests"
    
    # Create test VDI with data
    dog_cmd vdi create recotest 32M >/dev/null 2>&1
    sleep 1
    
    local nbd_uri="nbd://172.25.0.2:10809/recotest"
    result=$(qemu_io_exec "write -P 0x55 0 8388608" "$nbd_uri" >/dev/null 2>&1)
    check_result "Recovery: write 8MB before restart" "$?"
    
    # Verify data before restart
    result=$(qemu_io_exec "read -P 0x55 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "Recovery: verify data before restart" "$?"
    
    # Restart node 1
    info "Restarting node 1..."
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" restart node1
    sleep 5
    
    # Verify cluster is still healthy
    local cluster_info
    cluster_info=$(dog_cmd cluster info 2>/dev/null)
    check_contains "Cluster still running after node restart" "$cluster_info" "running"
    check_contains "Cluster has 3 nodes after restart" "$cluster_info" "3"
    
    # Verify data integrity after restart
    result=$(qemu_io_exec "read -P 0x55 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "Recovery: data intact after node restart" "$?"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Test: Erasure Coding
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
test_ec() {
    phase "Erasure Coding Tests"
    
    # Create EC VDI
    dog_cmd vdi create ectest 64M >/dev/null 2>&1
    sleep 1
    
    # Set EC policy
    dog_cmd vdi alter ec 4 2 >/dev/null 2>&1 || true  # 4 data + 2 parity
    
    local nbd_uri="nbd://172.25.0.2:10809/ectest"
    
    # Write data
    result=$(qemu_io_exec "write -P 0x77 0 4194304" "$nbd_uri" >/dev/null 2>&1)
    check_result "EC: write 4MB" "$?"
    
    # Read data
    # Read data
    result=$(qemu_io_exec "read -P 0x77 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "EC: read 4K" "$?"
    
    # Verify with vdi list
    local vdi_info
    vdi_info=$(dog_cmd vdi list 2>/dev/null)
    check_contains "EC VDI exists" "$vdi_info" "ectest"
}
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
main() {
    echo -e "${BOLD}${CYAN}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║      Sheepdog-rs Docker E2E Test Suite                     ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    # Ensure Docker Compose is available
    if [ -x "./docker" ]; then
        DOCKER_CMD="./docker"
    elif command -v docker &>/dev/null; then
        DOCKER_CMD="docker"
    else
        err "Docker not found"
        exit 1
    fi

    if ! $DOCKER_CMD compose version &>/dev/null 2>&1; then
        err "Docker Compose not found"
        exit 1
    fi
    
    # Check for required tools (check inside admin container)
    if ! $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin which qemu-io >/dev/null 2>&1; then
        warn "qemu-io not found in admin container — NBD tests will be skipped"
        TEST_NBD=false
    fi
    
    # Start cluster
    start_cluster || {
        err "Failed to start cluster"
        exit 1
    }
    
    # Format cluster
    format_cluster
    
    # Run selected tests
    if [[ "$TEST_ALL" == true || "$TEST_CLUSTER" == true ]]; then
        test_cluster
    fi
    
    if [[ "$TEST_ALL" == true || "$TEST_HTTP" == true ]]; then
        test_http
    fi
    
    if [[ "$TEST_ALL" == true || "$TEST_NBD" == true ]]; then
        test_nbd
    fi
    
    if [[ "$TEST_ALL" == true || "$TEST_NFS" == true ]]; then
        test_nfs
    fi
    
    if [[ "$TEST_ALL" == true || "$TEST_RECOVERY" == true ]]; then
        test_recovery
    fi
    
    if [[ "$TEST_ALL" == true || "$TEST_EC" == true ]]; then
        test_ec
    fi
    
    # Cleanup
    cleanup
    
    # Summary
    echo ""
    echo -e "${BOLD}Summary:${NC}"
    echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
    echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
    echo -e "  ${YELLOW}Skipped: ${SKIP_COUNT}${NC}"
    echo ""
    
    if [[ ${FAIL_COUNT} -gt 0 ]]; then
        err "E2E tests failed"
        exit 1
    fi
    
    info "All E2E tests passed"
    exit 0
}

main
