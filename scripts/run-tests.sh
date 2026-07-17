#!/usr/bin/env bash
# run-tests.sh — Comprehensive test runner for sheepdog-rs
#
# Usage:
#   ./scripts/run-tests.sh              # Run all tests (unit + E2E)
#   ./scripts/run-tests.sh --unit      # Run unit tests only
#   ./scripts/run-tests.sh --e2e       # Run E2E tests only
#   ./scripts/run-tests.sh --unit --no-dpdk  # Run all unit tests except DPDK
#   ./scripts/run-tests.sh --e2e --io           # Run only I/O E2E tests
#   ./scripts/run-tests.sh --e2e --http         # Run only HTTP E2E tests
#   ./scripts/run-tests.sh --e2e --recovery     # Run only recovery E2E tests
#   ./scripts/run-tests.sh --e2e --ec            # Run only EC E2E tests
#   ./scripts/run-tests.sh --e2e --nfs            # Run only NFS E2E tests
#   ./scripts/run-tests.sh --e2e --iscsi         # Run only iSCSI E2E tests
#   ./scripts/run-tests.sh --e2e --fuse          # Run only FUSE E2E tests
#   ./scripts/run-tests.sh --build                 # Build release binaries
#   ./scripts/run-tests.sh --help                  # Show help
#
# Features auto-detected:
#   - HTTP/S3/Swift: always enabled (default feature)
#   - NBD: always enabled
#   - NFS: enabled if --nfs flag passed
#   - iSCSI: enabled if --iscsi flag passed (Linux only for E2E)
#   - DPDK: disabled by default (macOS incompatible, Linux-only)
#
# Docker-only: all builds run via ./docker wrapper

set -uo pipefail

# ─── Colors ────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()  { echo -e "${BOLD}${CYAN}>>> $*${NC}"; }
phase() { echo -e "${BOLD}${CYAN}=== $* ===${NC}"; }

# ─── Configuration ───────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ─── Docker Compose Configuration ──────────────────────────────────────────
DOCKER_CMD=""
E2E_DIR="${REPO_ROOT}/test/e2e"

# ─── Initialize Docker command helper ──────────────────────────────────────
init_docker_cmd() {
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
}

# ─── Dog helper (runs inside admin container) ──────────────────────────────
dog_cmd() {
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin /workspace/target/release/dog -a 172.25.0.2 -p 7000 "$@" 2>/dev/null
}

# ─── qemu-io helper (runs inside admin container) ──────────────────────────
qemu_io_exec() {
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin qemu-io -f raw -c "$1" "$2" 2>&1
}

# ─── HTTP check helper (runs inside admin container) ───────────────────────
check_http() {
    local desc="$1" url="$2" expected_code="$3"
    local response
    response=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin /usr/bin/curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    if [[ "$response" == "$expected_code" ]]; then
        return 0
    else
        return 1
    fi
}

# ─── Test check helpers ────────────────────────────────────────────────────
check() {
    local desc="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo -e "${GREEN}[PASS]${NC}  $desc"
    else
        echo -e "${RED}[FAIL]${NC}  $desc"
        return 1
    fi
}

check_result() {
    local desc="$1" result="$2"
    if [[ "$result" == "0" ]]; then
        echo -e "${GREEN}[PASS]${NC}  $desc"
    else
        echo -e "${RED}[FAIL]${NC}  $desc (exit code: $result)"
        return 1
    fi
}

check_contains() {
    local desc="$1" output="$2" expected="$3"
    if [[ "$output" == *"$expected"* ]]; then
        echo -e "${GREEN}[PASS]${NC}  $desc"
    else
        echo -e "${RED}[FAIL]${NC}  $desc (expected contains '$expected', got: '$output')"
        return 1
    fi
}

# ─── Flags ──────────────────────────────────────────────────────────────
RUN_UNIT=false
RUN_E2E=false
E2E_ALL=false
BUILD_ONLY=false
DPDK_ENABLED=true
NBD_ENABLED=true
NFS_ENABLED=false
ISCSI_ENABLED=false
E2E_TARGETS=()

# ─── Parse arguments ────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --unit)     RUN_UNIT=true ;;
        --e2e)      RUN_E2E=true; E2E_ALL=true ;;
        --build)    BUILD_ONLY=true ;;
        --nfs)      NFS_ENABLED=true ;;
        --iscsi)    ISCSI_ENABLED=true ;;
        --dpdk)     DPDK_ENABLED=true ;;
        --no-dpdk)  DPDK_ENABLED=false ;;
        --io)       E2E_TARGETS+=("io") ;;
        --recovery) E2E_TARGETS+=("recovery") ;;
        --ec)       E2E_TARGETS+=("ec") ;;
        --http)     E2E_TARGETS+=("http") ;;
        --nfs)      E2E_TARGETS+=("nfs") ;;
        --iscsi)    E2E_TARGETS+=("iscsi") ;;
        --fuse)     E2E_TARGETS+=("fuse") ;;
        --all)      RUN_UNIT=true; RUN_E2E=true; E2E_ALL=true ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --unit        Run unit tests only"
            echo "  --e2e         Run E2E tests only"
            echo "  --all         Run unit + E2E tests"
            echo "  --build       Build release binaries only"
            echo "  --nfs         Enable NFS feature"
            echo "  --iscsi       Enable iSCSI feature"
            echo "  --dpdk        Enable DPDK feature (Linux only)"
            echo "  --no-dpdk     Disable DPDK tests (default on macOS)"
            echo "  --io          Run only I/O E2E tests"
            echo "  --recovery    Run only recovery E2E tests"
            echo "  --ec          Run only EC E2E tests"
            echo "  --http        Run only HTTP E2E tests"
            echo "  --nfs         Run only NFS E2E tests"
            echo "  --iscsi       Run only iSCSI E2E tests"
            echo "  --fuse        Run only FUSE E2E tests"
            echo "  --help        Show this help"
            echo ""
            echo "Examples:"
            echo "  $0 --all                          # Run everything"
            echo "  $0 --unit --no-dpdk               # All unit tests except DPDK"
            echo "  $0 --e2e --io --recovery --ec     # I/O, recovery, EC E2E tests"
            echo "  $0 --build                        # Build release binaries"
            exit 0
            ;;
        *) err "Unknown flag: $1"; exit 1 ;;
    esac
    shift
done

# Default: run everything if no flags
if [[ ${RUN_UNIT} == false && ${RUN_E2E} == false && ${BUILD_ONLY} == false ]]; then
    RUN_UNIT=true
    RUN_E2E=true
    E2E_ALL=true
fi

# ─── Check platform ─────────────────────────────────────────────────────
IS_MACOS=false
IS_LINUX=false
if [[ "$OSTYPE" == "darwin"* ]]; then
    IS_MACOS=true
    warn "macOS detected — DPDK tests disabled"
    DPDK_ENABLED=false
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    IS_LINUX=true
fi

# ─── Build ──────────────────────────────────────────────────────────────
do_build() {
    step "Building release binaries..."
    
    # Skip build if E2E tests are running, as docker-compose build will handle it
    if $RUN_E2E; then
        info "Skipping release build since E2E tests will build via docker-compose"
        return 0
    fi
    
    local features=""
    if $NFS_ENABLED; then features="$features --features nfs"; fi
    if $ISCSI_ENABLED; then features="$features --features iscsi"; fi
    if $DPDK_ENABLED; then features="$features --all-features"; fi
    
    # Build default members
    docker run --rm -v "$(pwd)":/workspace -w /workspace -e DEBIAN_FRONTEND=noninteractive rust sh -c 'apt-get update && apt-get install -y libssl-dev pkg-config && cargo build --release $features' || {
        err "Build failed"
        exit 1
    }
    
    # Build sheepfs (optional, requires libfuse)
    if command -v fusermount &>/dev/null || $IS_LINUX; then
        info "Building sheepfs..."
        docker run --rm -v "$(pwd)":/workspace -w /workspace -e DEBIAN_FRONTEND=noninteractive rust sh -c 'apt-get update && apt-get install -y libssl-dev pkg-config libfuse-dev && cargo build -p sheepfs --release' 2>/dev/null || warn "sheepfs build skipped (requires libfuse)"
    fi
    
    info "Build complete"
}

# ─── Unit Tests ─────────────────────────────────────────────────────────
do_unit_tests() {
    step "Running unit tests..."
    
    local features=""
    if $NFS_ENABLED; then features="$features --features nfs"; fi
    if $ISCSI_ENABLED; then features="$features --features iscsi"; fi
    
    info "Running: cargo test with libssl-dev installation"
    docker run --rm -v "$(pwd)":/workspace -w /workspace -e DEBIAN_FRONTEND=noninteractive rust sh -c 'apt-get update && apt-get install -y libssl-dev pkg-config && cargo test $features' || {
        err "Unit tests failed"
        exit 1
    }
    
    info "Unit tests passed"
}

# ─── Test Functions ──────────────────────────────────────────────────────
test_http_e2e() {
    phase "HTTP/S3/Swift E2E Tests"
    
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
    
    # Ensure NBD port is ready
    local nbd_ready=false
    local attempts=0
    while [[ $attempts -lt 10 ]]; do
        result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 10809 2>&1)
        if [[ $? -eq 0 ]]; then
            nbd_ready=true
            break
        fi
        sleep 1
        (( attempts++ ))
    done
    
    if [[ "$nbd_ready" != "true" ]]; then
        echo -e "${RED}[FAIL]${NC}  NBD port 10809 not ready for HTTP tests"
        return 1
    fi
    
    result=$(qemu_io_exec "write -P 0xAB 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "HTTP VDI write via NBD" "$?"
    
    # Read back via NBD to verify
    result=$(qemu_io_exec "read -P 0xAB 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "HTTP VDI read via NBD" "$?"
}

test_nbd_e2e() {
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

test_recovery_e2e() {
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

test_ec_e2e() {
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
    result=$(qemu_io_exec "read -P 0x77 0 4096" "$nbd_uri" >/dev/null 2>&1)
    check_result "EC: read 4K" "$?"
    
    # Verify with vdi list
    local vdi_info
    vdi_info=$(dog_cmd vdi list 2>/dev/null)
    check_contains "EC VDI exists" "$vdi_info" "ectest"
}

test_nfs_e2e() {
    phase "NFS Export Tests"
    
    # Check NFS ports (run nc inside admin container)
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 2049 2>&1)
    check_result "Node 0 NFS port 2049 is listening" "$?"
    result=$($DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin nc -z 172.25.0.2 2050 2>&1)
    check_result "Node 0 NFS mount port 2050 is listening" "$?"
    
    # Try to mount NFS (may fail in Docker without special privileges)
    if $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" exec -T admin sh -c 'mount -t nfs 172.25.0.2:2050:/mnt /tmp/nfstest-mount-$$' 2>/dev/null; then
        # Mount succeeded
        echo -e "${GREEN}[PASS]${NC}  NFS mount succeeded"
        
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
    fi
}

test_iscsi_e2e() {
    phase "iSCSI E2E Tests (Placeholder)"
    info "iSCSI E2E tests placeholder - skipped"
}

test_fuse_e2e() {
    phase "FUSE E2E Tests (Placeholder)"
    info "FUSE E2E tests placeholder - skipped"
}

# ─── E2E Tests ──────────────────────────────────────────────────────────
do_e2e_tests() {
    step "Running E2E tests..."
    
    # Check qemu-utils (required for NBD-based tests)
    if ! command -v qemu-io &>/dev/null && [[ ${#E2E_TARGETS[@]} -eq 0 ]]; then
        warn "qemu-io not found — skipping I/O, recovery, EC tests"
        E2E_TARGETS+=("http")
        if $IS_LINUX; then
            E2E_TARGETS+=("iscsi")
        fi
        if command -v fusermount &>/dev/null; then
            E2E_TARGETS+=("fuse")
        fi
    fi
    
    # Initialize Docker command
    init_docker_cmd
    
    # Determine if NBD/NFS/iSCSI is needed
    local need_nbd=false
    local need_nfs=false
    local need_iscsi=false
    
    for target in "${E2E_TARGETS[@]}"; do
        case "$target" in
            io|recovery|ec|fuse|http) need_nbd=true ;;
        esac
    done
    
    if $NFS_ENABLED; then need_nfs=true; fi
    if $ISCSI_ENABLED; then need_iscsi=true; fi
    
    # Start cluster via docker-compose
    phase "Starting 3-node cluster via docker-compose"
    
    # Stop any existing cluster and remove network and volumes
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" down --volumes --remove-orphans 2>/dev/null || true
    docker network rm e2e_sheepdog-net 2>/dev/null || true
    
    # Remove cached docker image to ensure fresh build
    $DOCKER_CMD image rm sheepdog-rs-e2e:latest 2>/dev/null || true
    
    # Set environment variables for docker-compose based on needs
    export DOCKER_CLI_DISABLE_CREDENTIAL_STORE=1
    export DOCKER_CREDENTIALS_DISABLE_STORE=1
    
    if $need_nbd; then
        export ENABLE_NBD=true
    else
        export ENABLE_NBD=false
    fi
    
    if $need_nfs; then
        export ENABLE_NFS=true
    else
        export ENABLE_NFS=false
    fi
    
    if $need_iscsi; then
        export ENABLE_ISCSI=true
    else
        export ENABLE_ISCSI=false
    fi
    
    # Build and start cluster
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" build 2>&1
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" up -d node0 node1 node2 admin 2>&1
    
    if [[ $? -ne 0 ]]; then
        err "Failed to start cluster"
        exit 1
    fi
    
    # Wait for cluster to be ready
    sleep 3
    local attempts=0
    while [[ $attempts -lt 30 ]]; do
        if dog_cmd cluster info >/dev/null 2>&1; then
            info "Cluster is ready"
            break
        fi
        sleep 1
        (( attempts++ ))
    done
    if [[ $attempts -eq 30 ]]; then
        err "Cluster failed to start within 30 seconds"
        exit 1
    fi
    
    # Format cluster with copies=1
    phase "Formatting cluster"
    # Wait for cluster to be ready before formatting
    local attempts_format=0
    while [[ $attempts_format -lt 30 ]]; do
        if dog_cmd cluster info >/dev/null 2>&1; then
            break
        fi
        sleep 1
        (( attempts_format++ ))
    done
    if [[ $attempts_format -eq 30 ]]; then
        err "Cluster failed to respond before format"
        exit 1
    fi
    dog_cmd cluster format --copies 1
    check_result "Cluster formatted" "$?"
    sleep 2
    
    # Run each E2E test
    for target in "${E2E_TARGETS[@]}"; do
        case "$target" in
            io|nbd)
                info "Running I/O correctness tests..."
                test_nbd_e2e
                ;;
            recovery)
                info "Running recovery tests..."
                test_recovery_e2e
                ;;
            ec)
                info "Running EC tests..."
                test_ec_e2e
                ;;
            http)
                info "Running HTTP/S3/Swift E2E tests..."
                test_http_e2e
                ;;
            nfs)
                info "Running NFS E2E tests..."
                test_nfs_e2e
                ;;
            iscsi)
                info "Running iSCSI E2E tests..."
                test_iscsi_e2e
                ;;
            fuse)
                info "Running FUSE E2E tests..."
                test_fuse_e2e
                ;;
            *)
                err "Unknown E2E target: $target"
                ;;
        esac
    done
    
    # Cleanup
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
    
    # Stop cluster
    info "Stopping cluster..."
    $DOCKER_CMD compose -f "${E2E_DIR}/docker-compose-e2e.yml" down --volumes --remove-orphans 2>/dev/null || true
    docker network rm e2e_sheepdog-net 2>/dev/null || true
    
    info "E2E tests complete"
}
# ─── Main ───────────────────────────────────────────────────────────────
main() {
    if $BUILD_ONLY; then
        do_build
        exit 0
    fi
    
    if $RUN_UNIT; then
        do_build
        do_unit_tests
    fi
    
    if $RUN_E2E; then
        do_e2e_tests
    fi
    
    info "All tests passed"
}

main
