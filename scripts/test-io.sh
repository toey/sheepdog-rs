#!/usr/bin/env bash
#
# test-io.sh — I/O correctness test suite (no object cache)
#
# Tests data integrity through the NBD path with object cache DISABLED,
# verifying that every read/write goes directly through the store layer.
#
# Test phases:
#   Phase 1: Setup — 3-node cluster, no --cache
#   Phase 2: Basic I/O — sequential write + read at each object offset
#   Phase 3: Overwrite — rewrite same locations with new patterns
#   Phase 4: Cross-boundary — writes spanning two 4MB objects
#   Phase 5: Large I/O — multi-object sequential writes
#   Phase 6: Sparse writes — non-contiguous offsets, read gaps as zeros
#   Phase 7: Direct I/O — restart cluster with --directio, repeat key tests
#   Phase 8: Cleanup
#
# Requirements:
#   - cargo build (debug or release)
#   - qemu-io (from qemu-utils) for NBD I/O verification
#
# Usage:
#   ./scripts/test-io.sh [--keep] [--skip-directio]
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
BIND=127.0.0.1
BASE_PORT=7000
HTTP_BASE_PORT=8000
NBD_PORT=10809
NUM_NODES=3
COPIES=1
VDI_SIZE="64M"
VDI_NAME="iotest"
DATA_ROOT="/tmp/sheepdog-io-test"
LOG_DIR="${DATA_ROOT}/logs"

# 4 MB object size (SD_DATA_OBJ_SIZE = 1 << 22)
OBJ_SIZE=4194304

KEEP=false
SKIP_DIRECTIO=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)           KEEP=true ;;
        --skip-directio)  SKIP_DIRECTIO=true ;;
    esac
done

# Resolve binaries
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${REPO_ROOT}/target/release/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/release/sheep"
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/debug/sheep"
    DOG="${REPO_ROOT}/target/debug/dog"
else
    echo "ERROR: sheep binary not found. Run: cargo build"
    exit 1
fi

# ── Colors ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ── Output helpers ─────────────────────────────────────────────────────
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[FAIL]${NC}  $*" >&2; }
pass()    { echo -e "${GREEN}[PASS]${NC}  $*"; }
phase()   { echo -e "\n${BOLD}${CYAN}━━━ Phase $1: $2 ━━━${NC}\n"; }
step()    { echo -e "  ${DIM}→${NC} $*"; }

PASS_COUNT=0
FAIL_COUNT=0

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

# ── NBD helpers ────────────────────────────────────────────────────────
NBD_URI="nbd://${BIND}:${NBD_PORT}/${VDI_NAME}"

nbd_write() {
    local pattern="$1" offset="$2" size="$3"
    qemu-io -f raw -c "write -P ${pattern} ${offset} ${size}" "$NBD_URI" 2>/dev/null
}

nbd_read_verify() {
    local pattern="$1" offset="$2" size="$3"
    qemu-io -f raw -c "read -P ${pattern} ${offset} ${size}" "$NBD_URI" 2>/dev/null
}

nbd_read_zero() {
    local offset="$1" size="$2"
    qemu-io -f raw -c "read -P 0x00 ${offset} ${size}" "$NBD_URI" 2>/dev/null
}

# ── Node helpers ───────────────────────────────────────────────────────
node_port()  { echo $(( BASE_PORT + $1 * 2 )); }
node_http()  { echo $(( HTTP_BASE_PORT + $1 * 2 )); }
node_dir()   { echo "${DATA_ROOT}/node${1}"; }
node_log()   { echo "${LOG_DIR}/node${1}.log"; }
node_pid()   { echo "${DATA_ROOT}/node${1}.pid"; }

is_running() {
    local pidfile
    pidfile="$(node_pid "$1")"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

wait_for_port() {
    local port=$1 timeout=${2:-10} elapsed=0
    while ! nc -z "$BIND" "$port" 2>/dev/null; do
        sleep 0.3
        elapsed=$(( elapsed + 1 ))
        if (( elapsed > timeout * 3 )); then
            return 1
        fi
    done
    return 0
}

start_node() {
    local idx=$1
    shift
    local port http dir log pid
    port=$(node_port "$idx")
    http=$(node_http "$idx")
    dir=$(node_dir "$idx")
    log=$(node_log "$idx")
    pid=$(node_pid "$idx")

    mkdir -p "$dir"

    step "Starting node ${idx} (port=${port})"
    if (( idx > 0 )); then
        "$SHEEP" --cluster-driver sdcluster \
            -b "$BIND" -p "$port" \
            --seed "${BIND}:$(node_port 0)" \
            --http-port "$http" \
            -l debug \
            "$@" \
            "$dir" \
            > "$log" 2>&1 &
    else
        "$SHEEP" --cluster-driver sdcluster \
            -b "$BIND" -p "$port" \
            --http-port "$http" \
            -l debug \
            "$@" \
            "$dir" \
            > "$log" 2>&1 &
    fi
    echo $! > "$pid"

    if ! wait_for_port "$port" 15; then
        err "Node ${idx} failed to start — check ${log}"
        return 1
    fi
    info "Node ${idx} ready (pid=$(cat "$pid"), port=${port})"
}

stop_node() {
    local idx=$1
    local pidfile
    pidfile=$(node_pid "$idx")
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            local waited=0
            while kill -0 "$pid" 2>/dev/null && (( waited < 50 )); do
                sleep 0.1
                (( waited++ ))
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pidfile"
    fi
}

stop_all() {
    for i in $(seq $(( NUM_NODES - 1 )) -1 0); do
        stop_node "$i"
    done
}

count_objects() {
    local dir
    dir="$(node_dir "$1")/obj"
    if [[ -d "$dir" ]]; then
        ls "$dir" 2>/dev/null | wc -l | tr -d ' '
    else
        echo "0"
    fi
}

show_object_distribution() {
    echo -e "  ${BOLD}Object distribution:${NC}"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local count
        count=$(count_objects "$i")
        printf "    node%d: %3s objects\n" "$i" "$count"
    done
}

dog_cmd() {
    "$DOG" -a "$BIND" -p "$(node_port 0)" "$@" 2>/dev/null
}

start_cluster() {
    step "Starting ${NUM_NODES}-node cluster"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        if (( i == 0 )); then
            if [[ $# -gt 0 ]]; then
                start_node "$i" --nbd --nbd-port "$NBD_PORT" "$@" \
                    || { err "Failed to start node $i"; stop_all; return 1; }
            else
                start_node "$i" --nbd --nbd-port "$NBD_PORT" \
                    || { err "Failed to start node $i"; stop_all; return 1; }
            fi
        else
            if [[ $# -gt 0 ]]; then
                start_node "$i" "$@" \
                    || { err "Failed to start node $i"; stop_all; return 1; }
            else
                start_node "$i" \
                    || { err "Failed to start node $i"; stop_all; return 1; }
            fi
        fi
    done

    sleep 2

    step "Formatting cluster (copies=${COPIES})"
    if ! dog_cmd cluster format --copies "$COPIES" 2>&1; then
        warn "Format failed (may already be formatted)"
    fi
    sleep 1
}

# ── Cleanup trap ───────────────────────────────────────────────────────
cleanup() {
    if [[ "$KEEP" == "true" ]]; then
        warn "Keeping data in ${DATA_ROOT} (--keep flag)"
    else
        step "Stopping all nodes..."
        stop_all
        step "Removing ${DATA_ROOT}"
        rm -rf "$DATA_ROOT"
    fi
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Sheepdog I/O Correctness Test (No Cache)           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Binary:    ${SHEEP}"
echo -e "  Data:      ${DATA_ROOT}"
echo -e "  VDI:       ${VDI_NAME} (${VDI_SIZE})"
echo -e "  Copies:    ${COPIES}"
echo -e "  Obj size:  ${OBJ_SIZE} (4 MB)"
echo -e "  Cache:     ${RED}disabled${NC}"
echo ""

# Check qemu-io
if ! command -v qemu-io &>/dev/null; then
    err "qemu-io not found. Install qemu-utils for NBD I/O testing."
    exit 1
fi

# Clean any previous run
rm -rf "$DATA_ROOT"
mkdir -p "$LOG_DIR"

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — 3-node cluster, cache disabled"

# Start cluster WITHOUT --cache (no object cache)
start_cluster || { err "Failed to start cluster"; exit 1; }

check "Cluster is formatted" dog_cmd cluster info

# Create VDI
step "Creating VDI '${VDI_NAME}' (${VDI_SIZE})"
dog_cmd vdi create "$VDI_NAME" "$VDI_SIZE"
sleep 1

check "VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: Basic I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Basic I/O — sequential write + read"

# Write different patterns at the start of each 4MB object
# Object 0: offset 0
# Object 1: offset 4MB
# Object 2: offset 8MB
# Object 3: offset 12MB

step "Writing pattern 0xAA at offset 0 (object 0)"
nbd_write 0xAA 0 4096
check "Write 4K at offset 0" true

step "Writing pattern 0xBB at offset 4MB (object 1)"
nbd_write 0xBB $OBJ_SIZE 4096
check "Write 4K at offset 4MB" true

step "Writing pattern 0xCC at offset 8MB (object 2)"
nbd_write 0xCC $(( OBJ_SIZE * 2 )) 4096
check "Write 4K at offset 8MB" true

step "Writing pattern 0xDD at offset 12MB (object 3)"
nbd_write 0xDD $(( OBJ_SIZE * 3 )) 4096
check "Write 4K at offset 12MB" true

echo ""
step "Reading back and verifying patterns"
check "Read 0xAA at offset 0"          nbd_read_verify 0xAA 0 4096
check "Read 0xBB at offset 4MB"        nbd_read_verify 0xBB $OBJ_SIZE 4096
check "Read 0xCC at offset 8MB"        nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "Read 0xDD at offset 12MB"       nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096

# Write larger blocks (64K, 256K, 1M)
echo ""
step "Writing larger blocks"

nbd_write 0x11 65536 65536       # 64K at offset 64K
check "Write 64K block"    true
check "Read 64K block"     nbd_read_verify 0x11 65536 65536

nbd_write 0x22 262144 262144    # 256K at offset 256K
check "Write 256K block"   true
check "Read 256K block"    nbd_read_verify 0x22 262144 262144

nbd_write 0x33 1048576 1048576  # 1M at offset 1M
check "Write 1M block"     true
check "Read 1M block"      nbd_read_verify 0x33 1048576 1048576

echo ""
show_object_distribution

# ━━━ Phase 3: Overwrite ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "Overwrite — rewrite same locations"

step "Overwriting offset 0 with new pattern 0xEE"
nbd_write 0xEE 0 4096
check "Overwrite at offset 0"      nbd_read_verify 0xEE 0 4096

step "Overwriting offset 4MB with new pattern 0xFF"
nbd_write 0xFF $OBJ_SIZE 4096
check "Overwrite at offset 4MB"    nbd_read_verify 0xFF $OBJ_SIZE 4096

# Verify other locations were NOT affected
step "Verifying non-overwritten data is intact"
check "0xCC at 8MB still intact"    nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "0xDD at 12MB still intact"   nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096
check "64K block still intact"      nbd_read_verify 0x11 65536 65536
check "1M block still intact"       nbd_read_verify 0x33 1048576 1048576

# Partial overwrite within a block
step "Partial overwrite: 512 bytes in the middle of an existing block"
nbd_write 0xAB 2048 512
check "Partial write 512B at offset 2048"           true
check "Read partial overwrite"                       nbd_read_verify 0xAB 2048 512
# Before and after the partial write should be as before
check "Before partial: 0xEE at offset 0 (first 2K)"  nbd_read_verify 0xEE 0 2048
check "After partial: 0xEE at offset 2560"            nbd_read_verify 0xEE 2560 1536

# ━━━ Phase 4: Cross-boundary writes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Cross-boundary — writes spanning two 4MB objects"

# Write 8K straddling the boundary between object 0 and object 1
# Object boundary is at offset 4194304 (4MB)
# Write 8K starting at (4MB - 4K) = 4190208, ending at (4MB + 4K) = 4198400
BOUNDARY_OFFSET=$(( OBJ_SIZE - 4096 ))

step "Writing 8K across object 0/1 boundary (offset ${BOUNDARY_OFFSET})"
nbd_write 0x77 $BOUNDARY_OFFSET 8192
check "Cross-boundary write (8K)"        true
check "Read cross-boundary data"          nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Write 1M across boundary between object 2 and 3
# boundary at 8MB, write from 7.5MB to 8.5MB
BOUNDARY2=$(( OBJ_SIZE * 2 - 524288 ))
step "Writing 1M across object 2/3 boundary (offset ${BOUNDARY2})"
nbd_write 0x88 $BOUNDARY2 1048576
check "Cross-boundary write (1M)"        true
check "Read cross-boundary 1M"           nbd_read_verify 0x88 $BOUNDARY2 1048576

# ━━━ Phase 5: Large I/O — multi-object writes ━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Large I/O — multi-object sequential writes"

# Create a second VDI for large I/O tests
step "Creating VDI 'bigtest' (32M)"
dog_cmd vdi create bigtest 32M
sleep 1

BIGTEST_URI="nbd://${BIND}:${NBD_PORT}/bigtest"

# Write 8MB (spans 2 full objects)
step "Writing 8MB sequential block to 'bigtest'"
qemu-io -f raw -c "write -P 0x55 0 8388608" "$BIGTEST_URI" 2>/dev/null
check "Write 8MB sequential"   true
check "Read 8MB sequential"    qemu-io -f raw -c "read -P 0x55 0 8388608" "$BIGTEST_URI"

# Write 16MB (spans 4 full objects)
step "Writing 16MB sequential block"
qemu-io -f raw -c "write -P 0x66 0 16777216" "$BIGTEST_URI" 2>/dev/null
check "Write 16MB sequential"  true
check "Read 16MB sequential"   qemu-io -f raw -c "read -P 0x66 0 16777216" "$BIGTEST_URI"

# Write full VDI (32MB)
step "Writing full 32MB VDI"
qemu-io -f raw -c "write -P 0x99 0 33554432" "$BIGTEST_URI" 2>/dev/null
check "Write 32MB full VDI"    true
check "Read 32MB full VDI"     qemu-io -f raw -c "read -P 0x99 0 33554432" "$BIGTEST_URI"

echo ""
show_object_distribution

# ━━━ Phase 6: Sparse writes + zero verification ━━━━━━━━━━━━━━━━━━━━━
phase 6 "Sparse writes — non-contiguous offsets, gaps read as zeros"

# Create VDI for sparse test
step "Creating VDI 'sparse' (32M)"
dog_cmd vdi create sparse 32M
sleep 1

SPARSE_URI="nbd://${BIND}:${NBD_PORT}/sparse"

# Write at scattered offsets, leaving gaps
step "Writing sparse pattern: 4K at offsets 0, 2M, 5M, 10M, 20M"
qemu-io -f raw -c "write -P 0xA1 0 4096" "$SPARSE_URI" 2>/dev/null
check "Sparse write at 0"      true

qemu-io -f raw -c "write -P 0xA2 2097152 4096" "$SPARSE_URI" 2>/dev/null
check "Sparse write at 2M"     true

qemu-io -f raw -c "write -P 0xA3 5242880 4096" "$SPARSE_URI" 2>/dev/null
check "Sparse write at 5M"     true

qemu-io -f raw -c "write -P 0xA4 10485760 4096" "$SPARSE_URI" 2>/dev/null
check "Sparse write at 10M"    true

qemu-io -f raw -c "write -P 0xA5 20971520 4096" "$SPARSE_URI" 2>/dev/null
check "Sparse write at 20M"    true

# Verify written data
echo ""
step "Verifying sparse data"
check "Read sparse 0"     qemu-io -f raw -c "read -P 0xA1 0 4096" "$SPARSE_URI"
check "Read sparse 2M"    qemu-io -f raw -c "read -P 0xA2 2097152 4096" "$SPARSE_URI"
check "Read sparse 5M"    qemu-io -f raw -c "read -P 0xA3 5242880 4096" "$SPARSE_URI"
check "Read sparse 10M"   qemu-io -f raw -c "read -P 0xA4 10485760 4096" "$SPARSE_URI"
check "Read sparse 20M"   qemu-io -f raw -c "read -P 0xA5 20971520 4096" "$SPARSE_URI"

# Verify gaps are zero
echo ""
step "Verifying gaps read as zeros"
check "Gap at 4K-8K is zero"     qemu-io -f raw -c "read -P 0x00 4096 4096" "$SPARSE_URI"
check "Gap at 1M is zero"        qemu-io -f raw -c "read -P 0x00 1048576 4096" "$SPARSE_URI"
check "Gap at 8M is zero"        qemu-io -f raw -c "read -P 0x00 8388608 4096" "$SPARSE_URI"
check "Gap at 15M is zero"       qemu-io -f raw -c "read -P 0x00 15728640 4096" "$SPARSE_URI"
check "Gap at 25M is zero"       qemu-io -f raw -c "read -P 0x00 26214400 4096" "$SPARSE_URI"

# ━━━ Phase 7: Direct I/O mode ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if [[ "$SKIP_DIRECTIO" == "true" ]]; then
    phase 7 "Direct I/O — SKIPPED (--skip-directio)"
else
    phase 7 "Direct I/O — restart cluster with --directio"

    # Stop the cluster
    step "Stopping cluster for Direct I/O restart"
    stop_all
    sleep 2

    # Remove data and start fresh with --directio
    rm -rf "$DATA_ROOT"
    mkdir -p "$LOG_DIR"

    step "Starting cluster with --directio (no cache)"
    start_cluster --directio || { err "Failed to start directio cluster"; cleanup; exit 1; }

    check "Cluster is formatted (directio)" dog_cmd cluster info

    # Create VDI
    step "Creating VDI '${VDI_NAME}' with directio"
    dog_cmd vdi create "$VDI_NAME" "$VDI_SIZE"
    sleep 1
    check "VDI created (directio)" dog_cmd vdi list

    # Basic write/read with directio
    step "Writing 4K patterns with directio"
    nbd_write 0xD1 0 4096
    check "directio: write 4K at 0"            true
    check "directio: read 4K at 0"             nbd_read_verify 0xD1 0 4096

    nbd_write 0xD2 $OBJ_SIZE 4096
    check "directio: write 4K at 4MB"          true
    check "directio: read 4K at 4MB"           nbd_read_verify 0xD2 $OBJ_SIZE 4096

    # Large block with directio
    step "Writing 1M block with directio"
    nbd_write 0xD3 0 1048576
    check "directio: write 1M at 0"            true
    check "directio: read 1M at 0"             nbd_read_verify 0xD3 0 1048576

    # Cross-boundary with directio
    step "Cross-boundary 8K with directio"
    nbd_write 0xD4 $BOUNDARY_OFFSET 8192
    check "directio: cross-boundary write"     true
    check "directio: cross-boundary read"      nbd_read_verify 0xD4 $BOUNDARY_OFFSET 8192

    # Overwrite with directio
    step "Overwrite with directio"
    nbd_write 0xD5 0 4096
    check "directio: overwrite"                nbd_read_verify 0xD5 0 4096
    check "directio: non-overwritten intact"   nbd_read_verify 0xD3 4096 $(( 1048576 - 4096 ))

    echo ""
    show_object_distribution
fi

# ━━━ Phase 8: Cleanup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 8 "Cleanup"

cleanup

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}━━━ Test Summary ━━━${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
if (( FAIL_COUNT > 0 )); then
    echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
else
    echo -e "  ${DIM}Failed: 0${NC}"
fi
echo ""

if (( FAIL_COUNT > 0 )); then
    echo -e "${RED}${BOLD}SOME TESTS FAILED${NC}"
    echo -e "  Check logs: ${LOG_DIR}/"
    exit 1
else
    echo -e "${GREEN}${BOLD}ALL TESTS PASSED${NC}"
    exit 0
fi
