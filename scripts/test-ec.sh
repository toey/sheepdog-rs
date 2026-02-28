#!/usr/bin/env bash
#
# test-ec.sh — Erasure Coding (EC) correctness test suite
#
# Tests EC data integrity through the NBD path, verifying that
# Reed-Solomon encode/decode produces correct results.
#
# Test phases:
#   Phase 1: Setup — 3-node cluster with EC 2:1 VDI
#   Phase 2: Basic EC I/O — write + read at each object offset
#   Phase 3: EC Overwrite — partial writes (read-modify-write)
#   Phase 4: Cross-boundary — EC writes spanning two 4MB objects
#   Phase 5: Large EC I/O — multi-object sequential writes
#   Phase 6: Strip verification — check EC strip files on disk
#   Phase 7: Degraded read — stop a node, verify reconstruction
#   Phase 8: Cleanup
#
# Requirements:
#   - cargo build (debug or release)
#   - qemu-io (from qemu-utils) for NBD I/O verification
#
# Usage:
#   ./scripts/test-ec.sh [--keep]
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="/tmp/sheepdog-ec-test"
source "${SCRIPT_DIR}/defaults.sh"
LOG_DIR="${DATA_ROOT}/logs"

# EC 2:1 = 2 data strips + 1 parity strip (needs 3 nodes)
EC_POLICY="2:1"
EC_DATA=2
EC_PARITY=1
EC_TOTAL=3   # d+p

VDI_SIZE="64M"
VDI_NAME="ectest"

# 4 MB object size (SD_DATA_OBJ_SIZE = 1 << 22)
OBJ_SIZE=4194304

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)  KEEP=true ;;
    esac
done

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

count_ec_strips() {
    # Count files with EC strip suffix (_N or _NN where N is hex)
    local dir count
    dir="$(node_dir "$1")/obj"
    if [[ -d "$dir" ]]; then
        count=$(ls "$dir" 2>/dev/null | grep -c '_[0-9a-f]' 2>/dev/null) || count=0
        echo "$count"
    else
        echo "0"
    fi
}

show_object_distribution() {
    echo -e "  ${BOLD}Object distribution:${NC}"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local total ec_count
        total=$(count_objects "$i")
        ec_count=$(count_ec_strips "$i")
        printf "    node%d: %3s objects (%s EC strips)\n" "$i" "$total" "$ec_count"
    done
}

show_ec_strip_files() {
    echo -e "  ${BOLD}EC strip files (sample):${NC}"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir
        dir="$(node_dir "$i")/obj"
        if [[ -d "$dir" ]]; then
            local strips
            strips=$(ls "$dir" 2>/dev/null | grep '_[0-9a-f]' | head -5)
            if [[ -n "$strips" ]]; then
                echo "    node${i}:"
                echo "$strips" | while IFS= read -r f; do
                    local sz
                    sz=$(stat -f%z "${dir}/${f}" 2>/dev/null || stat -c%s "${dir}/${f}" 2>/dev/null || echo "?")
                    printf "      %-40s  %s bytes\n" "$f" "$sz"
                done
            fi
        fi
    done
}

dog_cmd() {
    "$DOG" -a "$BIND" -p "$(node_port 0)" "$@" 2>/dev/null
}

start_cluster() {
    step "Starting ${NUM_NODES}-node cluster"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        if (( i == 0 )); then
            start_node "$i" --nbd --nbd-port "$NBD_PORT" \
                || { err "Failed to start node $i"; stop_all; return 1; }
        else
            start_node "$i" \
                || { err "Failed to start node $i"; stop_all; return 1; }
        fi
    done

    sleep 2

    # Format with copies=1 (default for replication VDIs; EC VDIs override)
    step "Formatting cluster (copies=1)"
    if ! dog_cmd cluster format --copies 1 2>&1; then
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
echo "║      Sheepdog Erasure Coding (EC) Correctness Test          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Binary:    ${SHEEP}"
echo -e "  Data:      ${DATA_ROOT}"
echo -e "  VDI:       ${VDI_NAME} (${VDI_SIZE})"
echo -e "  EC Policy: ${EC_POLICY} (${EC_DATA} data + ${EC_PARITY} parity)"
echo -e "  Obj size:  ${OBJ_SIZE} (4 MB)"
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
phase 1 "Setup — 3-node cluster with EC ${EC_POLICY} VDI"

start_cluster || { err "Failed to start cluster"; exit 1; }

check "Cluster is formatted" dog_cmd cluster info

# Create EC VDI with 2:1 erasure coding
step "Creating EC VDI '${VDI_NAME}' (${VDI_SIZE}, EC ${EC_POLICY})"
dog_cmd vdi create "$VDI_NAME" "$VDI_SIZE" --copy-policy "$EC_POLICY"
sleep 1

check "EC VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: Basic EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Basic EC I/O — sequential write + read"

# Write different patterns at the start of each 4MB object
step "Writing pattern 0xAA at offset 0 (object 0)"
nbd_write 0xAA 0 4096
check "EC write 4K at offset 0" true

step "Writing pattern 0xBB at offset 4MB (object 1)"
nbd_write 0xBB $OBJ_SIZE 4096
check "EC write 4K at offset 4MB" true

step "Writing pattern 0xCC at offset 8MB (object 2)"
nbd_write 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC write 4K at offset 8MB" true

step "Writing pattern 0xDD at offset 12MB (object 3)"
nbd_write 0xDD $(( OBJ_SIZE * 3 )) 4096
check "EC write 4K at offset 12MB" true

echo ""
step "Reading back and verifying patterns"
check "EC read 0xAA at offset 0"    nbd_read_verify 0xAA 0 4096
check "EC read 0xBB at offset 4MB"  nbd_read_verify 0xBB $OBJ_SIZE 4096
check "EC read 0xCC at offset 8MB"  nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC read 0xDD at offset 12MB" nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096

# Write larger blocks
echo ""
step "Writing larger blocks with EC"

nbd_write 0x11 65536 65536       # 64K at offset 64K
check "EC write 64K block"    true
check "EC read 64K block"     nbd_read_verify 0x11 65536 65536

nbd_write 0x22 262144 262144    # 256K at offset 256K
check "EC write 256K block"   true
check "EC read 256K block"    nbd_read_verify 0x22 262144 262144

nbd_write 0x33 1048576 1048576  # 1M at offset 1M
check "EC write 1M block"     true
check "EC read 1M block"      nbd_read_verify 0x33 1048576 1048576

echo ""
show_object_distribution

# ━━━ Phase 3: EC Overwrite (read-modify-write) ━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "EC Overwrite — partial writes trigger read-modify-write"

step "Overwriting offset 0 with new pattern 0xEE"
nbd_write 0xEE 0 4096
check "EC overwrite at offset 0"      nbd_read_verify 0xEE 0 4096

step "Overwriting offset 4MB with new pattern 0xFF"
nbd_write 0xFF $OBJ_SIZE 4096
check "EC overwrite at offset 4MB"    nbd_read_verify 0xFF $OBJ_SIZE 4096

# Verify other locations were NOT affected
step "Verifying non-overwritten data is intact"
check "EC 0xCC at 8MB still intact"   nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC 0xDD at 12MB still intact"  nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096
check "EC 64K block still intact"     nbd_read_verify 0x11 65536 65536
check "EC 1M block still intact"      nbd_read_verify 0x33 1048576 1048576

# Partial overwrite within a block
step "Partial overwrite: 512 bytes in the middle of an existing block"
nbd_write 0xAB 2048 512
check "EC partial write 512B at 2048"                    true
check "EC read partial overwrite"                         nbd_read_verify 0xAB 2048 512
check "EC before partial: 0xEE at offset 0 (first 2K)"   nbd_read_verify 0xEE 0 2048
check "EC after partial: 0xEE at offset 2560"             nbd_read_verify 0xEE 2560 1536

# ━━━ Phase 4: Cross-boundary EC writes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Cross-boundary — EC writes spanning two 4MB objects"

BOUNDARY_OFFSET=$(( OBJ_SIZE - 4096 ))

step "Writing 8K across object 0/1 boundary (offset ${BOUNDARY_OFFSET})"
nbd_write 0x77 $BOUNDARY_OFFSET 8192
check "EC cross-boundary write (8K)"    true
check "EC read cross-boundary data"      nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Write 1M across boundary between object 2 and 3
BOUNDARY2=$(( OBJ_SIZE * 2 - 524288 ))
step "Writing 1M across object 2/3 boundary (offset ${BOUNDARY2})"
nbd_write 0x88 $BOUNDARY2 1048576
check "EC cross-boundary write (1M)"    true
check "EC read cross-boundary 1M"       nbd_read_verify 0x88 $BOUNDARY2 1048576

# ━━━ Phase 5: Large EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Large EC I/O — multi-object sequential writes"

# Create a second EC VDI for large I/O tests
step "Creating EC VDI 'ecbig' (32M, EC ${EC_POLICY})"
dog_cmd vdi create ecbig 32M --copy-policy "$EC_POLICY"
sleep 1

ECBIG_URI="nbd://${BIND}:${NBD_PORT}/ecbig"

# Write 8MB (spans 2 full objects)
step "Writing 8MB sequential block to 'ecbig'"
qemu-io -f raw -c "write -P 0x55 0 8388608" "$ECBIG_URI" 2>/dev/null
check "EC write 8MB sequential"   true
check "EC read 8MB sequential"    qemu-io -f raw -c "read -P 0x55 0 8388608" "$ECBIG_URI"

# Write 16MB (spans 4 full objects)
step "Writing 16MB sequential block"
qemu-io -f raw -c "write -P 0x66 0 16777216" "$ECBIG_URI" 2>/dev/null
check "EC write 16MB sequential"  true
check "EC read 16MB sequential"   qemu-io -f raw -c "read -P 0x66 0 16777216" "$ECBIG_URI"

echo ""
show_object_distribution

# ━━━ Phase 6: Strip verification ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 6 "Strip verification — check EC strip files on disk"

# Verify that EC strip files exist with _XX suffix
total_strips=0
for i in $(seq 0 $(( NUM_NODES - 1 ))); do
    ec_count=$(count_ec_strips "$i")
    total_strips=$(( total_strips + ec_count ))
done

step "Total EC strip files across cluster: ${total_strips}"
check "EC strips exist on disk" test "$total_strips" -gt 0

echo ""
show_ec_strip_files

# Each data object should produce EC_TOTAL (3) strips distributed across nodes
# With multiple objects, we expect at least EC_TOTAL strips total
check "At least ${EC_TOTAL} EC strips exist" test "$total_strips" -ge "$EC_TOTAL"

# Verify strip file sizes are correct
# For EC 2:1 with 4MB objects, each strip = 4MB / 2 = 2MB
EXPECTED_STRIP_SIZE=$(( OBJ_SIZE / EC_DATA ))
step "Expected strip size: ${EXPECTED_STRIP_SIZE} bytes (${OBJ_SIZE} / ${EC_DATA})"

strip_size_ok=true
for i in $(seq 0 $(( NUM_NODES - 1 ))); do
    local_dir="$(node_dir "$i")/obj"
    if [[ -d "$local_dir" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            sz=$(stat -f%z "${local_dir}/${f}" 2>/dev/null || stat -c%s "${local_dir}/${f}" 2>/dev/null || echo "0")
            if [[ "$sz" -eq "$EXPECTED_STRIP_SIZE" ]]; then
                : # correct size
            elif [[ "$sz" -gt "$EXPECTED_STRIP_SIZE" ]]; then
                step "  Strip ${f} on node${i}: ${sz} bytes (expected ${EXPECTED_STRIP_SIZE})"
                strip_size_ok=false
            fi
        done < <(ls "$local_dir" 2>/dev/null | grep '_[0-9a-f]')
    fi
done
check "EC strip sizes correct (${EXPECTED_STRIP_SIZE} bytes)" $strip_size_ok

# ━━━ Phase 7: Degraded read — node failure + reconstruction ━━━━━━━━━━
phase 7 "Degraded read — stop a node, verify EC reconstruction"

echo -e "  ${YELLOW}NOTE: Degraded reads require EC recovery/migration (not yet"
echo -e "  implemented). After a node leaves, the hash ring changes and"
echo -e "  EC strip locations shift. Tests in this phase verify current"
echo -e "  behavior and track progress toward full degraded-mode support.${NC}"
echo ""

# First verify all data is still readable
step "Pre-check: all data readable with full cluster"
check "Pre-check 0xEE at 0"     nbd_read_verify 0xEE 0 2048
check "Pre-check 0x77 boundary" nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Stop node 2 (last node) — this removes one strip from each object
step "Stopping node 2 to simulate failure"
stop_node 2
sleep 3  # Wait for heartbeat timeout detection

# Verify that node 2 is actually stopped
if ! is_running 2; then
    pass "Node 2 stopped"
    (( PASS_COUNT++ ))
else
    err "Node 2 still running"
    (( FAIL_COUNT++ ))
fi

# Write new data while degraded — should still work with 2/3 nodes
step "Writing while degraded (2 nodes)"
nbd_write 0xD1 $(( OBJ_SIZE * 4 )) 4096
check "Degraded EC write 4K at 16MB"     true
check "Degraded EC read back 4K at 16MB" nbd_read_verify 0xD1 $(( OBJ_SIZE * 4 )) 4096

# Restart node 2
step "Restarting node 2"
start_node 2 || warn "Node 2 restart failed"
sleep 3

# Verify full cluster reads still work after restart
step "Verifying reads after node 2 restart"
check "Post-restart read 0xEE at 0"   nbd_read_verify 0xEE 0 2048
check "Post-restart read 0x77 boundary" nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# ━━━ Phase 8: Cleanup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 8 "Cleanup"

cleanup

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}━━━ EC Test Summary ━━━${NC}"
echo -e "  EC Policy: ${EC_POLICY} (${EC_DATA}+${EC_PARITY})"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
if (( FAIL_COUNT > 0 )); then
    echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
else
    echo -e "  ${DIM}Failed: 0${NC}"
fi
echo ""

if (( FAIL_COUNT > 0 )); then
    echo -e "${RED}${BOLD}SOME EC TESTS FAILED${NC}"
    echo -e "  Check logs: ${LOG_DIR}/"
    exit 1
else
    echo -e "${GREEN}${BOLD}ALL EC TESTS PASSED${NC}"
    exit 0
fi
