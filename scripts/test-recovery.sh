#!/usr/bin/env bash
#
# test-recovery.sh — Test node failure recovery and new node addition
#
# Runs a full scenario:
#   Phase 1: Start 3-node cluster, write data
#   Phase 2: Kill a node (simulate crash)
#   Phase 3: Verify cluster detects failure + recovers objects
#   Phase 4: Add a new node (node 3)
#   Phase 5: Verify rebalance + data integrity
#   Phase 6: Cleanup
#
# Requirements:
#   - cargo build (debug or release)
#   - qemu-io (from qemu-utils) for NBD I/O verification
#
# Usage:
#   ./scripts/test-recovery.sh [--keep]     # --keep: don't cleanup on success
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="/tmp/sheepdog-recovery-test"
source "${SCRIPT_DIR}/defaults.sh"
LOG_DIR="${DATA_ROOT}/logs"
COPIES=2
KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=true ;;
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

kill_node() {
    local idx=$1
    local pidfile
    pidfile=$(node_pid "$idx")
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            step "Sending SIGKILL to node ${idx} (pid=${pid})"
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$pidfile"
    fi
}

stop_all() {
    for i in 3 2 1 0; do
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
    for i in "$@"; do
        local count
        count=$(count_objects "$i")
        local status=""
        if is_running "$i"; then
            status="${GREEN}running${NC}"
        else
            status="${RED}stopped${NC}"
        fi
        printf "    node%d (port %d): %3s objects  [%b]\n" "$i" "$(node_port "$i")" "$count" "$status"
    done
}

dog_cmd() {
    "$DOG" -a "$BIND" -p "$(node_port 0)" "$@" 2>/dev/null
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
echo "║        Sheepdog Recovery & Rebalance Test Suite             ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Binary:  ${SHEEP}"
echo -e "  Data:    ${DATA_ROOT}"
echo -e "  Copies:  ${COPIES}"
echo ""

# Clean any previous run
rm -rf "$DATA_ROOT"
mkdir -p "$LOG_DIR"

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — 3-node cluster with data"

# Start 3 nodes (node 0 with NBD)
start_node 0 --nbd --nbd-port "$NBD_PORT" || { err "Failed to start node 0"; stop_all; exit 1; }
start_node 1 || { err "Failed to start node 1"; stop_all; exit 1; }
start_node 2 || { err "Failed to start node 2"; stop_all; exit 1; }

# Wait for all nodes to settle
sleep 2

# Format cluster
step "Formatting cluster (copies=${COPIES})"
if ! dog_cmd cluster format --copies "$COPIES" 2>&1; then
    warn "Format failed (may already be formatted)"
fi
sleep 1

# Check cluster
check "Cluster is formatted" dog_cmd cluster info

# Show node list
step "Node list:"
dog_cmd node list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# Create VDI
step "Creating VDI 'testdisk' (100M)"
dog_cmd vdi create testdisk 100M
sleep 1

check "VDI created" dog_cmd vdi list

# Write test patterns via NBD
HAS_QEMU_IO=true
if ! command -v qemu-io &>/dev/null; then
    warn "qemu-io not found — skipping NBD data verification"
    warn "Install qemu-utils for full test coverage"
    HAS_QEMU_IO=false
fi

if [[ "$HAS_QEMU_IO" == "true" ]]; then
    step "Writing test patterns via NBD"

    # Pattern 1: 0xAA at offset 0 (first object)
    qemu-io -f raw -c "write -P 0xAA 0 4096" \
        "nbd://${BIND}:${NBD_PORT}/testdisk" 2>/dev/null
    check "Write pattern 0xAA at offset 0" true

    # Pattern 2: 0xBB at offset 4MB (second object)
    qemu-io -f raw -c "write -P 0xBB 4194304 4096" \
        "nbd://${BIND}:${NBD_PORT}/testdisk" 2>/dev/null
    check "Write pattern 0xBB at offset 4MB" true

    # Pattern 3: 0xCC at offset 8MB (third object)
    qemu-io -f raw -c "write -P 0xCC 8388608 4096" \
        "nbd://${BIND}:${NBD_PORT}/testdisk" 2>/dev/null
    check "Write pattern 0xCC at offset 8MB" true

    # Verify reads
    step "Verifying written data"
    check "Read pattern 0xAA at offset 0" \
        qemu-io -f raw -c "read -P 0xAA 0 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xBB at offset 4MB" \
        qemu-io -f raw -c "read -P 0xBB 4194304 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xCC at offset 8MB" \
        qemu-io -f raw -c "read -P 0xCC 8388608 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
fi

echo ""
show_object_distribution 0 1 2

# Record epoch before kill
EPOCH_BEFORE=$(dog_cmd cluster info 2>/dev/null | grep -i epoch | head -1 | grep -oE '[0-9]+' | head -1)
info "Current epoch: ${EPOCH_BEFORE:-unknown}"

# ━━━ Phase 2: Kill Node ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Kill Node — simulate crash"

KILL_NODE=2
KILL_PORT=$(node_port $KILL_NODE)
OBJS_ON_KILLED=$(count_objects $KILL_NODE)

info "Node ${KILL_NODE} has ${OBJS_ON_KILLED} objects before kill"
echo ""
kill_node $KILL_NODE

if ! is_running $KILL_NODE; then
    pass "Node ${KILL_NODE} is stopped"
    (( PASS_COUNT++ ))
else
    err "Node ${KILL_NODE} is still running!"
    (( FAIL_COUNT++ ))
fi

# Wait for cluster to detect failure (heartbeat timeout = 15s)
step "Waiting for cluster to detect node failure (heartbeat timeout ~15-20s)..."
DETECT_TIMEOUT=30
elapsed=0
while (( elapsed < DETECT_TIMEOUT )); do
    node_count=$(dog_cmd node list 2>/dev/null | grep -c "127.0.0.1" || true)
    if (( node_count <= 2 )); then
        break
    fi
    sleep 2
    elapsed=$(( elapsed + 2 ))
    printf "    %ds / %ds ...\r" "$elapsed" "$DETECT_TIMEOUT"
done
echo ""

# Check node count
node_count=$(dog_cmd node list 2>/dev/null | grep -c "127.0.0.1" || true)
check "Cluster detected node failure (${node_count} nodes remaining)" test "$node_count" -le 2

EPOCH_AFTER_KILL=$(dog_cmd cluster info 2>/dev/null | grep -i epoch | head -1 | grep -oE '[0-9]+' | head -1)
info "Epoch after kill: ${EPOCH_AFTER_KILL:-unknown}"

if [[ -n "$EPOCH_BEFORE" && -n "$EPOCH_AFTER_KILL" ]]; then
    check "Epoch incremented after node leave" test "$EPOCH_AFTER_KILL" -gt "$EPOCH_BEFORE"
fi

step "Node list after kill:"
dog_cmd node list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 3: Verify Recovery ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "Recovery — verify data integrity"

# Wait for recovery to process
step "Waiting for recovery to run (5s)..."
sleep 5

echo ""
show_object_distribution 0 1 2

# Check recovery status
step "Recovery status:"
dog_cmd cluster recover status 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# Verify data still readable
if [[ "$HAS_QEMU_IO" == "true" ]]; then
    step "Verifying data integrity after node failure"
    check "Read pattern 0xAA after failure" \
        qemu-io -f raw -c "read -P 0xAA 0 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xBB after failure" \
        qemu-io -f raw -c "read -P 0xBB 4194304 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xCC after failure" \
        qemu-io -f raw -c "read -P 0xCC 8388608 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
fi

# VDI should still be accessible
check "VDI list still works" dog_cmd vdi list

# ━━━ Phase 4: Add New Node ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Add Node — join new node 3"

EPOCH_BEFORE_ADD=$(dog_cmd cluster info 2>/dev/null | grep -i epoch | head -1 | grep -oE '[0-9]+' | head -1)
info "Epoch before add: ${EPOCH_BEFORE_ADD:-unknown}"

start_node 3 || { err "Failed to start node 3"; stop_all; exit 1; }

# Wait for join
sleep 3

node_count=$(dog_cmd node list 2>/dev/null | grep -c "127.0.0.1" || true)
check "New node joined (${node_count} nodes)" test "$node_count" -ge 3

EPOCH_AFTER_ADD=$(dog_cmd cluster info 2>/dev/null | grep -i epoch | head -1 | grep -oE '[0-9]+' | head -1)
info "Epoch after add: ${EPOCH_AFTER_ADD:-unknown}"

if [[ -n "$EPOCH_BEFORE_ADD" && -n "$EPOCH_AFTER_ADD" ]]; then
    check "Epoch incremented after node join" test "$EPOCH_AFTER_ADD" -gt "$EPOCH_BEFORE_ADD"
fi

step "Node list after add:"
dog_cmd node list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 5: Verify Rebalance ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Rebalance — verify data after new node joins"

step "Waiting for rebalance (5s)..."
sleep 5

echo ""
show_object_distribution 0 1 2 3

# Verify data still readable with new cluster topology
if [[ "$HAS_QEMU_IO" == "true" ]]; then
    step "Verifying data integrity after rebalance"
    check "Read pattern 0xAA after rebalance" \
        qemu-io -f raw -c "read -P 0xAA 0 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xBB after rebalance" \
        qemu-io -f raw -c "read -P 0xBB 4194304 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
    check "Read pattern 0xCC after rebalance" \
        qemu-io -f raw -c "read -P 0xCC 8388608 4096" "nbd://${BIND}:${NBD_PORT}/testdisk"
fi

check "VDI list works after rebalance" dog_cmd vdi list

# ━━━ Phase 6: Cleanup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 6 "Cleanup"

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
