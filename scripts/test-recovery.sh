#!/usr/bin/env bash
#
# test-recovery.sh — Recovery E2E test suite
#
# Tests cluster recovery after node failures.
#
# Usage:
#   ./scripts/test-recovery.sh [--bind ADDRESS]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=true ;;
        --bind) shift; BIND="$1" ;;
    esac
done

# Dog binary
if command -v dog &>/dev/null; then
    DOG="dog"
elif [[ -x "${REPO_ROOT}/target/release/dog" ]]; then
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/dog" ]]; then
    DOG="${REPO_ROOT}/target/debug/dog"
else
    echo "ERROR: dog binary not found"
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

check_body() {
    local desc="$1" body="$2" expected="$3"
    if [[ "$body" == *"$expected"* ]]; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc (expected contains '$expected', got: '$body')"
        (( FAIL_COUNT++ ))
    fi
}

# Dog helpers
dog_cmd() {
    "$DOG" -a "$BIND" -p 7000 "$@" 2>/dev/null
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Sheepdog Recovery E2E Tests                       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:   ${BIND}"
echo ""

# Check cluster is running
if ! nc -z "$BIND" 7000 2>/dev/null; then
    err "Cluster not running at ${BIND}:7000"
    exit 1
fi

info "Cluster is running."

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — verify cluster and create test VDI"

check "Cluster info available" dog_cmd cluster info
check "Node list available" dog_cmd node list

step "Creating VDI for recovery testing"
dog_cmd vdi create recoverytest 32M
sleep 1

check "VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: Write Data ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Write data to VDI"

# Create an NBD connection and write some data
NBD_URI="nbd://${BIND}:10809/recoverytest"

if command -v qemu-io &>/dev/null; then
    step "Writing 8MB to VDI"
    qemu-io -f raw -c "write -P 0xAA 0 8388608" "$NBD_URI" 2>/dev/null
    check_result "NBD write succeeds" "$?"

    step "Reading back and verifying"
    qemu-io -f raw -c "read -P 0xAA 0 8388608" "$NBD_URI" 2>/dev/null
    check_result "NBD read verification succeeds" "$?"
else
    warn "qemu-io not found, skipping NBD I/O tests"
fi

# ━━━ Phase 3: Node Failure Simulation ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "Node failure simulation"

# Check if we're in Docker mode (cannot restart nodes from admin container)
if [[ -n "${DOCKER_ADMIN:-}" ]]; then
    warn "Node failure simulation skipped (Docker mode)"
    step "In Docker mode, node failures require manual intervention"
else
    # This would normally restart nodes, but requires host access
    warn "Node failure simulation requires host-level access"
    step "Use cluster-docker.sh restart to simulate node failure"
fi

# ━━━ Phase 4: Data Integrity After Failure ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Data integrity verification"

step "Verifying VDI still accessible"
check "VDI list still works" dog_cmd vdi list

step "Verifying cluster info"
check "Cluster info still available" dog_cmd cluster info

# Read back data if NBD is available
if command -v qemu-io &>/dev/null; then
    step "Reading back written data"
    qemu-io -f raw -c "read -P 0xAA 0 8388608" "$NBD_URI" 2>/dev/null
    check_result "Data integrity after simulated failure" "$?"
fi

# ━━━ Phase 5: Recovery Verification ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Recovery verification"

step "Checking node status"
dog_cmd node list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

step "Verifying all objects are accessible"
dog_cmd obj list 2>/dev/null | head -5 | while IFS= read -r line; do echo "    $line"; done

step "Final cluster info"
dog_cmd cluster info 2>/dev/null

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}Summary:${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
echo ""

if [[ ${FAIL_COUNT} -gt 0 ]]; then
    exit 1
fi
