#!/usr/bin/env bash
#
# test-ec.sh — Erasure Coding (EC) E2E test suite
#
# Tests EC data integrity against the Docker cluster.
#
# Usage:
#   ./scripts/test-ec.sh [--bind ADDRESS] [--nbd-port PORT] [--copies COPY_POLICY]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"
NBD_PORT="${NBD_PORT:-10809}"
COPIES="${COPIES:-2:1}"

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)     KEEP=true ;;
        --bind)     shift; BIND="$1" ;;
        --nbd-port) shift; NBD_PORT="$1" ;;
        --copies)   shift; COPIES="$1" ;;
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

# Dog helpers
dog_cmd() {
    "$DOG" -a "$BIND" -p 7000 "$@" 2>/dev/null
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║      Sheepdog Erasure Coding (EC) E2E Tests                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:   ${BIND}"
echo -e "  NBD Port:  ${NBD_PORT}"
echo -e "  Copies:    ${COPIES}"
echo ""

# Check prerequisites
if ! command -v qemu-io &>/dev/null; then
    err "qemu-io not found. Install qemu-utils for NBD I/O testing."
    exit 1
fi

if ! command -v nc &>/dev/null; then
    err "nc (netcat) not found"
    exit 1
fi

# Check cluster is running
if ! nc -z "$BIND" 7000 2>/dev/null; then
    err "Cluster not running at ${BIND}:7000"
    exit 1
fi

info "Cluster is running."

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — verify cluster and create EC VDI"

check "Cluster info available" dog_cmd cluster info
check "Node list available" dog_cmd node list

# Create EC VDI
VDI_NAME="ectest"
step "Creating EC VDI '${VDI_NAME}' (64M, copies=${COPIES})"
dog_cmd vdi create "$VDI_NAME" 64M --copy-policy "$COPIES"
sleep 1

check "EC VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: Basic EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Basic EC I/O — sequential write + read"

NBD_URI="nbd://${BIND}:${NBD_PORT}/${VDI_NAME}"

step "Writing patterns at object boundaries"

# 4MB = SD_DATA_OBJ_SIZE
OBJ_SIZE=4194304

step "Writing pattern 0xAA at offset 0 (object 0)"
qemu-io -f raw -c "write -P 0xAA 0 4096" "$NBD_URI" 2>/dev/null
check_result "EC write 4K at offset 0" "$?"

step "Writing pattern 0xBB at offset 4MB (object 1)"
qemu-io -f raw -c "write -P 0xBB ${OBJ_SIZE} 4096" "$NBD_URI" 2>/dev/null
check_result "EC write 4K at offset 4MB" "$?"

step "Writing pattern 0xCC at offset 8MB (object 2)"
qemu-io -f raw -c "write -P 0xCC $(( OBJ_SIZE * 2 )) 4096" "$NBD_URI" 2>/dev/null
check_result "EC write 4K at offset 8MB" "$?"

step "Reading back and verifying patterns"
check "EC read 0xAA at offset 0"        qemu-io -f raw -c "read -P 0xAA 0 4096" "$NBD_URI" 2>/dev/null
check "EC read 0xBB at offset 4MB"      qemu-io -f raw -c "read -P 0xBB ${OBJ_SIZE} 4096" "$NBD_URI" 2>/dev/null
check "EC read 0xCC at offset 8MB"      qemu-io -f raw -c "read -P 0xCC $(( OBJ_SIZE * 2 )) 4096" "$NBD_URI" 2>/dev/null

# Write larger blocks
echo ""
step "Writing larger blocks"

qemu-io -f raw -c "write -P 0x11 65536 65536" "$NBD_URI" 2>/dev/null
check_result "EC write 64K block" "$?"
check "EC read 64K block" qemu-io -f raw -c "read -P 0x11 65536 65536" "$NBD_URI" 2>/dev/null

qemu-io -f raw -c "write -P 0x33 1048576 1048576" "$NBD_URI" 2>/dev/null
check_result "EC write 1M block" "$?"
check "EC read 1M block" qemu-io -f raw -c "read -P 0x33 1048576 1048576" "$NBD_URI" 2>/dev/null

# ━━━ Phase 3: Overwrite (read-modify-write) ━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "EC Overwrite — partial writes"

step "Overwriting offset 0 with new pattern 0xEE"
qemu-io -f raw -c "write -P 0xEE 0 4096" "$NBD_URI" 2>/dev/null
check_result "EC overwrite at offset 0" "$?"
check "EC read 0xEE at offset 0" qemu-io -f raw -c "read -P 0xEE 0 4096" "$NBD_URI" 2>/dev/null

step "Verifying non-overwritten data is intact"
check "EC 0xBB at 4MB still intact"    qemu-io -f raw -c "read -P 0xBB ${OBJ_SIZE} 4096" "$NBD_URI" 2>/dev/null
check "EC 0xCC at 8MB still intact"    qemu-io -f raw -c "read -P 0xCC $(( OBJ_SIZE * 2 )) 4096" "$NBD_URI" 2>/dev/null
check "EC 64K block still intact"      qemu-io -f raw -c "read -P 0x11 65536 65536" "$NBD_URI" 2>/dev/null

# ━━━ Phase 4: Cross-boundary writes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Cross-boundary — writes spanning two 4MB objects"

BOUNDARY_OFFSET=$(( OBJ_SIZE - 4096 ))
step "Writing 8K across object 0/1 boundary (offset ${BOUNDARY_OFFSET})"
qemu-io -f raw -c "write -P 0x77 ${BOUNDARY_OFFSET} 8192" "$NBD_URI" 2>/dev/null
check_result "EC cross-boundary write (8K)" "$?"
check "EC read cross-boundary data" qemu-io -f raw -c "read -P 0x77 ${BOUNDARY_OFFSET} 8192" "$NBD_URI" 2>/dev/null

# ━━━ Phase 5: Large EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Large EC I/O — multi-object sequential writes"

step "Creating EC VDI 'ecbig' (32M, copies=${COPIES})"
dog_cmd vdi create ecbig 32M --copy-policy "$COPIES"
sleep 1

ECBIG_URI="nbd://${BIND}:${NBD_PORT}/ecbig"

step "Writing 8MB sequential block"
qemu-io -f raw -c "write -P 0x55 0 8388608" "$ECBIG_URI" 2>/dev/null
check_result "EC write 8MB sequential" "$?"
check "EC read 8MB sequential" qemu-io -f raw -c "read -P 0x55 0 8388608" "$ECBIG_URI" 2>/dev/null

step "Writing 16MB sequential block"
qemu-io -f raw -c "write -P 0x66 0 16777216" "$ECBIG_URI" 2>/dev/null
check_result "EC write 16MB sequential" "$?"
check "EC read 16MB sequential" qemu-io -f raw -c "read -P 0x66 0 16777216" "$ECBIG_URI" 2>/dev/null

# ━━━ Phase 6: Data Integrity ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 6 "Data integrity verification"

step "Final verification of all written data"
check "0xEE at offset 0" qemu-io -f raw -c "read -P 0xEE 0 4096" "$NBD_URI" 2>/dev/null
check "0x77 at boundary" qemu-io -f raw -c "read -P 0x77 ${BOUNDARY_OFFSET} 8192" "$NBD_URI" 2>/dev/null
check "0x55 at 8MB"      qemu-io -f raw -c "read -P 0x55 0 8388608" "$ECBIG_URI" 2>/dev/null
check "0x66 at 16MB"     qemu-io -f raw -c "read -P 0x66 0 16777216" "$ECBIG_URI" 2>/dev/null

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}━━━ EC Test Summary ━━━${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
if (( FAIL_COUNT > 0 )); then
    echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
else
    echo -e "  ${DIM}Failed: 0${NC}"
fi
echo ""

if (( FAIL_COUNT > 0 )); then
    echo -e "${RED}${BOLD}SOME EC TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}${BOLD}ALL EC TESTS PASSED${NC}"
    exit 0
fi
