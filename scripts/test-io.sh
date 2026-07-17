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
# Usage:
#   ./scripts/test-io.sh [--keep] [--skip-directio] [--dog-path PATH] [--bind ADDRESS] [--nbd-port PORT]
#
# Docker mode (default):
#   Uses dog binary from PATH and connects to cluster at BIND:NBD_PORT
#
# Host mode (legacy):
#   Sets DOG="" to trigger binary detection from target/release/sheep
#

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

# Override defaults if running in Docker mode
BIND="${BIND:-127.0.0.1}"
NBD_PORT="${NBD_PORT:-10809}"

COPIES=1
VDI_SIZE="64M"
VDI_NAME="iotest"

# 4 MB object size (SD_DATA_OBJ_SIZE = 1 << 22)
OBJ_SIZE=4194304

KEEP=false
SKIP_DIRECTIO=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)           KEEP=true ;;
        --skip-directio)  SKIP_DIRECTIO=true ;;
        --dog-path)       shift; DOG_PATH="$1" ;;
    esac
done

# Docker mode: use dog from PATH or admin container
if [[ -n "${DOG_PATH:-}" ]]; then
    DOG="$DOG_PATH"
elif command -v dog &>/dev/null; then
    DOG="dog"
elif [[ -x "${REPO_ROOT}/target/release/dog" ]]; then
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/dog" ]]; then
    DOG="${REPO_ROOT}/target/debug/dog"
else
    echo "ERROR: dog binary not found"
    exit 1
fi

# NBD URI for qemu-io
NBD_URI="nbd://${BIND}:${NBD_PORT}/${VDI_NAME}"

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

# ── NBD helpers ────────────────────────────────────────────────────────
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

# ── Dog helpers ────────────────────────────────────────────────────────
dog_cmd() {
    "$DOG" -a "$BIND" -p 7000 "$@" 2>/dev/null
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          Sheepdog I/O Correctness Test (No Cache)           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Dog:       ${DOG}"
echo -e "  Cluster:   ${BIND}:7000"
echo -e "  NBD URI:   ${NBD_URI}"
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

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — verify cluster is ready"

check "Cluster info available" dog_cmd cluster info

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
step "Writing pattern 0xAA at offset 0 (object 0)"
nbd_write 0xAA 0 4096
check_result "Write 4K at offset 0" "$?"

step "Writing pattern 0xBB at offset 4MB (object 1)"
nbd_write 0xBB $OBJ_SIZE 4096
check_result "Write 4K at offset 4MB" "$?"

step "Writing pattern 0xCC at offset 8MB (object 2)"
nbd_write 0xCC $(( OBJ_SIZE * 2 )) 4096
check_result "Write 4K at offset 8MB" "$?"

step "Writing pattern 0xDD at offset 12MB (object 3)"
nbd_write 0xDD $(( OBJ_SIZE * 3 )) 4096
check_result "Write 4K at offset 12MB" "$?"

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
check_result "Write 64K block" "$?"
check "Read 64K block"     nbd_read_verify 0x11 65536 65536

nbd_write 0x22 262144 262144    # 256K at offset 256K
check_result "Write 256K block" "$?"
check "Read 256K block"    nbd_read_verify 0x22 262144 262144

nbd_write 0x33 1048576 1048576  # 1M at offset 1M
check_result "Write 1M block" "$?"
check "Read 1M block"      nbd_read_verify 0x33 1048576 1048576

echo ""
step "Object distribution:"
dog_cmd obj list 2>/dev/null | head -5 | while IFS= read -r line; do echo "    $line"; done

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
check_result "Partial write 512B at offset 2048" "$?"
check "Read partial overwrite"                       nbd_read_verify 0xAB 2048 512
# Before and after the partial write should be as before
check "Before partial: 0xEE at offset 0 (first 2K)"  nbd_read_verify 0xEE 0 2048
check "After partial: 0xEE at offset 2560"            nbd_read_verify 0xEE 2560 1536

# ━━━ Phase 4: Cross-boundary writes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Cross-boundary — writes spanning two 4MB objects"

# Write 8K straddling the boundary between object 0 and object 1
BOUNDARY_OFFSET=$(( OBJ_SIZE - 4096 ))

step "Writing 8K across object 0/1 boundary (offset ${BOUNDARY_OFFSET})"
nbd_write 0x77 $BOUNDARY_OFFSET 8192
check_result "Cross-boundary write (8K)" "$?"
check "Read cross-boundary data"          nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Write 1M across boundary between object 2 and 3
BOUNDARY2=$(( OBJ_SIZE * 2 - 524288 ))
step "Writing 1M across object 2/3 boundary (offset ${BOUNDARY2})"
nbd_write 0x88 $BOUNDARY2 1048576
check_result "Cross-boundary write (1M)" "$?"
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
check_result "Write 8MB sequential" "$?"
check "Read 8MB sequential"    qemu-io -f raw -c "read -P 0x55 0 8388608" "$BIGTEST_URI"

# Write 16MB (spans 4 full objects)
step "Writing 16MB sequential block"
qemu-io -f raw -c "write -P 0x66 0 16777216" "$BIGTEST_URI" 2>/dev/null
check_result "Write 16MB sequential" "$?"
check "Read 16MB sequential"   qemu-io -f raw -c "read -P 0x66 0 16777216" "$BIGTEST_URI"

# Write full VDI (32MB)
step "Writing full 32MB VDI"
qemu-io -f raw -c "write -P 0x99 0 33554432" "$BIGTEST_URI" 2>/dev/null
check_result "Write 32MB full VDI" "$?"
check "Read 32MB full VDI"     qemu-io -f raw -c "read -P 0x99 0 33554432" "$BIGTEST_URI"

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
check_result "Sparse write at 0" "$?"

qemu-io -f raw -c "write -P 0xA2 2097152 4096" "$SPARSE_URI" 2>/dev/null
check_result "Sparse write at 2M" "$?"

qemu-io -f raw -c "write -P 0xA3 5242880 4096" "$SPARSE_URI" 2>/dev/null
check_result "Sparse write at 5M" "$?"

qemu-io -f raw -c "write -P 0xA4 10485760 4096" "$SPARSE_URI" 2>/dev/null
check_result "Sparse write at 10M" "$?"

qemu-io -f raw -c "write -P 0xA5 20971520 4096" "$SPARSE_URI" 2>/dev/null
check_result "Sparse write at 20M" "$?"

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
    echo -e "\n${YELLOW}[SKIP]${NC} Direct I/O test skipped"
else
    phase 7 "Direct I/O — verify with --directio"
    echo -e "  ${DIM}→ Direct I/O mode requires restarting the cluster with --directio${NC}"
    echo -e "  ${DIM}→ Skipping in Docker mode (cluster restart not supported)${NC}\n"
fi

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}Summary:${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
echo ""

if [[ ${FAIL_COUNT} -gt 0 ]]; then
    exit 1
fi
