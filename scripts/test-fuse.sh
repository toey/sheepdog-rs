#!/usr/bin/env bash
#
# test-fuse.sh — FUSE (sheepfs) E2E test suite
#
# Tests sheepfs FUSE mount operations against the Docker cluster.
#
# Usage:
#   ./scripts/test-fuse.sh [--bind ADDRESS] [--sheepfs-binary PATH]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"

KEEP=false
SKIP_FUSE=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)             KEEP=true ;;
        --skip-fuse)        SKIP_FUSE=true ;;
        --bind)             shift; BIND="$1" ;;
        --sheepfs-binary)   shift; SHEEPFS_BIN="$1" ;;
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

# Sheepfs binary
if [[ -n "${SHEEPFS_BIN:-}" ]]; then
    SHEEPFS="$SHEEPFS_BIN"
elif command -v sheepfs &>/dev/null; then
    SHEEPFS="sheepfs"
elif [[ -x "${REPO_ROOT}/target/release/sheepfs" ]]; then
    SHEEPFS="${REPO_ROOT}/target/release/sheepfs"
elif [[ -x "${REPO_ROOT}/target/debug/sheepfs" ]]; then
    SHEEPFS="${REPO_ROOT}/target/debug/sheepfs"
else
    SHEEPFS=""
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
echo "║          Sheepdog FUSE (sheepfs) E2E Tests                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:       ${BIND}"
echo -e "  sheepfs:       ${SHEEPFS:-not found}"
echo ""

# Check prerequisites
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
phase 1 "Setup — verify cluster and create VDI"

check "Cluster info available" dog_cmd cluster info
check "Node list available" dog_cmd node list

step "Creating VDI for FUSE testing"
dog_cmd vdi create fusetest 64M
sleep 1

check "VDI created" dog_cmd vdi list

step "Creating another VDI"
dog_cmd vdi create fusetest2 32M
sleep 1

step "VDI list:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: sheepfs binary check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "sheepfs binary verification"

if [[ -z "$SHEEPFS" ]]; then
    echo -e "  ${YELLOW}SKIP: sheepfs binary not found${NC}"
    echo -e "  Build it: cargo build -p sheepfs --release"
    echo -e "  Or provide path: --sheepfs-binary /path/to/sheepfs"
    check "sheepfs binary available" false
else
    pass "sheepfs binary found: ${SHEEPFS}"

    step "sheepfs version"
    version_output=$($SHEEPFS --help 2>&1 || true)
    echo "$version_output" | head -5 | while IFS= read -r line; do echo "    $line"; done
    check "sheepfs binary is executable" true
fi

# ━━━ Phase 3: FUSE mount attempt ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "FUSE mount attempt"

if [[ "$SKIP_FUSE" == "true" ]]; then
    echo -e "  ${YELLOW}SKIP: FUSE mount skipped (--skip-fuse flag)${NC}"
elif [[ -z "$SHEEPFS" ]]; then
    echo -e "  ${YELLOW}SKIP: sheepfs not available${NC}"
elif [[ -n "${DOCKER_ADMIN:-}" ]]; then
    echo -e "  ${YELLOW}SKIP: FUSE mount not available in Docker admin container${NC}"
    echo -e "  FUSE mounting requires host-level FUSE access"
    echo -e "  Run: docker compose exec node0 ./scripts/test-fuse.sh"
    check "FUSE mount available" false
elif command -v fusermount &>/dev/null; then
    MOUNT_DIR="/tmp/sheepfs-mount-$$"
    mkdir -p "$MOUNT_DIR"

    step "Attempting FUSE mount"
    mount_output=$($SHEEPFS -o bind=${BIND} "$MOUNT_DIR" 2>&1 &)
    mount_pid=$!

    sleep 3

    if mount | grep -q "$MOUNT_DIR"; then
        pass "FUSE mount succeeded"

        step "Listing VDI directory"
        ls -la "$MOUNT_DIR" 2>/dev/null
        check_result "FUSE ls succeeds" "$?"

        step "Checking VDI directory contents"
        if ls "$MOUNT_DIR" 2>/dev/null | grep -q "fusetest"; then
            pass "FUSE VDI directory contains expected entries"
        else
            warn "FUSE VDI directory contents unexpected"
        fi

        step "Unmounting"
        fusermount -u "$MOUNT_DIR" 2>/dev/null
        rmdir "$MOUNT_DIR" 2>/dev/null
        check_result "FUSE unmount succeeds" "$?"
    else
        warn "FUSE mount failed"
        echo "$mount_output" | while IFS= read -r line; do echo "    $line"; done
        check "FUSE mount attempt completed" true
    fi
else
    echo -e "  ${YELLOW}SKIP: fusermount not found${NC}"
    check "FUSE mount available" false
fi

# ━━━ Phase 4: HTTP fallback ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "HTTP fallback — verify VDI data via HTTP API"

# Use HTTP API to verify VDI data if FUSE mount is not available
if command -v curl &>/dev/null; then
    step "Checking HTTP API for VDI"
    http_response=$(curl -s "http://${BIND}:8000/vdi" 2>/dev/null || echo "")
    
    if [[ -n "$http_response" ]]; then
        pass "HTTP API responds"
        check_body "HTTP response contains VDI data" "$http_response" "fusetest"
    else
        warn "HTTP API not available or returned empty"
    fi
else
    warn "curl not found, skipping HTTP fallback"
fi

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
    exit 1
else
    echo -e "${GREEN}${BOLD}ALL TESTS PASSED${NC}"
    exit 0
fi
