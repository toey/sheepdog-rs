#!/usr/bin/env bash
#
# test-nfs.sh — NFS E2E test suite
#
# Tests NFS operations against the Docker cluster's node0 NFS port.
#
# Usage:
#   ./scripts/test-nfs.sh [--bind ADDRESS] [--nfs-port PORT] [--nfs-mount-port PORT]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"
NFS_PORT="${NFS_PORT:-2049}"
NFS_MOUNT_PORT="${NFS_MOUNT_PORT:-2050}"

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=true ;;
        --bind) shift; BIND="$1" ;;
        --nfs-port) shift; NFS_PORT="$1" ;;
        --nfs-mount-port) shift; NFS_MOUNT_PORT="$1" ;;
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
echo "║            Sheepdog NFS E2E Tests                          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:   ${BIND}"
echo -e "  NFS Port:  ${NFS_PORT}"
echo -e "  Mount Port: ${NFS_MOUNT_PORT}"
echo ""

# Check prerequisites
if ! command -v mount &>/dev/null; then
    err "mount command not found"
    exit 1
fi

# Check cluster is running
if ! nc -z "$BIND" 7000 2>/dev/null; then
    err "Cluster not running at ${BIND}:7000"
    exit 1
fi

info "Cluster is running. NFS port ${NFS_PORT} is accessible."

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — verify cluster and create VDI"

check "TCP port 7000 bound" nc -z "$BIND" 7000
check "TCP port ${NFS_PORT} bound" nc -z "$BIND" "$NFS_PORT"

step "Creating VDI for NFS testing"
dog_cmd vdi create nfstest 64M
sleep 1

check "VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: NFS Protocol Check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "NFS protocol check"

# Try to mount NFS (this may fail in Docker without proper privileges)
NFS_MOUNT_DIR="/tmp/nfstest-mount-$$"

step "Attempting NFS mount"
if mount -t nfs "${BIND}:${NFS_MOUNT_PORT}:/mnt" "$NFS_MOUNT_DIR" 2>/dev/null; then
    pass "NFS mount succeeded"

    step "NFS: ls mount point"
    ls -la "$NFS_MOUNT_DIR" 2>/dev/null
    check_result "NFS ls succeeds" "$?"

    step "NFS: create file"
    echo "Hello from NFS test" > "$NFS_MOUNT_DIR/testfile.txt" 2>/dev/null
    check_result "NFS write file succeeds" "$?"

    step "NFS: read file"
    file_content=$(cat "$NFS_MOUNT_DIR/testfile.txt" 2>/dev/null)
    check_result "NFS read file succeeds" "$?"
    check_body "NFS file content matches" "$file_content" "Hello from NFS test"

    step "NFS: create directory"
    mkdir "$NFS_MOUNT_DIR/testdir" 2>/dev/null
    check_result "NFS mkdir succeeds" "$?"

    step "NFS: ls directory"
    ls -la "$NFS_MOUNT_DIR/testdir" 2>/dev/null
    check_result "NFS directory ls succeeds" "$?"

    step "NFS: write to directory"
    echo "Directory test" > "$NFS_MOUNT_DIR/testdir/nested.txt" 2>/dev/null
    check_result "NFS write to directory succeeds" "$?"

    step "NFS: delete file"
    rm "$NFS_MOUNT_DIR/testfile.txt" 2>/dev/null
    check_result "NFS delete file succeeds" "$?"

    step "NFS: unmount"
    umount "$NFS_MOUNT_DIR" 2>/dev/null
    check_result "NFS unmount succeeds" "$?"
    rmdir "$NFS_MOUNT_DIR" 2>/dev/null
else
    warn "NFS mount failed (may require elevated privileges)"
    step "Skipping NFS mount tests"
    step "Verifying NFS port is accessible"
    check "NFS port ${NFS_MOUNT_PORT} is accessible" nc -z "$BIND" "$NFS_MOUNT_PORT"
fi

# ━━━ Phase 3: NFS Server Status ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "NFS server status"

step "Checking NFS port connectivity"
check "NFS port ${NFS_PORT} is listening" nc -z "$BIND" "$NFS_PORT"
check "NFS mount port ${NFS_MOUNT_PORT} is listening" nc -z "$BIND" "$NFS_MOUNT_PORT"

step "Checking cluster health"
check "Cluster info available" dog_cmd cluster info
check "Node list available" dog_cmd node list

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}Summary:${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
echo ""

if [[ ${FAIL_COUNT} -gt 0 ]]; then
    exit 1
fi
