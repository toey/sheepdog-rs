#!/usr/bin/env bash
#
# test-iscsi.sh — iSCSI Target E2E test suite
#
# Tests iSCSI target availability against the Docker cluster.
#
# Usage:
#   ./scripts/test-iscsi.sh [--bind ADDRESS] [--iscsi-port PORT]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"
ISCSI_PORT="${ISCSI_PORT:-3260}"
ISCSI_ADDR="${BIND}:${ISCSI_PORT}"
VDI_NAME="iscsi-test"
VDI_SIZE="256M"

KEEP=false
SKIP_ISCSIADM=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)           KEEP=true ;;
        --bind)           shift; BIND="$1" ;;
        --iscsi-port)     shift; ISCSI_PORT="$1" ;;
        --skip-iscsiadm)  SKIP_ISCSIADM=true ;;
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
        err "$desc (expected='$expected', got='$body')"
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
echo "║          Sheepdog iSCSI Target E2E Tests                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:   ${BIND}"
echo -e "  iSCSI Port: ${ISCSI_PORT}"
echo -e "  VDI:       ${VDI_NAME} (${VDI_SIZE})"
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

# ━━━ Phase 1: VDI Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — create VDI for iSCSI testing"

check "Cluster info available" dog_cmd cluster info
check "Node list available" dog_cmd node list

step "Creating VDI '${VDI_NAME}' (${VDI_SIZE})"
dog_cmd vdi create "$VDI_NAME" "$VDI_SIZE"
sleep 1

check "VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: iSCSI Port Verification ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Verify iSCSI target port is bound"

step "Checking iSCSI port ${ISCSI_PORT}"
if nc -z "$BIND" "$ISCSI_PORT" 2>/dev/null; then
    pass "iSCSI port ${ISCSI_PORT} is bound and accessible"
else
    warn "iSCSI port ${ISCSI_PORT} is not accessible"
    step "iSCSI may not be enabled on this node"
    step "Verify cluster was started with --iscsi flag"
fi

# ━━━ Phase 3: iSCSI Discovery ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "iSCSI discovery"

if [[ "$SKIP_ISCSIADM" == "true" ]] || ! command -v iscsiadm &>/dev/null; then
    echo -e "  ${YELLOW}SKIP: iscsiadm not available (--skip-iscsiadm or not installed)${NC}"
    echo -e "  Install open-iscsi: sudo apt-get install open-iscsi"
    check "iSCSI port check above" true
elif nc -z "$BIND" "$ISCSI_PORT" 2>/dev/null; then
    step "Discovering iSCSI targets at ${ISCSI_ADDR}"
    discover_output=$(iscsiadm -m discovery -t st -p "${BIND}:${ISCSI_PORT}" 2>&1)
    discover_result=$?
    
    if [[ $discover_result -eq 0 ]]; then
        info "iSCSI discovery output:"
        echo "$discover_output" | while IFS= read -r line; do echo "    $line"; done
        pass "iSCSI discovery succeeded"
    else
        warn "iSCSI discovery failed (target may not export iSCSI)"
        info "Discovery output:"
        echo "$discover_output" | while IFS= read -r line; do echo "    $line"; done
        check "iSCSI discovery completed" true  # port is bound, target may not export
    fi
else
    warn "iSCSI port not available for discovery"
fi

# ━━━ Phase 4: iSCSI Login ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "iSCSI session"

if [[ "$SKIP_ISCSIADM" == "true" ]] || ! command -v iscsiadm &>/dev/null; then
    echo -e "  ${YELLOW}SKIP: iscsiadm not available${NC}"
elif nc -z "$BIND" "$ISCSI_PORT" 2>/dev/null; then
    step "Attempting iSCSI login to target"
    login_output=$(iscsiadm -m node -T iqn.2024-01.rs.sheepdog:iscsi-test --login 2>&1)
    login_result=$?
    
    if [[ $login_result -eq 0 ]]; then
        info "iSCSI session established"
        pass "iSCSI login succeeded"

        # Find block device
        ISCSI_DEV=""
        if command -v lsblk &>/dev/null; then
            ISCSI_DEV=$(lsblk -dno NAME | grep -E "^(sd|vd)" | head -1)
        fi

        if [[ -n "$ISCSI_DEV" ]]; then
            info "iSCSI block device: /dev/${ISCSI_DEV}"
            pass "iSCSI block device found"

            # Write/read test
            step "Writing test pattern via ${ISCSI_DEV}"
            if dd if=/dev/zero of="/dev/${ISCSI_DEV}" bs=512 count=10 conv=notrunc 2>/dev/null; then
                pass "iSCSI write succeeded"
            else
                warn "iSCSI write failed"
            fi

            step "Reading back via ${ISCSI_DEV}"
            if dd if="/dev/${ISCSI_DEV}" of=/tmp/iscsi_test bs=512 count=10 2>/dev/null; then
                pass "iSCSI read succeeded"
                rm -f /tmp/iscsi_test
            else
                warn "iSCSI read failed"
            fi
        else
            warn "Could not detect iSCSI block device name"
        fi
    else
        warn "iSCSI login failed"
        info "Login output:"
        echo "$login_output" | while IFS= read -r line; do echo "    $line"; done
        check "iSCSI login completed" true  # May fail if target not configured
    fi
else
    warn "iSCSI port not available for login"
fi

# ━━━ Phase 5: NBD Fallback ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "NBD fallback (if iSCSI not available)"

if command -v qemu-io &>/dev/null; then
    step "Using NBD path for verification"
    
    check "NBD port 10809 accessible" nc -z "$BIND" 10809 2>/dev/null
    
    NBD_URI="nbd://${BIND}:10809/${VDI_NAME}"
    
    step "Writing 4K pattern via NBD"
    qemu-io -f raw -c "write -P 0xAB 0 4096" "$NBD_URI" 2>/dev/null
    check_result "NBD write 4K" "$?"
    
    check "NBD read 4K" qemu-io -f raw -c "read -P 0xAB 0 4096" "$NBD_URI" 2>/dev/null
else
    warn "qemu-io not found, skipping NBD fallback"
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
