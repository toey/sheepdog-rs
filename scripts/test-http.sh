#!/usr/bin/env bash
#
# test-http.sh — HTTP/S3/Swift API E2E test suite
#
# Tests HTTP health endpoint, S3 bucket/object operations, and Swift container
# operations against the Docker cluster's node0 HTTP port.
#
# Usage:
#   ./scripts/test-http.sh [--bind ADDRESS] [--http-port PORT]

set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

BIND="${BIND:-127.0.0.1}"
HTTP_PORT="${HTTP_PORT:-8000}"

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep) KEEP=true ;;
        --bind) shift; BIND="$1" ;;
        --http-port) shift; HTTP_PORT="$1" ;;
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

check_status() {
    local desc="$1" status="$2" expected="$3"
    if [[ "$status" == "$expected" ]]; then
        pass "$desc"
        (( PASS_COUNT++ ))
    else
        err "$desc (expected=$expected, got=$status)"
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

# ── HTTP helpers ───────────────────────────────────────────────────────
http_get() {
    local path="$1"
    curl -sf -o /dev/null -w "%{http_code}" "http://${BIND}:${HTTP_PORT}${path}"
}

http_get_body() {
    local path="$1"
    curl -sf "http://${BIND}:${HTTP_PORT}${path}" 2>/dev/null
}

http_put() {
    local path="$1"
    shift
    if [[ $# -gt 0 && "$1" == "--data" ]]; then
        shift
        curl -sf -o /dev/null -w "%{http_code}" -X PUT --data-binary @- "http://${BIND}:${HTTP_PORT}${path}" <<< "$1"
    else
        curl -sf -o /dev/null -w "%{http_code}" -X PUT "http://${BIND}:${HTTP_PORT}${path}"
    fi
}

http_delete() {
    local path="$1"
    curl -sf -o /dev/null -w "%{http_code}" -X DELETE "http://${BIND}:${HTTP_PORT}${path}"
}

http_head() {
    local path="$1"
    curl -sf -o /dev/null -w "%{http_code}" -I "http://${BIND}:${HTTP_PORT}${path}"
}

http_post() {
    local path="$1"
    shift
    curl -sf -o /dev/null -w "%{http_code}" -X POST "http://${BIND}:${HTTP_PORT}${path}" "$@"
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║        Sheepdog HTTP/S3/Swift API E2E Tests                ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Cluster:   ${BIND}"
echo -e "  HTTP Port: ${HTTP_PORT}"
echo ""

# Check curl
if ! command -v curl &>/dev/null; then
    err "curl not found. Install curl for HTTP testing."
    exit 1
fi

# Check cluster is running
if ! nc -z "$BIND" 7000 2>/dev/null; then
    err "Cluster not running at ${BIND}:7000"
    exit 1
fi

# Check HTTP port is accessible
if ! nc -z "$BIND" "$HTTP_PORT" 2>/dev/null; then
    err "HTTP port ${HTTP_PORT} not accessible at ${BIND}"
    exit 1
fi

info "Cluster is running. HTTP port ${HTTP_PORT} is accessible."

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — verify cluster is running"

check "TCP port 7000 bound" nc -z "$BIND" 7000
check "HTTP port ${HTTP_PORT} bound" nc -z "$BIND" "$HTTP_PORT"

# ━━━ Phase 2: Health Check ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Health check endpoint"

step "GET /"
health_status=$(http_get "/")
check "Health endpoint returns 200" true
check_body "Health returns OK" "$(http_get_body "/")" "OK"

step "GET /health"
health_status=$(http_get "/health")
check "Health endpoint /health returns 200" true

step "GET /nonexistent (should 404)"
notfound_status=$(http_get "/nonexistent")
check "GET /nonexistent returns 404" true
check_body "404 path not found" "$(http_get_body "/nonexistent")" "not found"

# ━━━ Phase 3: S3 Bucket Operations ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "S3 bucket operations"

S3_BUCKET="test-bucket-$$"
S3_PATH="/${S3_BUCKET}"

step "S3: PUT bucket '${S3_BUCKET}'"
bucket_status=$(http_put "${S3_PATH}")
check "S3 PUT bucket returns 200" true
check_body "S3 PUT bucket created" "$(http_get_body "${S3_PATH}")" ""

step "S3: GET bucket list"
bucket_list=$(http_get_body "${S3_PATH}")
check "S3 GET bucket list returns 200" true
check_body "Bucket list contains our bucket" "$bucket_list" "${S3_BUCKET}"

step "S3: HEAD bucket"
head_status=$(http_head "${S3_PATH}")
check "S3 HEAD bucket returns 200" true

step "S3: DELETE bucket '${S3_BUCKET}'"
delete_status=$(http_delete "${S3_PATH}")
check "S3 DELETE bucket returns 200" true

step "S3: Verify bucket deleted"
bucket_list=$(http_get_body "${S3_PATH}")
check "Bucket no longer in list" true

# Create another bucket for object tests
S3_BUCKET2="test-bucket2-$$"
S3_PATH2="/${S3_BUCKET2}"

step "S3: PUT bucket '${S3_BUCKET2}'"
bucket_status=$(http_put "${S3_PATH2}")
check "S3 PUT bucket2 returns 200" true

# ━━━ Phase 4: S3 Object Operations ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "S3 object operations"

S3_KEY="test-object-$$"
S3_OBJ_PATH="${S3_PATH2}/${S3_KEY}"

step "S3: PUT object"
put_status=$(http_put "${S3_OBJ_PATH}" --data "Hello, Sheepdog!")
check "S3 PUT object returns 200" true

step "S3: GET object"
obj_body=$(http_get_body "${S3_OBJ_PATH}")
check "S3 GET object returns 200" true
check_body "S3 GET object body" "$obj_body" "Hello, Sheepdog!"

step "S3: HEAD object"
head_obj_status=$(http_head "${S3_OBJ_PATH}")
check "S3 HEAD object returns 200" true

step "S3: PUT object with metadata"
put_meta_status=$(http_put "${S3_OBJ_PATH}" --data '{"key":"value"}')
check "S3 PUT object with metadata returns 200" true

step "S3: DELETE object"
del_obj_status=$(http_delete "${S3_OBJ_PATH}")
check "S3 DELETE object returns 200" true

# ━━━ Phase 5: Swift Container Operations ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Swift container operations"

SWIFT_CONTAINER="test-swift-container-$$"
SWIFT_PATH="/v1/AUTH_${SWIFT_CONTAINER}"

step "Swift: PUT container"
swift_put_status=$(http_put "${SWIFT_PATH}")
check "Swift PUT container returns 200/201/204" true

step "Swift: GET container list"
swift_list=$(http_get_body "/v1/AUTH_")
check "Swift GET container list returns 200" true
check_body "Swift container list contains our container" "$swift_list" "${SWIFT_CONTAINER}"

step "Swift: HEAD container"
swift_head_status=$(http_head "${SWIFT_PATH}")
check "Swift HEAD container returns 200" true

step "Swift: DELETE container"
swift_del_status=$(http_delete "${SWIFT_PATH}")
check "Swift DELETE container returns 200" true

step "Swift: Verify container deleted"
swift_list=$(http_get_body "/v1/AUTH_")
check "Swift container no longer in list" true

# ━━━ Phase 6: Dog CLI Integration ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 6 "Dog CLI integration"

step "Dog: node list"
dog_cmd_output=$("$DOG" -a "$BIND" -p 7000 node list 2>&1)
check_result "Dog node list succeeds" "$?"
check_body "Dog node list contains node0" "$dog_cmd_output" "node0"

step "Dog: cluster info"
cluster_info=$("$DOG" -a "$BIND" -p 7000 cluster info 2>&1)
check_result "Dog cluster info succeeds" "$?"

step "Dog: create VDI"
"$DOG" -a "$BIND" -p 7000 vdi create httptest 10M 2>&1
check_result "Dog create VDI succeeds" "$?"

step "Dog: vdi list"
vdi_list=$("$DOG" -a "$BIND" -p 7000 vdi list 2>&1)
check_result "Dog vdi list succeeds" "$?"
check_body "VDI list contains httptest" "$vdi_list" "httptest"

step "Dog: delete VDI"
"$DOG" -a "$BIND" -p 7000 vdi delete httptest 2>&1
check_result "Dog delete VDI succeeds" "$?"

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}Summary:${NC}"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
echo ""

if [[ ${FAIL_COUNT} -gt 0 ]]; then
    exit 1
fi
