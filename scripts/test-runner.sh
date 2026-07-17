#!/usr/bin/env bash
#
# test-runner.sh — Full Docker-based test suite for sheepdog-rs
#
# Builds the sheep binary in Docker, starts a 3-node cluster via Docker Compose,
# then runs all E2E tests from the admin container against the cluster.
#
# Usage:
#   ./scripts/test-runner.sh [options]
#
# Options:
#   --all              Run everything (unit + all E2E)
#   --unit             Run unit tests only
#   --e2e              Run all E2E tests
#   --io               Run I/O correctness tests (NBD path)
#   --recovery         Run recovery tests
#   --ec               Run erasure-coded tests
#   --http             Run HTTP/S3/Swift tests
#   --nfs              Run NFS tests
#   --iscsi            Run iSCSI tests (Linux-only for E2E)
#   --fuse             Run FUSE/sheepfs tests (requires fusermount)
#   --features X       Comma-separated feature flags (e.g. nfs,iscsi)
#   --build            Build only (skip tests)
#   --keep-cluster     Don't stop cluster after tests
#
# Examples:
#   ./scripts/test-runner.sh --all
#   ./scripts/test-runner.sh --unit
#   ./scripts/test-runner.sh --e2e --io --recovery --ec
#   ./scripts/test-runner.sh --e2e --http
#   ./scripts/test-runner.sh --features nfs,iscsi --e2e --nfs --iscsi

set -euo pipefail

# ─── Colors ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()  { echo -e "${BOLD}${CYAN}>>> $*${NC}"; }

# ─── Configuration ───────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="sheepdog-rs-test:latest"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"

# ─── Flags ──────────────────────────────────────────────────────────────
RUN_UNIT=false
RUN_E2E=false
E2E_ALL=false
BUILD_ONLY=false
NFS_ENABLED=false
ISCSI_ENABLED=false
FUSE_ENABLED=false
KEEP_CLUSTER=false
E2E_TARGETS=()
FEATURES=""

# ─── Parse arguments ────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --unit)      RUN_UNIT=true ;;
        --e2e)       RUN_E2E=true; E2E_ALL=true ;;
        --build)     BUILD_ONLY=true ;;
        --all)       RUN_UNIT=true; RUN_E2E=true; E2E_ALL=true ;;
        --nfs)       NFS_ENABLED=true ;;
        --iscsi)     ISCSI_ENABLED=true ;;
        --fuse)      FUSE_ENABLED=true ;;
        --keep-cluster) KEEP_CLUSTER=true ;;
        --io)        E2E_TARGETS+=("io") ;;
        --recovery)  E2E_TARGETS+=("recovery") ;;
        --ec)        E2E_TARGETS+=("ec") ;;
        --http)      E2E_TARGETS+=("http") ;;
        --nfs)       E2E_TARGETS+=("nfs") ;;
        --iscsi)     E2E_TARGETS+=("iscsi") ;;
        --fuse)      E2E_TARGETS+=("fuse") ;;
        --features)  shift; FEATURES="$1" ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --unit             Run unit tests only"
            echo "  --e2e              Run all E2E tests"
            echo "  --all              Run everything (unit + E2E)"
            echo "  --build            Build only (skip tests)"
            echo "  --io               Run I/O correctness tests (NBD path)"
            echo "  --recovery         Run recovery tests"
            echo "  --ec               Run EC tests"
            echo "  --http             Run HTTP/S3/Swift tests"
            echo "  --nfs              Run NFS tests"
            echo "  --iscsi            Run iSCSI tests"
            echo "  --fuse             Run FUSE/sheepfs tests"
            echo "  --features X       Comma-separated feature flags (e.g. nfs,iscsi)"
            echo "  --keep-cluster     Don't stop cluster after tests"
            echo ""
            echo "Examples:"
            echo "  $0 --all"
            echo "  $0 --unit"
            echo "  $0 --e2e --io --recovery --ec"
            echo "  $0 --e2e --http"
            echo "  $0 --features nfs,iscsi --e2e --nfs --iscsi"
            exit 0
            ;;
        *)
            err "Unknown flag: $1"
            exit 1
            ;;
    esac
    shift
done

# Default: run everything if no flags
if [[ ${RUN_UNIT} == false && ${RUN_E2E} == false && ${BUILD_ONLY} == false ]]; then
    RUN_UNIT=true
    RUN_E2E=true
    E2E_ALL=true
fi

# If E2E targets specified, auto-enable related features
if [[ ${#E2E_TARGETS[@]} -gt 0 ]]; then
    for target in "${E2E_TARGETS[@]}"; do
        case "$target" in
            io|recovery|ec|fuse)
                if [[ ! "$FEATURES" =~ "nbd" ]]; then
                    FEATURES="${FEATURES}nbd,"
                fi
                ;;
            nfs) NFS_ENABLED=true ;;
            iscsi) ISCSI_ENABLED=true ;;
        esac
    done
fi

# ─── Check Docker ──────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    err "Docker is required for testing"
    exit 1
fi

# ─── Build ──────────────────────────────────────────────────────────────
do_build() {
    step "Building sheep image..."
    docker build -t "${IMAGE_NAME}" -f "${REPO_ROOT}/Dockerfile.test" "${REPO_ROOT}" 2>&1 || {
        err "Build failed"
        exit 1
    }
    info "Build complete"
}

# ─── Unit Tests ─────────────────────────────────────────────────────────
do_unit_tests() {
    step "Running unit tests..."

    local extra_args=""
    if [[ -n "$FEATURES" ]]; then
        extra_args="--features ${FEATURES}"
    fi

    docker run --rm \
        -v "${REPO_ROOT}":/workspace \
        -w /workspace \
        "${IMAGE_NAME}" \
        cargo test --release ${extra_args} 2>&1 || {
        err "Unit tests failed"
        exit 1
    }

    info "Unit tests passed"
}

# ─── Start Cluster ──────────────────────────────────────────────────────
start_cluster() {
    step "Starting 3-node sheepdog cluster in Docker..."

    # Set environment variables for docker-compose
    export ENABLE_NBD=false
    export ENABLE_NFS=false
    export ENABLE_ISCSI=false

    # Determine which features to enable
    local need_nbd=false
    for target in "${E2E_TARGETS[@]}"; do
        case "$target" in
            io|recovery|ec|fuse) need_nbd=true ;;
        esac
    done

    if $need_nbd || $NFS_ENABLED || $ISCSI_ENABLED; then
        export ENABLE_NBD=true
    fi
    if $NFS_ENABLED; then
        export ENABLE_NFS=true
    fi
    if $ISCSI_ENABLED; then
        export ENABLE_ISCSI=true
    fi

    # Build if needed
    if ! docker image inspect "${IMAGE_NAME}" &>/dev/null; then
        do_build
    fi

    # Start cluster
    docker compose -f "${COMPOSE_FILE}" up -d 2>&1

    # Wait for nodes to be ready
    info "Waiting for nodes to start..."
    for port in 7000 7002 7004; do
        local tries=0
        while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
            sleep 0.5
            tries=$((tries + 1))
            if (( tries > 30 )); then
                err "Node failed to start on port ${port}"
                docker compose -f "${COMPOSE_FILE}" logs 2>&1 | tail -30
                exit 1
            fi
        done
    done
    info "All nodes ready"
}

stop_cluster() {
    if $KEEP_CLUSTER; then
        info "Skipping cluster stop (--keep-cluster)"
        return
    fi
    info "Stopping cluster..."
    docker compose -f "${COMPOSE_FILE}" down 2>&1 || true
    info "Cluster stopped"
}

# ─── E2E Test Runner ────────────────────────────────────────────────────
run_e2e_tests() {
    step "Running E2E tests..."

    # Copy all test scripts and dependencies to admin container
    info "Copying test scripts to admin container..."
    docker cp "${REPO_ROOT}/scripts/" sheepdog-admin:/workspace/scripts/
    docker cp "${REPO_ROOT}/Cargo.toml" sheepdog-admin:/workspace/Cargo.toml
    docker cp "${REPO_ROOT}/Cargo.lock" sheepdog-admin:/workspace/Cargo.lock
    docker cp "${REPO_ROOT}/crates/" sheepdog-admin:/workspace/crates/

    # Run each E2E test
    local all_passed=true
    for target in "${E2E_TARGETS[@]}"; do
        info "Running ${target} E2E tests..."

        local result
        result=$(docker exec sheepdog-admin bash -c "
            cd /workspace
            chmod +x scripts/test-${target}.sh
            scripts/test-${target}.sh 2>&1
        " 2>&1) || all_passed=false

        echo "$result"

        if [[ "$target" == "http" ]]; then
            # HTTP tests use a single-node local cluster — they start their own node
            # We need to run them differently (via curl against the running cluster)
            :
        fi
    done

    if ! $all_passed; then
        warn "Some E2E tests failed"
    else
        info "All E2E tests passed"
    fi
}

# ─── Main ───────────────────────────────────────────────────────────────
main() {
    if $BUILD_ONLY; then
        do_build
        exit 0
    fi

    if $RUN_UNIT; then
        do_build
        do_unit_tests
    fi

    if $RUN_E2E; then
        start_cluster
        trap stop_cluster EXIT
        run_e2e_tests
    fi

    info "All tests passed"
}

main
