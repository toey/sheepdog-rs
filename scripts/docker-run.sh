#!/usr/bin/env bash
# docker-run.sh — Docker wrapper for running Cargo commands
#
# Usage:
#   ./scripts/docker-run.sh build --release
#   ./scripts/docker-run.sh build -p sheepdog-dpdk
#   ./scripts/docker-run.sh test --all-features
#   ./scripts/docker-run.sh test -p sheep

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Try ./docker first (Go Mach-O binary), then fallback to docker/podman
if [[ -x "${REPO_ROOT}/./docker" ]]; then
    CMD="${REPO_ROOT}/./docker"
elif command -v docker &>/dev/null; then
    CMD="docker"
elif command -v podman &>/dev/null; then
    CMD="podman"
else
    echo "ERROR: No Docker/Podman found" >&2
    exit 1
fi

exec ${CMD} run --rm -v "${REPO_ROOT}":/workspace -w /workspace rust "$@"
