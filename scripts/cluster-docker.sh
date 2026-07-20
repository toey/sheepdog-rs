#!/usr/bin/env bash
#
# cluster-docker.sh — Start / Stop / Status a 3-node sheepdog-rs cluster in Docker
#
# Usage:
#   ./scripts/cluster-docker.sh start [--nbd] [--nfs] [--iscsi] [--format] [--copies N] [--all]
#   ./scripts/cluster-docker.sh stop
#   ./scripts/cluster-docker.sh status
#   ./scripts/cluster-docker.sh clean        # stop + remove data
#   ./scripts/cluster-docker.sh restart      # stop + start
#   ./scripts/cluster-docker.sh exec <cmd>   # run command in admin container
#   ./scripts/cluster-docker.sh logs [node]  # show logs
#
# Environment variables:
#   ENABLE_NBD=true/false   Enable NBD (default: false)
#   ENABLE_NFS=true/false   Enable NFS (default: false)
#   ENABLE_ISCSI=true/false Enable iSCSI (default: false)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Colors ─────────────────────────────────────────────────────────────
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

# ── Docker Compose helpers ─────────────────────────────────────────────
COMPOSE="docker compose -f ${REPO_ROOT}/docker-compose.yml"

# ── Build ──────────────────────────────────────────────────────────────
do_build() {
    step "Building sheep image..."
    ${COMPOSE} build sheep 2>&1 || {
        err "Build failed"
        exit 1
    }
    info "Build complete"
}

# ── Start ──────────────────────────────────────────────────────────────
do_start() {
    local enable_nbd=false
    local enable_nfs=false
    local enable_iscsi=false
    local do_format=false
    local copies=1
    local enable_all=false

    # Parse flags
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --all)      enable_all=true ;;
            --nbd)      enable_nbd=true ;;
            --nfs)      enable_nfs=true ;;
            --iscsi)    enable_iscsi=true ;;
            --format)   do_format=true ;;
            --copies)   shift; copies="$1" ;;
            *)          err "Unknown flag: $1"; exit 1 ;;
        esac
        shift
    done

    # --all enables every feature
    if $enable_all; then
        enable_nbd=true
        enable_nfs=true
        enable_iscsi=true
    fi

    # Build if needed
    if ! ${COMPOSE} config > /dev/null 2>&1; then
        do_build
    fi

    # Check if already running
    running=$(${COMPOSE} ps --services --filter "status=running" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$running" -eq 4 ]]; then
        warn "All 4 services already running"
        do_status
        return 0
    elif (( running > 0 )); then
        warn "${running} service(s) already running — restarting"
        do_stop
        sleep 2
    fi

    step "Starting 3-node sheepdog cluster in Docker"

    # Set environment variables for the compose file
    export ENABLE_NBD="${enable_nbd}"
    export ENABLE_NFS="${enable_nfs}"
    export ENABLE_ISCSI="${enable_iscsi}"

    ${COMPOSE} up -d 2>&1

    # Wait for nodes to be ready
    info "Waiting for nodes to start..."
    for i in 0 1 2; do
        local port
        case $i in
            0) port=7000 ;;
            1) port=7002 ;;
            2) port=7004 ;;
        esac
        local tries=0
        while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
            sleep 0.5
            tries=$((tries + 1))
            if (( tries > 30 )); then
                err "Node $i failed to start — check logs"
                ${COMPOSE} logs node${i} 2>&1 | tail -20
                exit 1
            fi
        done
        info "Node $i ready  (port ${port})"
    done

    echo ""

    # Format cluster if requested
    if $do_format; then
        sleep 2
        info "Formatting cluster (copies=${copies})"
        if ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 cluster format --copies "$copies" 2>&1; then
            info "Cluster formatted"
        else
            warn "Format failed (cluster may already be formatted)"
        fi
        echo ""
    fi

    do_status
    echo ""

    # Print useful info
    echo -e "${BOLD}Useful commands:${NC}"
    echo "  ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 node list"
    echo "  ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 cluster info"
    echo "  ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 vdi create test 10G"
    echo "  ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 vdi list"
    if $enable_nbd; then
        echo "  qemu-img info nbd://127.0.0.1:10809/test  (host port)"
    fi
    echo "  ${0} stop"
    echo "  ${0} logs [node0|node1|node2|admin]"
}

# ── Stop ───────────────────────────────────────────────────────────────
do_stop() {
    step "Stopping sheepdog cluster"
    ${COMPOSE} down 2>&1 || true
    info "Stopped"
}

# ── Status ─────────────────────────────────────────────────────────────
do_status() {
    echo -e "${BOLD}Cluster Status${NC}"
    echo "────────────────────────────────────────────────"
    printf "  %-10s  %-10s  %-8s  %s\n" "Service" "Image" "Status" "Ports"
    echo "────────────────────────────────────────────────"

    ${COMPOSE} ps --format "table {{.Service}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null | tail -n +3 | sed 's/^/  /' || true
    echo "────────────────────────────────────────────────"
    echo ""

    # Try to show dog node list if node0 is running
    if ${COMPOSE} ps node0 | grep -q "running"; then
        local port0
        port0=$(node_port 0)
        echo ""
        ${COMPOSE} exec node0 ./dog -a 172.20.0.2 -p 7000 node list 2>/dev/null || true
    fi
}

node_port() {
    case $1 in
        0) echo 7000 ;;
        1) echo 7002 ;;
        2) echo 7004 ;;
        *) echo $(( 7000 + $1 * 2 )) ;;
    esac
}

# ── Clean ──────────────────────────────────────────────────────────────
do_clean() {
    do_stop
    echo ""
    info "Removing volumes..."
    ${COMPOSE} down -v 2>&1 || true
    info "Cleaned"
}

# ── Logs ───────────────────────────────────────────────────────────────
do_logs() {
    local service="${1:-}"
    if [[ -n "$service" ]]; then
        ${COMPOSE} logs -f "$service" 2>&1
    else
        ${COMPOSE} logs -f 2>&1
    fi
}

# ── Exec ───────────────────────────────────────────────────────────────
do_exec() {
    local service="${1:-admin}"
    shift
    ${COMPOSE} exec "$service" "$@"
}

# ── Main ───────────────────────────────────────────────────────────────
case "${1:-help}" in
    start)
        shift
        do_start "$@"
        ;;
    stop)
        do_stop
        ;;
    status)
        do_status
        ;;
    clean)
        do_clean
        ;;
    restart)
        shift
        do_stop
        sleep 2
        do_start "$@"
        ;;
    logs)
        shift
        do_logs "$@"
        ;;
    exec)
        shift
        do_exec "$@"
        ;;
    help|--help|-h)
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  start   [--nbd] [--nfs] [--iscsi] [--format] [--copies N]"
        echo "          Start a 3-node cluster in Docker"
        echo "  stop    Stop all containers"
        echo "  status  Show cluster status"
        echo "  clean   Stop + remove all data and volumes"
        echo "  restart Stop + start (passes flags to start)"
        echo "  logs    Show container logs"
        echo "  exec    Run command in a container"
        echo ""
        echo "Environment variables:"
        echo "  ENABLE_NBD=true/false     Enable NBD (default: false)"
        echo "  ENABLE_NFS=true/false     Enable NFS (default: false)"
        echo "  ENABLE_ISCSI=true/false   Enable iSCSI (default: false)"
        echo ""
        echo "Examples:"
        echo "  $0 start --format --copies 1"
        echo "  $0 start --format --nbd"
        echo "  $0 start --format --nbd --nfs --iscsi"
        echo "  $0 start --all --format"
        echo "  $0 stop"
        echo "  $0 clean"
        echo "  $0 logs node0"
        echo "  $0 exec node0 ./dog -a 172.20.0.2 -p 7000 node list"
        ;;
    *)
        err "Unknown command: $1"
        echo "Run '$0 help' for usage"
        exit 1
        ;;
esac
