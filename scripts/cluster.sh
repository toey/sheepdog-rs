#!/usr/bin/env bash
#
# cluster.sh — Start / Stop / Status a 3-node sheepdog-rs cluster on localhost
#
# Usage:
#   ./scripts/cluster.sh start   [--nbd] [--nfs] [--format] [--copies N]
#   ./scripts/cluster.sh stop
#   ./scripts/cluster.sh status
#   ./scripts/cluster.sh clean       # stop + remove data
#   ./scripts/cluster.sh restart     # stop + start
#
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/defaults.sh"
LOG_DIR="${DATA_ROOT}/logs"

# Resolve binary path (release → debug → PATH)
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${REPO_ROOT}/target/release/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/release/sheep"
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/debug/sheep"
    DOG="${REPO_ROOT}/target/debug/dog"
else
    SHEEP="sheep"
    DOG="dog"
fi

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

# ── Helpers ────────────────────────────────────────────────────────────
node_port()  { echo $(( BASE_PORT + $1 * 2 )); }
node_http()  { echo $(( HTTP_BASE_PORT + $1 * 2 )); }
node_dir()   { echo "${DATA_ROOT}/node${1}"; }
node_log()   { echo "${LOG_DIR}/node${1}.log"; }
node_pid()   { echo "${DATA_ROOT}/node${1}.pid"; }

is_running() {
    local pidfile
    pidfile="$(node_pid "$1")"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

wait_for_port() {
    local port=$1 timeout=${2:-10} elapsed=0
    while ! nc -z "$BIND" "$port" 2>/dev/null; do
        sleep 0.3
        elapsed=$(( elapsed + 1 ))
        if (( elapsed > timeout * 3 )); then
            return 1
        fi
    done
    return 0
}

# ── Start ──────────────────────────────────────────────────────────────
do_start() {
    local enable_nbd=false
    local enable_nfs=false
    local do_format=false
    local copies=1

    # Parse flags
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --nbd)      enable_nbd=true ;;
            --nfs)      enable_nfs=true ;;
            --format)   do_format=true ;;
            --copies)   shift; copies="$1" ;;
            *)          err "Unknown flag: $1"; exit 1 ;;
        esac
        shift
    done

    # Check binary
    if ! command -v "$SHEEP" &>/dev/null && [[ ! -x "$SHEEP" ]]; then
        err "sheep binary not found. Run: cargo build --release"
        exit 1
    fi

    # Check if already running
    local already_running=0
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        if is_running "$i"; then
            (( already_running++ ))
        fi
    done
    if (( already_running == NUM_NODES )); then
        warn "All ${NUM_NODES} nodes already running"
        do_status
        return 0
    elif (( already_running > 0 )); then
        warn "${already_running} node(s) already running — stopping first"
        do_stop
        sleep 1
    fi

    echo -e "${BOLD}${CYAN}Starting ${NUM_NODES}-node sheepdog cluster${NC}"
    echo -e "  Binary:  ${SHEEP}"
    echo -e "  Data:    ${DATA_ROOT}"
    echo ""

    mkdir -p "$LOG_DIR"

    # ── Node 0 (seed) ──
    local port0 http0 dir0 log0 pid0
    port0=$(node_port 0)
    http0=$(node_http 0)
    dir0=$(node_dir 0)
    log0=$(node_log 0)
    pid0=$(node_pid 0)

    mkdir -p "$dir0"

    local extra_args=""
    if $enable_nbd; then
        extra_args="--nbd --nbd-port $NBD_PORT"
    fi
    if $enable_nfs; then
        extra_args="$extra_args --nfs --nfs-port $NFS_PORT"
    fi

    info "Starting node 0  (port=${port0}, http=${http0})"
    # shellcheck disable=SC2086
    "$SHEEP" --cluster-driver sdcluster \
        -b "$BIND" -p "$port0" \
        --http-port "$http0" \
        -l info \
        $extra_args \
        "$dir0" \
        > "$log0" 2>&1 &
    echo $! > "$pid0"

    if ! wait_for_port "$port0" 10; then
        err "Node 0 failed to start — check $(node_log 0)"
        exit 1
    fi
    info "Node 0 ready  (pid=$(cat "$pid0"))"

    # ── Node 1..N (join via seed) ──
    for i in $(seq 1 $(( NUM_NODES - 1 ))); do
        local port http dir log pid
        port=$(node_port "$i")
        http=$(node_http "$i")
        dir=$(node_dir "$i")
        log=$(node_log "$i")
        pid=$(node_pid "$i")

        mkdir -p "$dir"

        info "Starting node ${i}  (port=${port}, http=${http})"
        "$SHEEP" --cluster-driver sdcluster \
            -b "$BIND" -p "$port" \
            --seed "${BIND}:${port0}" \
            --http-port "$http" \
            -l info \
            "$dir" \
            > "$log" 2>&1 &
        echo $! > "$pid"

        if ! wait_for_port "$port" 10; then
            err "Node ${i} failed to start — check $(node_log "$i")"
            exit 1
        fi
        info "Node ${i} ready  (pid=$(cat "$pid"))"
    done

    echo ""

    # ── Format cluster ──
    if $do_format; then
        sleep 1
        info "Formatting cluster (copies=${copies})"
        if "$DOG" -a "$BIND" -p "$port0" cluster format --copies "$copies" 2>&1; then
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
    echo "  $DOG node list"
    echo "  $DOG cluster info"
    echo "  $DOG vdi create test 10G"
    echo "  $DOG vdi list"
    if $enable_nbd; then
        echo "  qemu-img info nbd://${BIND}:${NBD_PORT}/test"
    fi
    echo "  ./scripts/cluster.sh stop"
}

# ── Stop ───────────────────────────────────────────────────────────────
do_stop() {
    echo -e "${BOLD}${CYAN}Stopping sheepdog cluster${NC}"

    local stopped=0
    # Stop in reverse order (workers first, seed last)
    for i in $(seq $(( NUM_NODES - 1 )) -1 0); do
        local pidfile
        pidfile=$(node_pid "$i")
        if [[ -f "$pidfile" ]]; then
            local pid
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                info "Stopping node ${i}  (pid=${pid})"
                kill "$pid" 2>/dev/null || true
                # Wait for graceful shutdown (max 5s)
                local waited=0
                while kill -0 "$pid" 2>/dev/null && (( waited < 50 )); do
                    sleep 0.1
                    (( waited++ ))
                done
                if kill -0 "$pid" 2>/dev/null; then
                    warn "Node ${i} didn't exit gracefully — sending SIGKILL"
                    kill -9 "$pid" 2>/dev/null || true
                fi
                (( stopped++ ))
            fi
            rm -f "$pidfile"
        fi
    done

    if (( stopped == 0 )); then
        warn "No running nodes found"
    else
        info "Stopped ${stopped} node(s)"
    fi
}

# ── Status ─────────────────────────────────────────────────────────────
do_status() {
    echo -e "${BOLD}Cluster Status${NC}"
    echo "────────────────────────────────────────────────"
    printf "  %-6s  %-8s  %-8s  %-8s  %s\n" "Node" "Port" "HTTP" "PID" "Status"
    echo "────────────────────────────────────────────────"

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local port http status pid_str
        port=$(node_port "$i")
        http=$(node_http "$i")

        if is_running "$i"; then
            pid_str=$(cat "$(node_pid "$i")")
            status="${GREEN}running${NC}"
        else
            pid_str="—"
            status="${RED}stopped${NC}"
        fi

        printf "  %-6s  %-8s  %-8s  %-8s  " "node${i}" "$port" "$http" "$pid_str"
        echo -e "$status"
    done
    echo "────────────────────────────────────────────────"
    echo "  Data: ${DATA_ROOT}"
    echo "  Logs: ${LOG_DIR}"

    # Try to show dog node list if any node is running
    if is_running 0; then
        local port0
        port0=$(node_port 0)
        echo ""
        "$DOG" -a "$BIND" -p "$port0" node list 2>/dev/null || true
    fi
}

# ── Clean ──────────────────────────────────────────────────────────────
do_clean() {
    do_stop
    echo ""
    if [[ -d "$DATA_ROOT" ]]; then
        info "Removing ${DATA_ROOT}"
        rm -rf "$DATA_ROOT"
        info "Cleaned"
    else
        warn "Nothing to clean — ${DATA_ROOT} does not exist"
    fi
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
        sleep 1
        do_start "$@"
        ;;
    help|--help|-h)
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  start   [--nbd] [--nfs] [--format] [--copies N]"
        echo "          Start a ${NUM_NODES}-node cluster on localhost"
        echo "  stop    Stop all nodes"
        echo "  status  Show cluster status"
        echo "  clean   Stop + remove all data"
        echo "  restart Stop + start (passes flags to start)"
        echo ""
        echo "Examples:"
        echo "  $0 start --format --copies 1"
        echo "  $0 start --format --nbd"
        echo "  $0 stop"
        echo "  $0 clean"
        ;;
    *)
        err "Unknown command: $1"
        echo "Run '$0 help' for usage"
        exit 1
        ;;
esac
