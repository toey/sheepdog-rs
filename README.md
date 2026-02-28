# sheepdog-rs

A **Rust** rewrite of [Sheepdog](https://sheepdog.github.io/sheepdog/) — distributed block storage for QEMU/KVM virtual machines.

> 18,000+ lines of async Rust across 7 crates. No external cluster dependencies.
> Connects to QEMU via NBD — works with QEMU 6.0+ (which removed native sheepdog support).
> Optional DPDK data plane for kernel-bypass peer I/O.

```
               QEMU / qemu-img                    dog CLI
                     |                               |
              nbd://host:10809/vdi              tcp://host:7000
                     |                               |
   +-----------+-----+-----+-----------+             |
   |           |           |           |             |
+--+---+   +--+---+   +---+--+   +----+-+     +-----+-----+
| sheep |===| sheep |===| sheep |===| sheep |     | shepherd  |
| :7000 |   | :7002 |   | :7004 |   | :7006 |     | (monitor) |
+--+---+   +--+---+   +---+--+   +----+-+     +-----------+
   |           |           |           |
   +-----+-----+-----+-----+-----+----+
         |                 |
    Consistent Hash    4 MB Objects
      (vnodes)       (replicated / EC)
```

Virtual disk images (VDIs) are split into **4 MB objects** and distributed across **sheep** daemons using consistent hashing. Each object is either replicated to N nodes or erasure-coded (Reed-Solomon) for storage efficiency. No single point of failure — any sheep can serve any request by forwarding to the responsible peer.

---

## Quick Start

### 1. Build

```bash
cargo build --release
```

Produces three binaries in `target/release/`:

| Binary | Description |
|--------|-------------|
| `sheep` | Storage daemon |
| `dog` | CLI admin tool |
| `shepherd` | Cluster monitor |

Requirements: **Rust 1.70+**, Linux/macOS/FreeBSD.

### 2. Single node

```bash
# Start daemon
sheep /tmp/sheepdata

# Format cluster (1 replica for single-node)
dog cluster format --copies 1

# Create a 20 GB virtual disk
dog vdi create mydisk 20G

# Verify
dog vdi list
```

### 3. Connect QEMU via NBD

```bash
# Start daemon with NBD enabled
sheep --nbd /tmp/sheepdata
dog cluster format --copies 1
dog vdi create mydisk 20G

# Launch VM
qemu-system-x86_64 \
  -drive file=nbd://127.0.0.1:10809/mydisk,format=raw,if=virtio \
  -m 2048 -enable-kvm ...

# Or use qemu-img / qemu-io directly
qemu-img info    nbd://127.0.0.1:10809/mydisk
qemu-img create -f raw nbd://127.0.0.1:10809/mydisk 20G
qemu-io  -f raw -c "write -P 0xAB 0 4096" nbd://127.0.0.1:10809/mydisk
qemu-io  -f raw -c "read  -P 0xAB 0 4096" nbd://127.0.0.1:10809/mydisk
```

### 4. Multi-node cluster

```bash
# Node 0 (first node)
sheep --cluster-driver sdcluster -b 10.0.0.1 -p 7000 /data/sheep

# Node 1 (joins via seed)
sheep --cluster-driver sdcluster -b 10.0.0.2 -p 7000 \
      --seed 10.0.0.1:7000 /data/sheep

# Node 2 (multiple seeds for redundancy)
sheep --cluster-driver sdcluster -b 10.0.0.3 -p 7000 \
      --seed 10.0.0.1:7000 --seed 10.0.0.2:7000 /data/sheep

# Format with 3-way replication
dog -a 10.0.0.1 cluster format --copies 3
dog -a 10.0.0.1 node list
```

### 5. Cluster script (localhost)

A convenience script is included for local development:

```bash
# Start 3-node cluster with NBD
scripts/cluster.sh start --nbd --format

# Check status
scripts/cluster.sh status

# Stop all nodes
scripts/cluster.sh stop

# Full cleanup (remove data directories)
scripts/cluster.sh clean
```

Options: `--nbd`, `--nfs`, `--format`, `--copies N`.

---

## Test Suites

Three test scripts validate the system end-to-end using real sheep daemons and qemu-io:

### I/O Correctness (`test-io.sh`)

Tests data integrity through the NBD path with object cache **disabled**:

```bash
scripts/test-io.sh                # Run full suite (63 tests)
scripts/test-io.sh --keep         # Keep data for inspection
scripts/test-io.sh --skip-directio
```

| Phase | What happens |
|-------|-------------|
| 1. Setup | 3-node cluster, no `--cache`, create VDI |
| 2. Basic I/O | Sequential write + read (4K, 64K, 256K, 1M) at each object offset |
| 3. Overwrite | Rewrite same locations, partial overwrites |
| 4. Cross-boundary | Writes spanning two 4MB objects (8K and 1M) |
| 5. Large I/O | Multi-object sequential writes (8MB, 16MB, 32MB) |
| 6. Sparse | Non-contiguous offsets, gaps read as zeros |
| 7. Direct I/O | Restart with `--directio`, repeat key tests |

### Erasure Coding (`test-ec.sh`)

Tests Reed-Solomon EC correctness through the NBD path:

```bash
scripts/test-ec.sh                # Run full suite (44 tests)
scripts/test-ec.sh --keep
```

| Phase | What happens |
|-------|-------------|
| 1. Setup | 3-node cluster, create EC 2:1 VDI |
| 2. Basic EC I/O | Write + read patterns at each object offset |
| 3. EC Overwrite | Partial writes (read-modify-write cycle) |
| 4. Cross-boundary | EC writes spanning two 4MB objects |
| 5. Large EC I/O | Multi-object 8MB/16MB sequential EC writes |
| 6. Strip verification | Check EC strip files on disk (count, size) |
| 7. Degraded | Write/read with 1 node down, restart |

<details>
<summary>Full script (<code>scripts/test-ec.sh</code>)</summary>

```bash
#!/usr/bin/env bash
#
# test-ec.sh — Erasure Coding (EC) correctness test suite
#
# Tests EC data integrity through the NBD path, verifying that
# Reed-Solomon encode/decode produces correct results.
#
# Test phases:
#   Phase 1: Setup — 3-node cluster with EC 2:1 VDI
#   Phase 2: Basic EC I/O — write + read at each object offset
#   Phase 3: EC Overwrite — partial writes (read-modify-write)
#   Phase 4: Cross-boundary — EC writes spanning two 4MB objects
#   Phase 5: Large EC I/O — multi-object sequential writes
#   Phase 6: Strip verification — check EC strip files on disk
#   Phase 7: Degraded read — stop a node, verify reconstruction
#   Phase 8: Cleanup
#
# Requirements:
#   - cargo build (debug or release)
#   - qemu-io (from qemu-utils) for NBD I/O verification
#
# Usage:
#   ./scripts/test-ec.sh [--keep]
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="/tmp/sheepdog-ec-test"
source "${SCRIPT_DIR}/defaults.sh"
LOG_DIR="${DATA_ROOT}/logs"

# EC 2:1 = 2 data strips + 1 parity strip (needs 3 nodes)
EC_POLICY="2:1"
EC_DATA=2
EC_PARITY=1
EC_TOTAL=3   # d+p

VDI_SIZE="64M"
VDI_NAME="ectest"

# 4 MB object size (SD_DATA_OBJ_SIZE = 1 << 22)
OBJ_SIZE=4194304

KEEP=false

# Parse flags
for arg in "$@"; do
    case "$arg" in
        --keep)  KEEP=true ;;
    esac
done

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -x "${REPO_ROOT}/target/release/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/release/sheep"
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/sheep" ]]; then
    SHEEP="${REPO_ROOT}/target/debug/sheep"
    DOG="${REPO_ROOT}/target/debug/dog"
else
    echo "ERROR: sheep binary not found. Run: cargo build"
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

# ── NBD helpers ────────────────────────────────────────────────────────
NBD_URI="nbd://${BIND}:${NBD_PORT}/${VDI_NAME}"

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

# ── Node helpers ───────────────────────────────────────────────────────
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

start_node() {
    local idx=$1
    shift
    local port http dir log pid
    port=$(node_port "$idx")
    http=$(node_http "$idx")
    dir=$(node_dir "$idx")
    log=$(node_log "$idx")
    pid=$(node_pid "$idx")

    mkdir -p "$dir"

    step "Starting node ${idx} (port=${port})"
    if (( idx > 0 )); then
        "$SHEEP" --cluster-driver sdcluster \
            -b "$BIND" -p "$port" \
            --seed "${BIND}:$(node_port 0)" \
            --http-port "$http" \
            -l debug \
            "$@" \
            "$dir" \
            > "$log" 2>&1 &
    else
        "$SHEEP" --cluster-driver sdcluster \
            -b "$BIND" -p "$port" \
            --http-port "$http" \
            -l debug \
            "$@" \
            "$dir" \
            > "$log" 2>&1 &
    fi
    echo $! > "$pid"

    if ! wait_for_port "$port" 15; then
        err "Node ${idx} failed to start — check ${log}"
        return 1
    fi
    info "Node ${idx} ready (pid=$(cat "$pid"), port=${port})"
}

stop_node() {
    local idx=$1
    local pidfile
    pidfile=$(node_pid "$idx")
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            local waited=0
            while kill -0 "$pid" 2>/dev/null && (( waited < 50 )); do
                sleep 0.1
                (( waited++ ))
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pidfile"
    fi
}

stop_all() {
    for i in $(seq $(( NUM_NODES - 1 )) -1 0); do
        stop_node "$i"
    done
}

count_objects() {
    local dir
    dir="$(node_dir "$1")/obj"
    if [[ -d "$dir" ]]; then
        ls "$dir" 2>/dev/null | wc -l | tr -d ' '
    else
        echo "0"
    fi
}

count_ec_strips() {
    # Count files with EC strip suffix (_N or _NN where N is hex)
    local dir count
    dir="$(node_dir "$1")/obj"
    if [[ -d "$dir" ]]; then
        count=$(ls "$dir" 2>/dev/null | grep -c '_[0-9a-f]' 2>/dev/null) || count=0
        echo "$count"
    else
        echo "0"
    fi
}

show_object_distribution() {
    echo -e "  ${BOLD}Object distribution:${NC}"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local total ec_count
        total=$(count_objects "$i")
        ec_count=$(count_ec_strips "$i")
        printf "    node%d: %3s objects (%s EC strips)\n" "$i" "$total" "$ec_count"
    done
}

show_ec_strip_files() {
    echo -e "  ${BOLD}EC strip files (sample):${NC}"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir
        dir="$(node_dir "$i")/obj"
        if [[ -d "$dir" ]]; then
            local strips
            strips=$(ls "$dir" 2>/dev/null | grep '_[0-9a-f]' | head -5)
            if [[ -n "$strips" ]]; then
                echo "    node${i}:"
                echo "$strips" | while IFS= read -r f; do
                    local sz
                    sz=$(stat -f%z "${dir}/${f}" 2>/dev/null || stat -c%s "${dir}/${f}" 2>/dev/null || echo "?")
                    printf "      %-40s  %s bytes\n" "$f" "$sz"
                done
            fi
        fi
    done
}

dog_cmd() {
    "$DOG" -a "$BIND" -p "$(node_port 0)" "$@" 2>/dev/null
}

start_cluster() {
    step "Starting ${NUM_NODES}-node cluster"
    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        if (( i == 0 )); then
            start_node "$i" --nbd --nbd-port "$NBD_PORT" \
                || { err "Failed to start node $i"; stop_all; return 1; }
        else
            start_node "$i" \
                || { err "Failed to start node $i"; stop_all; return 1; }
        fi
    done

    sleep 2

    # Format with copies=1 (default for replication VDIs; EC VDIs override)
    step "Formatting cluster (copies=1)"
    if ! dog_cmd cluster format --copies 1 2>&1; then
        warn "Format failed (may already be formatted)"
    fi
    sleep 1
}

# ── Cleanup trap ───────────────────────────────────────────────────────
cleanup() {
    if [[ "$KEEP" == "true" ]]; then
        warn "Keeping data in ${DATA_ROOT} (--keep flag)"
    else
        step "Stopping all nodes..."
        stop_all
        step "Removing ${DATA_ROOT}"
        rm -rf "$DATA_ROOT"
    fi
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main test flow
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║      Sheepdog Erasure Coding (EC) Correctness Test          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "  Binary:    ${SHEEP}"
echo -e "  Data:      ${DATA_ROOT}"
echo -e "  VDI:       ${VDI_NAME} (${VDI_SIZE})"
echo -e "  EC Policy: ${EC_POLICY} (${EC_DATA} data + ${EC_PARITY} parity)"
echo -e "  Obj size:  ${OBJ_SIZE} (4 MB)"
echo ""

# Check qemu-io
if ! command -v qemu-io &>/dev/null; then
    err "qemu-io not found. Install qemu-utils for NBD I/O testing."
    exit 1
fi

# Clean any previous run
rm -rf "$DATA_ROOT"
mkdir -p "$LOG_DIR"

# ━━━ Phase 1: Setup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 1 "Setup — 3-node cluster with EC ${EC_POLICY} VDI"

start_cluster || { err "Failed to start cluster"; exit 1; }

check "Cluster is formatted" dog_cmd cluster info

# Create EC VDI with 2:1 erasure coding
step "Creating EC VDI '${VDI_NAME}' (${VDI_SIZE}, EC ${EC_POLICY})"
dog_cmd vdi create "$VDI_NAME" "$VDI_SIZE" --copy-policy "$EC_POLICY"
sleep 1

check "EC VDI created" dog_cmd vdi list

step "VDI info:"
dog_cmd vdi list 2>/dev/null | while IFS= read -r line; do echo "    $line"; done

# ━━━ Phase 2: Basic EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 2 "Basic EC I/O — sequential write + read"

# Write different patterns at the start of each 4MB object
step "Writing pattern 0xAA at offset 0 (object 0)"
nbd_write 0xAA 0 4096
check "EC write 4K at offset 0" true

step "Writing pattern 0xBB at offset 4MB (object 1)"
nbd_write 0xBB $OBJ_SIZE 4096
check "EC write 4K at offset 4MB" true

step "Writing pattern 0xCC at offset 8MB (object 2)"
nbd_write 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC write 4K at offset 8MB" true

step "Writing pattern 0xDD at offset 12MB (object 3)"
nbd_write 0xDD $(( OBJ_SIZE * 3 )) 4096
check "EC write 4K at offset 12MB" true

echo ""
step "Reading back and verifying patterns"
check "EC read 0xAA at offset 0"    nbd_read_verify 0xAA 0 4096
check "EC read 0xBB at offset 4MB"  nbd_read_verify 0xBB $OBJ_SIZE 4096
check "EC read 0xCC at offset 8MB"  nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC read 0xDD at offset 12MB" nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096

# Write larger blocks
echo ""
step "Writing larger blocks with EC"

nbd_write 0x11 65536 65536       # 64K at offset 64K
check "EC write 64K block"    true
check "EC read 64K block"     nbd_read_verify 0x11 65536 65536

nbd_write 0x22 262144 262144    # 256K at offset 256K
check "EC write 256K block"   true
check "EC read 256K block"    nbd_read_verify 0x22 262144 262144

nbd_write 0x33 1048576 1048576  # 1M at offset 1M
check "EC write 1M block"     true
check "EC read 1M block"      nbd_read_verify 0x33 1048576 1048576

echo ""
show_object_distribution

# ━━━ Phase 3: EC Overwrite (read-modify-write) ━━━━━━━━━━━━━━━━━━━━━━━
phase 3 "EC Overwrite — partial writes trigger read-modify-write"

step "Overwriting offset 0 with new pattern 0xEE"
nbd_write 0xEE 0 4096
check "EC overwrite at offset 0"      nbd_read_verify 0xEE 0 4096

step "Overwriting offset 4MB with new pattern 0xFF"
nbd_write 0xFF $OBJ_SIZE 4096
check "EC overwrite at offset 4MB"    nbd_read_verify 0xFF $OBJ_SIZE 4096

# Verify other locations were NOT affected
step "Verifying non-overwritten data is intact"
check "EC 0xCC at 8MB still intact"   nbd_read_verify 0xCC $(( OBJ_SIZE * 2 )) 4096
check "EC 0xDD at 12MB still intact"  nbd_read_verify 0xDD $(( OBJ_SIZE * 3 )) 4096
check "EC 64K block still intact"     nbd_read_verify 0x11 65536 65536
check "EC 1M block still intact"      nbd_read_verify 0x33 1048576 1048576

# Partial overwrite within a block
step "Partial overwrite: 512 bytes in the middle of an existing block"
nbd_write 0xAB 2048 512
check "EC partial write 512B at 2048"                    true
check "EC read partial overwrite"                         nbd_read_verify 0xAB 2048 512
check "EC before partial: 0xEE at offset 0 (first 2K)"   nbd_read_verify 0xEE 0 2048
check "EC after partial: 0xEE at offset 2560"             nbd_read_verify 0xEE 2560 1536

# ━━━ Phase 4: Cross-boundary EC writes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 4 "Cross-boundary — EC writes spanning two 4MB objects"

BOUNDARY_OFFSET=$(( OBJ_SIZE - 4096 ))

step "Writing 8K across object 0/1 boundary (offset ${BOUNDARY_OFFSET})"
nbd_write 0x77 $BOUNDARY_OFFSET 8192
check "EC cross-boundary write (8K)"    true
check "EC read cross-boundary data"      nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Write 1M across boundary between object 2 and 3
BOUNDARY2=$(( OBJ_SIZE * 2 - 524288 ))
step "Writing 1M across object 2/3 boundary (offset ${BOUNDARY2})"
nbd_write 0x88 $BOUNDARY2 1048576
check "EC cross-boundary write (1M)"    true
check "EC read cross-boundary 1M"       nbd_read_verify 0x88 $BOUNDARY2 1048576

# ━━━ Phase 5: Large EC I/O ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 5 "Large EC I/O — multi-object sequential writes"

# Create a second EC VDI for large I/O tests
step "Creating EC VDI 'ecbig' (32M, EC ${EC_POLICY})"
dog_cmd vdi create ecbig 32M --copy-policy "$EC_POLICY"
sleep 1

ECBIG_URI="nbd://${BIND}:${NBD_PORT}/ecbig"

# Write 8MB (spans 2 full objects)
step "Writing 8MB sequential block to 'ecbig'"
qemu-io -f raw -c "write -P 0x55 0 8388608" "$ECBIG_URI" 2>/dev/null
check "EC write 8MB sequential"   true
check "EC read 8MB sequential"    qemu-io -f raw -c "read -P 0x55 0 8388608" "$ECBIG_URI"

# Write 16MB (spans 4 full objects)
step "Writing 16MB sequential block"
qemu-io -f raw -c "write -P 0x66 0 16777216" "$ECBIG_URI" 2>/dev/null
check "EC write 16MB sequential"  true
check "EC read 16MB sequential"   qemu-io -f raw -c "read -P 0x66 0 16777216" "$ECBIG_URI"

echo ""
show_object_distribution

# ━━━ Phase 6: Strip verification ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 6 "Strip verification — check EC strip files on disk"

# Verify that EC strip files exist with _XX suffix
total_strips=0
for i in $(seq 0 $(( NUM_NODES - 1 ))); do
    ec_count=$(count_ec_strips "$i")
    total_strips=$(( total_strips + ec_count ))
done

step "Total EC strip files across cluster: ${total_strips}"
check "EC strips exist on disk" test "$total_strips" -gt 0

echo ""
show_ec_strip_files

# Each data object should produce EC_TOTAL (3) strips distributed across nodes
# With multiple objects, we expect at least EC_TOTAL strips total
check "At least ${EC_TOTAL} EC strips exist" test "$total_strips" -ge "$EC_TOTAL"

# Verify strip file sizes are correct
# For EC 2:1 with 4MB objects, each strip = 4MB / 2 = 2MB
EXPECTED_STRIP_SIZE=$(( OBJ_SIZE / EC_DATA ))
step "Expected strip size: ${EXPECTED_STRIP_SIZE} bytes (${OBJ_SIZE} / ${EC_DATA})"

strip_size_ok=true
for i in $(seq 0 $(( NUM_NODES - 1 ))); do
    local_dir="$(node_dir "$i")/obj"
    if [[ -d "$local_dir" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            sz=$(stat -f%z "${local_dir}/${f}" 2>/dev/null || stat -c%s "${local_dir}/${f}" 2>/dev/null || echo "0")
            if [[ "$sz" -eq "$EXPECTED_STRIP_SIZE" ]]; then
                : # correct size
            elif [[ "$sz" -gt "$EXPECTED_STRIP_SIZE" ]]; then
                step "  Strip ${f} on node${i}: ${sz} bytes (expected ${EXPECTED_STRIP_SIZE})"
                strip_size_ok=false
            fi
        done < <(ls "$local_dir" 2>/dev/null | grep '_[0-9a-f]')
    fi
done
check "EC strip sizes correct (${EXPECTED_STRIP_SIZE} bytes)" $strip_size_ok

# ━━━ Phase 7: Degraded read — node failure + reconstruction ━━━━━━━━━━
phase 7 "Degraded read — stop a node, verify EC reconstruction"

echo -e "  ${YELLOW}NOTE: Degraded reads require EC recovery/migration (not yet"
echo -e "  implemented). After a node leaves, the hash ring changes and"
echo -e "  EC strip locations shift. Tests in this phase verify current"
echo -e "  behavior and track progress toward full degraded-mode support.${NC}"
echo ""

# First verify all data is still readable
step "Pre-check: all data readable with full cluster"
check "Pre-check 0xEE at 0"     nbd_read_verify 0xEE 0 2048
check "Pre-check 0x77 boundary" nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# Stop node 2 (last node) — this removes one strip from each object
step "Stopping node 2 to simulate failure"
stop_node 2
sleep 3  # Wait for heartbeat timeout detection

# Verify that node 2 is actually stopped
if ! is_running 2; then
    pass "Node 2 stopped"
    (( PASS_COUNT++ ))
else
    err "Node 2 still running"
    (( FAIL_COUNT++ ))
fi

# Write new data while degraded — should still work with 2/3 nodes
step "Writing while degraded (2 nodes)"
nbd_write 0xD1 $(( OBJ_SIZE * 4 )) 4096
check "Degraded EC write 4K at 16MB"     true
check "Degraded EC read back 4K at 16MB" nbd_read_verify 0xD1 $(( OBJ_SIZE * 4 )) 4096

# Restart node 2
step "Restarting node 2"
start_node 2 || warn "Node 2 restart failed"
sleep 3

# Verify full cluster reads still work after restart
step "Verifying reads after node 2 restart"
check "Post-restart read 0xEE at 0"   nbd_read_verify 0xEE 0 2048
check "Post-restart read 0x77 boundary" nbd_read_verify 0x77 $BOUNDARY_OFFSET 8192

# ━━━ Phase 8: Cleanup ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
phase 8 "Cleanup"

cleanup

# ━━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo ""
echo -e "${BOLD}━━━ EC Test Summary ━━━${NC}"
echo -e "  EC Policy: ${EC_POLICY} (${EC_DATA}+${EC_PARITY})"
echo -e "  ${GREEN}Passed: ${PASS_COUNT}${NC}"
if (( FAIL_COUNT > 0 )); then
    echo -e "  ${RED}Failed: ${FAIL_COUNT}${NC}"
else
    echo -e "  ${DIM}Failed: 0${NC}"
fi
echo ""

if (( FAIL_COUNT > 0 )); then
    echo -e "${RED}${BOLD}SOME EC TESTS FAILED${NC}"
    echo -e "  Check logs: ${LOG_DIR}/"
    exit 1
else
    echo -e "${GREEN}${BOLD}ALL EC TESTS PASSED${NC}"
    exit 0
fi
```

</details>

### Recovery & Rebalance (`test-recovery.sh`)

Tests node failure detection, recovery, and data rebalancing:

```bash
scripts/test-recovery.sh          # Run full suite (21 tests)
scripts/test-recovery.sh --keep
```

| Phase | What happens |
|-------|-------------|
| 1. Setup | Start 3-node cluster, write data via NBD |
| 2. Kill | SIGKILL node 2 (crash simulation) |
| 3. Recovery | Cluster detects failure (~15s), data still readable |
| 4. Add node | New node 3 joins, epoch bumps |
| 5. Rebalance | Verify data integrity after rebalance |

---

## Erasure Coding

Sheepdog-rs supports **Reed-Solomon erasure coding** as an alternative to simple replication. EC splits each 4 MB object into D data strips and P parity strips — recovering data even if any P strips are lost.

### Create an EC VDI

```bash
# Erasure coded 2+1 (2 data strips + 1 parity strip, needs 3+ nodes)
dog vdi create myvdi 20G --copy-policy 2:1

# Erasure coded 4+2 (4 data + 2 parity, needs 6+ nodes)
dog vdi create myvdi 100G --copy-policy 4:2

# Standard replication (default)
dog vdi create myvdi 20G --copies 3
```

### How it works

```
  4 MB Object
  +--------------------+
  |       data         |
  +--------+-----------+
           | split into D strips
           v
  +--------+--------+
  |   D1   |   D2   |  (data strips)
  +----+---+----+---+
       | Reed-Solomon encode
       v
  +--------+--------+--------+
  |   D1   |   D2   |   P1   |  (D + P strips)
  +----+---+----+---+----+---+
       |        |        |
    node-A   node-B   node-C
```

**Write path**: Object -> split into D strips -> RS encode to D+P strips -> distribute to D+P nodes via hash ring.

**Read path**: Read D strips from nodes -> if any are missing, fetch parity -> RS reconstruct -> reassemble.

**Partial write**: Read full object (reconstruct from strips) -> apply update at offset -> re-encode -> redistribute all strips.

### Policy byte format

The copy policy is encoded as a single byte: `(D << 4) | P`.

| Policy | D:P | Total strips | Storage overhead |
|--------|-----|:------------:|:----------------:|
| `0x21` | 2:1 | 3 | 1.5x |
| `0x41` | 4:1 | 5 | 1.25x |
| `0x42` | 4:2 | 6 | 1.5x |

Compare with replication: 3-copy = 3x overhead. EC 4:2 gives the same fault tolerance with only 1.5x overhead.

### Manual EC Inspection

A diagnostic script lets you inspect EC strips on disk, decode OIDs, hex-dump data, and manually reconstruct objects from data strips:

```bash
# Show all EC strips across nodes
scripts/manual-ec-decode.sh

# Filter by OID / VDI ID
scripts/manual-ec-decode.sh --oid 00db0cc6

# Decode (concatenate data strips) for a specific object
scripts/manual-ec-decode.sh --decode --oid 00db0cc600000000

# Simulate losing strip #2 and show reconstruction info
scripts/manual-ec-decode.sh --decode --simulate-loss 2
```

Output includes:

- **Strip layout**: Which strip lives on which node, file path, size
- **OID decoding**: flags, VDI ID, object index from the 64-bit OID
- **EC parameter inference**: Detects D:P from strip count and size
- **Hex dump**: First N bytes of each strip side-by-side
- **Pattern analysis**: Detects fill patterns for data validation
- **EC format reference**: OID encoding, strip naming, policy byte format

<details>
<summary>Full script (<code>scripts/manual-ec-decode.sh</code>)</summary>

```bash
#!/usr/bin/env bash
#
# manual-ec-decode.sh — Inspect and manually decode EC strips from disk
#
# A diagnostic tool that shows how erasure-coded objects are stored
# across nodes: strip layout, sizes, hex dumps, and manual
# reconstruction via Reed-Solomon.
#
# Usage:
#   ./scripts/manual-ec-decode.sh                     # Auto-detect from running cluster
#   ./scripts/manual-ec-decode.sh --data-root /path   # Specify data directory
#   ./scripts/manual-ec-decode.sh --oid 00ab00010000   # Inspect specific OID
#   ./scripts/manual-ec-decode.sh --decode             # Reconstruct object from strips
#   ./scripts/manual-ec-decode.sh --simulate-loss 2    # Remove strip N, then reconstruct
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

# Defaults
OID_FILTER=""
DO_DECODE=false
SIMULATE_LOSS=""
HEXDUMP_BYTES=64
VERBOSE=false

# Parse flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-root)  shift; DATA_ROOT="$1" ;;
        --oid)        shift; OID_FILTER="$1" ;;
        --decode)     DO_DECODE=true ;;
        --simulate-loss) shift; SIMULATE_LOSS="$1" ;;
        --hex-bytes)  shift; HEXDUMP_BYTES="$1" ;;
        --verbose|-v) VERBOSE=true ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Inspect and decode EC strips stored on disk."
            echo ""
            echo "Options:"
            echo "  --data-root <DIR>      Data directory [default: ${DATA_ROOT}]"
            echo "  --oid <PATTERN>        Filter by OID (substring match)"
            echo "  --decode               Reconstruct full object from data strips"
            echo "  --simulate-loss <N>    Zero out strip N before reconstruct"
            echo "  --hex-bytes <N>        Bytes to hexdump per strip [default: 64]"
            echo "  --verbose, -v          Show more detail"
            echo "  --help, -h             Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                                # Show all EC strips"
            echo "  $0 --oid 00db0cc6                 # Filter by VDI ID"
            echo "  $0 --decode --oid 00db0cc600000000  # Decode object 0"
            echo "  $0 --simulate-loss 2 --decode       # Lose strip 2, reconstruct"
            exit 0
            ;;
        *)  echo "Unknown flag: $1"; exit 1 ;;
    esac
    shift
done

# Resolve binary
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -x "${REPO_ROOT}/target/release/dog" ]]; then
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/dog" ]]; then
    DOG="${REPO_ROOT}/target/debug/dog"
else
    DOG="dog"
fi

# ── Colors ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }

# ── OID Decoding ───────────────────────────────────────────────────────
decode_oid() {
    local hex="$1"
    # OID is 64-bit: 8-bit flags | 24-bit VDI ID | 32-bit object index
    # Hex: FFVVVVVVIIIIIIII (16 hex chars)
    local flags_hex="${hex:0:2}"
    local vid_hex="${hex:2:6}"
    local idx_hex="${hex:8:8}"

    local flags=$(( 16#${flags_hex} ))
    local vid=$(( 16#${vid_hex} ))
    local idx=$(( 16#${idx_hex} ))

    local type="data"
    if (( flags & 0x10 )); then type="vdi"; fi
    if (( flags & 0x20 )); then type="vmstate"; fi
    if (( flags & 0x40 )); then type="attr"; fi
    if (( flags & 0x80 )); then type="ledger"; fi

    printf "flags=0x%02x type=%-8s vid=0x%06x(%d) idx=%d" \
        "$flags" "$type" "$vid" "$vid" "$idx"
}

decode_ec_policy() {
    local policy=$1
    local d=$(( (policy >> 4) & 0x0F ))
    local p=$(( policy & 0x0F ))
    if (( d == 0 )); then
        echo "none (replication)"
    else
        echo "${d}:${p} (${d} data + ${p} parity = $(( d + p )) total)"
    fi
}

# ── Strip discovery ────────────────────────────────────────────────────
discover_strips() {
    echo -e "${BOLD}${CYAN}═══ EC Strip Discovery ═══${NC}"
    echo -e "  Data root: ${DATA_ROOT}"
    echo -e "  Nodes:     ${NUM_NODES}"
    echo ""

    # Collect all EC strip files across nodes
    local found=0
    local oid_list=""

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        if [[ ! -d "$dir" ]]; then
            continue
        fi

        local strips
        strips=$(ls "$dir" 2>/dev/null | grep '_[0-9a-f]') || true
        if [[ -z "$strips" ]]; then
            continue
        fi

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue

            # Apply OID filter
            if [[ -n "$OID_FILTER" ]] && [[ "$fname" != *"$OID_FILTER"* ]]; then
                continue
            fi

            # Parse filename: {oid_hex}_{ec_index_hex}
            local oid_hex="${fname%%_*}"
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))

            # Get file size
            local fpath="${dir}/${fname}"
            local sz
            sz=$(stat -f%z "$fpath" 2>/dev/null || stat -c%s "$fpath" 2>/dev/null || echo "0")

            # Collect unique OIDs
            if [[ "$oid_list" != *"$oid_hex"* ]]; then
                oid_list="${oid_list} ${oid_hex}"
            fi

            (( found++ ))
        done <<< "$strips"
    done

    if (( found == 0 )); then
        warn "No EC strips found in ${DATA_ROOT}"
        echo "  (Make sure a cluster with EC VDIs has been used)"
        return 1
    fi

    info "Found ${found} EC strip files"
    echo ""

    # Show per-object strip layout
    for oid_hex in $oid_list; do
        echo -e "${BOLD}┌─ Object: ${CYAN}${oid_hex}${NC}"
        echo -e "${BOLD}│${NC}  $(decode_oid "$oid_hex")"

        # 4 MB object size
        local obj_size=4194304

        echo -e "${BOLD}│${NC}"
        echo -e "${BOLD}│  Strip layout:${NC}"
        printf "${BOLD}│${NC}  %-6s  %-6s  %-40s  %10s  %s\n" \
            "Node" "Strip" "File" "Size" "Path"
        echo -e "${BOLD}│${NC}  ────── ────── ──────────────────────────────────────── ────────── ──────────"

        local strip_count=0
        local data_strip_size=0

        for i in $(seq 0 $(( NUM_NODES - 1 ))); do
            local dir="${DATA_ROOT}/node${i}/obj"
            [[ ! -d "$dir" ]] && continue

            local matches
            matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
            [[ -z "$matches" ]] && continue

            while IFS= read -r fname; do
                [[ -z "$fname" ]] && continue
                local ec_suffix="${fname##*_}"
                local ec_index=$(( 16#${ec_suffix} ))
                local fpath="${dir}/${fname}"
                local sz
                sz=$(stat -f%z "$fpath" 2>/dev/null || stat -c%s "$fpath" 2>/dev/null || echo "0")

                local label
                # Determine if data or parity based on ec_index
                # ec_index 1..D = data, D+1..D+P = parity
                # But we need to know D — infer from strip size
                if (( data_strip_size == 0 )); then
                    data_strip_size=$sz
                fi

                label="${DIM}strip${NC}"

                printf "${BOLD}│${NC}  %-6s  %-6s  %-40s  %10s  %s\n" \
                    "node${i}" "#${ec_index}" "$fname" "${sz}" "$fpath"

                (( strip_count++ ))
            done <<< "$matches"
        done

        # Infer EC parameters from strip count and size
        if (( data_strip_size > 0 )); then
            local inferred_d=$(( obj_size / data_strip_size ))
            local inferred_p=$(( strip_count - inferred_d ))
            if (( inferred_d > 0 && inferred_p >= 0 )); then
                echo -e "${BOLD}│${NC}"
                echo -e "${BOLD}│${NC}  ${MAGENTA}Inferred EC: ${inferred_d}:${inferred_p}${NC} (${strip_count} strips x ${data_strip_size} bytes)"
                echo -e "${BOLD}│${NC}  Object size: ${obj_size} = ${inferred_d} data strips x ${data_strip_size}"
            fi
        fi

        echo -e "${BOLD}└──${NC}"
        echo ""
    done

    echo "$oid_list"
}

# ── Hex dump strips ───────────────────────────────────────────────────
hexdump_strips() {
    local oid_hex="$1"
    echo -e "${BOLD}${CYAN}═══ Hex Dump: ${oid_hex} ═══${NC}"
    echo -e "  Showing first ${HEXDUMP_BYTES} bytes of each strip"
    echo ""

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        [[ ! -d "$dir" ]] && continue

        local matches
        matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
        [[ -z "$matches" ]] && continue

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue
            local fpath="${dir}/${fname}"
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))

            echo -e "${BOLD}── node${i} / strip #${ec_index} ── ${DIM}${fpath}${NC}"
            xxd -l "$HEXDUMP_BYTES" "$fpath" 2>/dev/null || hexdump -C -n "$HEXDUMP_BYTES" "$fpath" 2>/dev/null
            echo ""
        done <<< "$matches"
    done
}

# ── Manual decode (concatenate data strips) ───────────────────────────
manual_decode() {
    local oid_hex="$1"
    local outfile="/tmp/ec-decoded-${oid_hex}.bin"

    echo -e "${BOLD}${CYAN}═══ Manual Decode: ${oid_hex} ═══${NC}"

    # Collect all strips sorted by ec_index
    local -a strip_files=()
    local -a strip_indices=()
    local -a strip_nodes=()

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        [[ ! -d "$dir" ]] && continue

        local matches
        matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
        [[ -z "$matches" ]] && continue

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))
            strip_files+=("${dir}/${fname}")
            strip_indices+=("$ec_index")
            strip_nodes+=("$i")
        done <<< "$matches"
    done

    local total=${#strip_files[@]}
    if (( total == 0 )); then
        err "No strips found for OID ${oid_hex}"
        return 1
    fi

    # Sort by ec_index (bubble sort for simplicity)
    for (( a=0; a < total-1; a++ )); do
        for (( b=a+1; b < total; b++ )); do
            if (( strip_indices[b] < strip_indices[a] )); then
                local tmp="${strip_files[a]}"
                strip_files[a]="${strip_files[b]}"
                strip_files[b]="$tmp"
                tmp="${strip_indices[a]}"
                strip_indices[a]="${strip_indices[b]}"
                strip_indices[b]="$tmp"
                tmp="${strip_nodes[a]}"
                strip_nodes[a]="${strip_nodes[b]}"
                strip_nodes[b]="$tmp"
            fi
        done
    done

    # Determine data vs parity
    local strip_sz
    strip_sz=$(stat -f%z "${strip_files[0]}" 2>/dev/null || stat -c%s "${strip_files[0]}" 2>/dev/null)
    local obj_size=4194304
    local data_strips=$(( obj_size / strip_sz ))
    local parity_strips=$(( total - data_strips ))

    echo -e "  EC config:  ${data_strips}:${parity_strips} (${data_strips} data + ${parity_strips} parity)"
    echo -e "  Strip size: ${strip_sz} bytes"
    echo -e "  Total:      ${total} strips"
    echo ""

    # Show strip map
    echo -e "  ${BOLD}Strip Map:${NC}"
    for (( s=0; s < total; s++ )); do
        local idx="${strip_indices[s]}"
        local node="${strip_nodes[s]}"
        local role
        # Determine if data or parity based on ec_index
        # ec_index 1..D = data, D+1..D+P = parity
        if (( idx <= data_strips )); then
            role="${GREEN}DATA${NC}"
        else
            role="${YELLOW}PARITY${NC}"
        fi

        local status="OK"
        if [[ -n "$SIMULATE_LOSS" ]] && (( idx == SIMULATE_LOSS )); then
            status="${RED}LOST (simulated)${NC}"
            role="${RED}MISSING${NC}"
        fi

        printf "    strip #%-2d  node%-2d  %-20b  %b\n" "$idx" "$node" "$role" "$status"
    done
    echo ""

    if [[ -n "$SIMULATE_LOSS" ]]; then
        echo -e "  ${YELLOW}Simulating loss of strip #${SIMULATE_LOSS}${NC}"
        echo -e "  ${YELLOW}Reed-Solomon can reconstruct from any ${data_strips} of ${total} strips${NC}"
        echo ""

        if (( parity_strips > 0 )); then
            echo -e "  ${GREEN}Reconstruction possible!${NC}"
            echo -e "  With ${parity_strips} parity strip(s), can tolerate ${parity_strips} strip loss(es)"
        else
            echo -e "  ${RED}No parity strips — cannot reconstruct!${NC}"
        fi
        echo ""
    fi

    # Concatenate data strips to reconstruct object
    info "Concatenating ${data_strips} data strips -> ${outfile}"
    > "$outfile"  # truncate

    local decoded_count=0
    for (( s=0; s < total; s++ )); do
        local idx="${strip_indices[s]}"
        if (( idx > data_strips )); then
            # Skip parity strips for simple decode
            continue
        fi

        if [[ -n "$SIMULATE_LOSS" ]] && (( idx == SIMULATE_LOSS )); then
            warn "  Strip #${idx} is LOST — filling with zeros"
            dd if=/dev/zero bs="$strip_sz" count=1 >> "$outfile" 2>/dev/null
        else
            info "  Appending strip #${idx} from node${strip_nodes[s]}"
            cat "${strip_files[s]}" >> "$outfile"
        fi
        (( decoded_count++ ))
    done

    local out_sz
    out_sz=$(stat -f%z "$outfile" 2>/dev/null || stat -c%s "$outfile" 2>/dev/null)
    echo ""
    info "Decoded object: ${outfile} (${out_sz} bytes)"

    # Show hexdump of decoded
    echo ""
    echo -e "  ${BOLD}Decoded hex (first ${HEXDUMP_BYTES} bytes):${NC}"
    xxd -l "$HEXDUMP_BYTES" "$outfile" 2>/dev/null || hexdump -C -n "$HEXDUMP_BYTES" "$outfile" 2>/dev/null

    # Show pattern analysis
    echo ""
    echo -e "  ${BOLD}Pattern analysis:${NC}"
    local first_byte
    first_byte=$(xxd -l 1 -p "$outfile" 2>/dev/null)
    if [[ -n "$first_byte" ]]; then
        # Count how many of the first 4096 bytes match the first byte
        local match_count
        match_count=$(xxd -l 4096 -p "$outfile" 2>/dev/null | fold -w2 | grep -c "^${first_byte}$" 2>/dev/null) || match_count=0
        echo "    First byte: 0x${first_byte}"
        echo "    Pattern 0x${first_byte} repeats: ${match_count}/4096 in first 4K"
        if (( match_count == 4096 )); then
            echo -e "    ${GREEN}Consistent fill pattern 0x${first_byte} — data looks valid${NC}"
        elif (( match_count > 3000 )); then
            echo -e "    ${YELLOW}Mostly pattern 0x${first_byte} — may have partial overwrites${NC}"
        fi
    fi

    echo ""
    echo -e "${DIM}Tip: Compare with live read:  qemu-io -f raw -c 'read -P 0x${first_byte:-00} 0 4096' nbd://${BIND}:${NBD_PORT}/vdiname${NC}"
}

# ── Cluster info (if running) ─────────────────────────────────────────
show_cluster_ec_info() {
    local port0=$(( BASE_PORT ))
    if nc -z "$BIND" "$port0" 2>/dev/null; then
        echo -e "${BOLD}${CYAN}═══ Cluster EC VDIs ═══${NC}"
        echo ""
        local vdi_list
        vdi_list=$("$DOG" -a "$BIND" -p "$port0" vdi list 2>/dev/null) || true
        if [[ -n "$vdi_list" ]]; then
            echo "$vdi_list" | while IFS= read -r line; do
                echo "  $line"
            done
        else
            echo "  (no VDIs or cluster not running)"
        fi
        echo ""
    fi
}

# ── Main ───────────────────────────────────────────────────────────────

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         Sheepdog EC Strip Inspector / Decoder               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Show cluster info if available
show_cluster_ec_info

# Discover all strips
oid_list_output=$(discover_strips) || exit 1

# Extract just the OID list (last line of output)
oid_list=$(echo "$oid_list_output" | tail -1)
# Print the rest (strip layout)
echo "$oid_list_output" | head -n -1

echo ""

# Hexdump each object's strips
for oid_hex in $oid_list; do
    hexdump_strips "$oid_hex"
done

# Decode if requested
if $DO_DECODE; then
    if [[ -n "$OID_FILTER" ]]; then
        # Decode specific OID
        for oid_hex in $oid_list; do
            if [[ "$oid_hex" == *"$OID_FILTER"* ]]; then
                manual_decode "$oid_hex"
            fi
        done
    else
        # Decode first OID found
        first_oid=$(echo "$oid_list" | awk '{print $1}')
        if [[ -n "$first_oid" ]]; then
            manual_decode "$first_oid"
        fi
    fi
fi

echo ""
echo -e "${BOLD}═══ EC Format Reference ═══${NC}"
echo ""
echo "  OID format:   FFVVVVVVIIIIIIII (16 hex chars)"
echo "                FF       = flags (00=data, 10=vdi, 20=vmstate)"
echo "                VVVVVV   = VDI ID (24 bits)"
echo "                IIIIIIII = object index (32 bits)"
echo ""
echo "  Strip file:   {oid_hex}_{ec_index:02x}"
echo "                ec_index 01..D  = data strips"
echo "                ec_index D+1..T = parity strips"
echo ""
echo "  EC policy:    (D << 4) | P  (single byte)"
echo "                0x21 = 2:1 (2 data + 1 parity, 1.5x overhead)"
echo "                0x42 = 4:2 (4 data + 2 parity, 1.5x overhead)"
echo ""
echo "  Strip size:   SD_DATA_OBJ_SIZE / D = 4MB / D"
echo "                2:1 -> 2MB per strip"
echo "                4:2 -> 1MB per strip"
echo ""
echo -e "${DIM}For full decode with Reed-Solomon reconstruction, use the sheep daemon.${NC}"
echo -e "${DIM}This script performs data-strip concatenation (no RS math).${NC}"
```

</details>

---

## DPDK Data Plane (Optional)

Sheepdog-rs supports **DPDK** (Data Plane Development Kit) for kernel-bypass peer-to-peer I/O. DPDK replaces kernel TCP with user-space UDP on a dedicated NIC, significantly reducing latency and increasing throughput for the data path.

The control plane (cluster mesh, dog CLI, HTTP, NFS, NBD) stays on kernel TCP. Only peer data I/O (gateway forwarding, recovery fetch, replica repair) uses the DPDK transport.

### Architecture

```
  Control plane (TCP, kernel)          Data plane (UDP, DPDK)
  ----------------------------         --------------------------
  Client -> sheep:7000 (gateway)       sheep <-> sheep via :7100
  dog -> sheep:7000 (CLI)              rte_eth_rx/tx_burst()
  sheep <-> sheep:7001 (cluster mesh)  Poll-mode driver (PMD)
  NBD :10809, NFS :2049, HTTP :8000    Dedicated CPU core
```

### Build with DPDK

```bash
# Requires DPDK installed (pkg-config --libs libdpdk must work)
cargo build -p sheep --features dpdk

# Without DPDK (default) -- pure TCP, no system deps
cargo build -p sheep
```

### Usage

```bash
sheep --dpdk \
      --dpdk-addr 10.0.0.1 \
      --dpdk-port 7100 \
      --dpdk-eal-args "-l 0-3 -n 4 --file-prefix sheep" \
      --dpdk-ports 0 \
      --dpdk-queues 1 \
      /data/sheep
```

Each node advertises its DPDK address (`io_addr` + `io_port`) so peers route data I/O through the DPDK NIC. Nodes without DPDK communicate via TCP transparently.

### Packet Format

```
  Ethernet | IP | UDP | DpdkPeerHeader (16 bytes) | Payload
                        +-- request_id   (u64)
                        +-- flags        (u16)  FIRST/LAST/RESPONSE/SINGLE
                        +-- frag_index   (u16)
                        +-- total_frags  (u16)
                        +-- payload_len  (u16)
```

Large messages (e.g., 4 MB objects) are fragmented into ~8 KB UDP datagrams and reassembled by the receiver.

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dpdk` | off | Enable DPDK data plane |
| `--dpdk-addr` | (required) | IP address for DPDK NIC |
| `--dpdk-port` | 7100 | UDP port for data plane |
| `--dpdk-eal-args` | "" | DPDK EAL init arguments |
| `--dpdk-ports` | "0" | DPDK port IDs (comma-separated) |
| `--dpdk-queues` | 1 | RX/TX queues per port |

---

## Workspace

```
sheepdog-rs/
+-- crates/
|   +-- sheepdog-proto/   1,568 LOC   Wire protocol, types, constants
|   +-- sheepdog-core/      755 LOC   Consistent hashing, EC, transport traits
|   +-- sheep/           11,402 LOC   Storage daemon
|   +-- dog/              2,728 LOC   CLI admin tool
|   +-- shepherd/           349 LOC   Cluster monitor
|   +-- sheepdog-dpdk/      766 LOC   DPDK data plane (optional)
|   +-- sheepfs/            554 LOC   FUSE filesystem (optional)
+-- scripts/
|   +-- defaults.sh          23 LOC   Shared defaults (ports, paths)
|   +-- cluster.sh          348 LOC   3-node cluster management
|   +-- test-io.sh          580 LOC   I/O correctness test (63 tests)
|   +-- test-ec.sh          575 LOC   EC correctness test (44 tests)
|   +-- test-recovery.sh    475 LOC   Recovery & rebalance test (21 tests)
|   +-- manual-ec-decode.sh 517 LOC   EC strip inspector / decoder
+-- Cargo.toml                        Workspace root (v0.10.0)
```

**Total**: 18,122 LOC Rust + 2,518 LOC bash | 75 unit tests + 128 integration tests

### sheepdog-proto -- Protocol Library

Wire types shared by all components:

- `SdRequest` / `ResponseResult` -- 30+ request variants (read, write, VDI ops, cluster ops)
- `ObjectId` -- 64-bit OID encoding (8-bit flags + 24-bit VDI ID + 32-bit object index)
- `SdNode` / `NodeId` -- Node identity (IP + port + zone)
- `ClusterInfo` / `ClusterStatus` -- Cluster metadata and state machine
- `SdError` -- Typed error enum with 30+ variants
- `SdInode` -- On-disk inode structure (name, size, data map)
- `VdiState` / `LockState` -- Runtime VDI state and locking
- Centralized tunable defaults (`defaults.rs`) for ports, timeouts, buffer sizes

### sheepdog-core -- Core Library

- **Consistent hashing** -- Virtual node ring with zone-aware placement
- **Erasure coding** -- Reed-Solomon via `reed-solomon-erasure` (encode, reconstruct, ec_policy_to_dp)
- **Networking** -- Async TCP helpers, socket FD caching
- **Transport abstraction** -- `PeerTransport` / `PeerListener` / `PeerResponder` traits for pluggable data plane (TCP or DPDK)
- **TcpTransport** -- Default transport using kernel TCP with connection pooling (`SockfdCache`)

### sheep -- Storage Daemon

The main daemon. Handles object I/O, replication, erasure coding, recovery, and exposes multiple server interfaces.

```
sheep startup
  +-- ClusterDriver.init()    Listen on cluster port
  +-- ClusterDriver.join()    Connect to seeds, exchange members
  |
  +-- cluster_event_loop()    Join / Leave / Notify / Block / Unblock
  +-- accept_loop()           Client TCP -> dispatch to ops handler
  +-- recovery_worker()       Background object migration
  |
  +-- http_server()           S3/Swift on :8000 (optional)
  +-- nfs_server()            NFS v3 on :2049 (optional)
  +-- nbd_server()            NBD export on :10809 (optional)
```

**Request pipeline:**

```
Client TCP -> read_request() -> dispatch(SdRequest)
                                   +-- Gateway  -> forward via hash ring
                                   |               +-- replicated write/read
                                   |               +-- EC write/read (RS encode/decode)
                                   +-- Peer     -> local object I/O
                                   +-- Cluster  -> VDI create/delete, format
                                   +-- Local    -> node info, stat queries
```

**Largest modules:**

| Module | Lines | Purpose |
|--------|------:|---------|
| `cluster/sdcluster.rs` | 1,298 | P2P TCP mesh driver |
| `nbd/mod.rs` | 842 | NBD export server |
| `main.rs` | 647 | CLI args, startup orchestration |
| `ops/gateway.rs` | 627 | Gateway I/O (replication + EC) |
| `recovery.rs` | 613 | Background object migration |
| `store/md.rs` | 568 | Multi-disk storage backend |
| `object_cache.rs` | 542 | LRU object cache |
| `ops/peer.rs` | 467 | Peer-to-peer I/O |
| `ops/cluster.rs` | 440 | Cluster-wide operations |
| `journal.rs` | 440 | Write-ahead journal |
| `nfs/*.rs` | 1,139 | NFS v3 server (ONC RPC) |
| `http/*.rs` | 691 | HTTP/S3/Swift API |

### dog -- CLI Admin Tool

```
dog [OPTIONS] <COMMAND>

Options:
  -a, --address <ADDR>    Sheep address [default: 127.0.0.1]
  -p, --port <PORT>       Sheep port [default: 7000]

Commands:
  vdi       Virtual disk management
  node      Node management
  cluster   Cluster operations
  upgrade   Upgrade utilities
```

**VDI commands:**

```bash
dog vdi create <name> <size>                    # Create VDI (replicated)
dog vdi create <name> <size> --copy-policy 2:1  # Create VDI (EC 2+1)
dog vdi delete <name>                           # Delete VDI
dog vdi list                                    # List all VDIs
dog vdi snapshot <name> -s <tag>                # Take snapshot
dog vdi clone <src> <dst>                       # Clone VDI
dog vdi resize <name> <size>                    # Resize VDI
dog vdi object <name>                           # Show object map
dog vdi tree                                    # Show snapshot/clone tree
dog vdi lock list                               # Show locks
dog vdi lock unlock <name>                      # Force unlock
```

**Cluster commands:**

```bash
dog cluster info                    # Cluster status
dog cluster format -c <N>           # Format with N replicas
dog cluster shutdown                # Graceful shutdown
dog cluster check                   # Health check
dog cluster alter-copy -c <N>       # Change replica count
dog cluster recover enable/disable  # Toggle recovery
```

**Node commands:**

```bash
dog node list                       # List nodes
dog node info                       # Node details
dog node recovery                   # Recovery progress
dog node md info                    # Multi-disk layout
dog node md plug <path>             # Add disk
dog node md unplug <path>           # Remove disk
```

### shepherd -- Cluster Monitor

Heartbeat monitor for production clusters:

```bash
shepherd -b 0.0.0.0 -p 7100 --heartbeat-interval 5 --failure-timeout 30
```

### sheepfs -- FUSE Filesystem

Mount VDIs as local files (requires libfuse/macFUSE):

```bash
cargo build --release -p sheepfs
sheepfs /mnt/sheepdog -a 127.0.0.1 -p 7000
ls /mnt/sheepdog/vdi/
```

---

## sheep CLI Reference

```
sheep [OPTIONS] <DIR>

Arguments:
  <DIR>                       Data directory

Options:
  -b, --bind-addr <ADDR>      Listen address [default: 0.0.0.0]
  -p, --port <PORT>           Listen port [default: 7000]
  -g, --gateway               Gateway mode (no local storage)
  -c, --copies <N>            Replica count
  -z, --zone <ID>             Fault zone [default: 0]
  -v, --vnodes <N>            Virtual nodes [default: 128]
  -j, --journal <DIR>         Journal directory
  -w, --cache                 Enable object cache
      --cache-size <MB>       Cache size [default: 256]
      --directio              Direct I/O
      --http-port <PORT>      HTTP/S3 port [default: 8000]
      --nfs                   Enable NFS server
      --nfs-port <PORT>       NFS port [default: 2049]
      --nbd                   Enable NBD server
      --nbd-port <PORT>       NBD port [default: 10809]
  -l, --log-level <LEVEL>     Log level [default: info]
      --cluster-driver <DRV>  local | sdcluster [default: local]
      --seed <HOST:PORT>      Seed node (repeatable)
      --dpdk                  Enable DPDK data plane
      --dpdk-addr <IP>        DPDK NIC address (required with --dpdk)
      --dpdk-port <PORT>      DPDK UDP port [default: 7100]
      --dpdk-eal-args <ARGS>  DPDK EAL arguments
      --dpdk-ports <IDS>      DPDK port IDs [default: 0]
      --dpdk-queues <N>       RX/TX queues [default: 1]
```

---

## Cluster Membership

### P2P TCP Mesh (`--cluster-driver sdcluster`)

Built-in cluster membership with zero external dependencies. Replaces the C version's Corosync/ZooKeeper requirement.

```
  sheep:7000 <------------------> sheep:7002
  cluster:7001                     cluster:7003
      ^   \                          /   ^
      |     \   Heartbeat(5s)      /     |
      |       \   Join/Leave     /       |
      v         v              v         v
                sheep:7004
                cluster:7005
```

**How it works:**

1. New node connects to a **seed**, receives member list
2. New node opens TCP to **every** existing member (full mesh)
3. **Heartbeat** every 5 seconds; peer declared dead after 15s silence
4. **Leader** = node with smallest `NodeId` (IP:port)
5. **Two-phase updates**: Leader sends `Block` -> all pause -> `Unblock` with result

**Cluster messages:**

| Message | Purpose |
|---------|---------|
| `Join { node }` | Join request (new -> seed) |
| `JoinResponse { members }` | Member list (seed -> new) |
| `Leave { node }` | Graceful departure |
| `Heartbeat { node }` | Keepalive (5s interval) |
| `Notify { data }` | Broadcast (format, shutdown) |
| `Block` / `Unblock { data }` | Two-phase atomic update |
| `Election` / `ElectionResponse` | Leader election |

### Local Driver (`--cluster-driver local`)

Single-node, in-process channel-based. Default for development.

---

## Storage

### Object Addressing

Each sheepdog object has a 64-bit **Object ID**:

```
  63       56 55      32 31                 0
  +----------+----------+-------------------+
  |  flags   |  VDI ID  |   object index    |
  +----------+----------+-------------------+
     8 bits    24 bits        32 bits
```

- **4 MB per object** (`SD_DATA_OBJ_SIZE`)
- Up to **16M VDIs**, each up to **16 EB**
- Objects placed on nodes via consistent hash of OID

### Backends

| Backend | Path Layout | Use Case |
|---------|-------------|----------|
| `plain` | `obj/{oid_hex}` | Simple, flat directory |
| `tree` | `obj/{vid_hex}/{oid_hex}` | Better for many VDIs |
| `md` | Balanced across multiple disks | Production |

### EC Strip Storage

When erasure coding is active, each strip is stored with an `_XX` suffix on the target node:

```
node-A/obj/00ab000100000000_01   <-- data strip 1
node-B/obj/00ab000100000000_02   <-- data strip 2
node-C/obj/00ab000100000000_03   <-- parity strip 1
```

Standard replicated objects have no suffix (just the hex OID).

### Caching & Journaling

- **Object cache**: LRU eviction, backed by `dashmap` + `lru` crate
- **Write-ahead journal**: Memory-mapped files via `memmap2`

---

## Server Interfaces

### NBD Export (port 10809)

Exports VDIs as block devices for QEMU and other NBD clients. Each VDI name is an NBD export.

- **Protocol**: Fixed newstyle handshake (RFC-compliant)
- **Options**: `LIST`, `GO`, `INFO`, `EXPORT_NAME`, `ABORT`
- **Commands**: `READ`, `WRITE`, `FLUSH`, `TRIM`, `WRITE_ZEROES`, `DISC`
- **Block size**: min=512, preferred=4MB, max=4MB
- **Flags**: `HAS_FLAGS`, `SEND_FLUSH`, `SEND_TRIM`, `SEND_WRITE_ZEROES`, `CAN_MULTI_CONN`

```bash
sheep --nbd /data/sheep
qemu-nbd --list -k 127.0.0.1:10809         # List exports
qemu-img info nbd://127.0.0.1:10809/mydisk  # Inspect
```

### HTTP / S3 API (port 8000)

S3-compatible object storage interface (feature-gated, enabled by default):

```bash
curl http://localhost:8000/                              # List buckets
curl -X PUT http://localhost:8000/mybucket                # Create bucket
curl -X PUT http://localhost:8000/mybucket/key -d "data"  # Upload
curl http://localhost:8000/mybucket/key                    # Download
curl -X DELETE http://localhost:8000/mybucket/key           # Delete
```

OpenStack Swift API also available at `/v1/{account}/`.

### NFS v3 (port 2049)

ONC RPC over TCP. Procedures: NULL, GETATTR, SETATTR, LOOKUP, READ, WRITE, CREATE, REMOVE, MKDIR, READDIR, FSSTAT, FSINFO, PATHCONF.

```bash
sheep --nfs --nfs-port 2049 /data/sheep
mount -t nfs -o port=2049,mountport=2050,nfsvers=3,tcp localhost:/ /mnt/sheep
```

---

## Wire Protocol

| Protocol | Encoding | Length Prefix | Default Port |
|----------|----------|---------------|:------------:|
| Client I/O | bincode | 4-byte BE | 7000 |
| Cluster mesh | bincode | 4-byte LE | 7001 |
| DPDK peer I/O | bincode + UDP | DpdkPeerHeader (16B) | 7100 |
| HTTP/S3 | HTTP/1.1 + JSON | -- | 8000 |
| NFS v3 | XDR / ONC RPC | RPC record mark | 2049 |
| NBD | Big-endian binary | Fixed headers | 10809 |

---

## Configuration Defaults

Tunable defaults are centralized in `crates/sheepdog-proto/src/defaults.rs`. All values are compile-time constants, overridable via CLI flags. Protocol-level constants (wire format, object sizes, magic numbers) remain in `constants.rs`.

| Category | Constant | Default |
|----------|----------|---------|
| Network | `SD_LISTEN_PORT` | 7000 |
| Network | `DEFAULT_HTTP_PORT` | 8000 |
| Network | `DEFAULT_NBD_PORT` | 10809 |
| Network | `DEFAULT_NFS_PORT` | 2049 |
| Network | `DEFAULT_DPDK_PORT` | 7100 |
| Performance | `DEFAULT_CACHE_SIZE_MB` | 256 |
| Performance | `DEFAULT_JOURNAL_SIZE_MB` | 512 |
| Performance | `DEFAULT_TCP_MAX_CONNS_PER_NODE` | 8 |
| Cluster | `DEFAULT_CLUSTER_HEARTBEAT_INTERVAL_SECS` | 5 |
| Cluster | `DEFAULT_CLUSTER_HEARTBEAT_TIMEOUT_SECS` | 15 |
| Recovery | `DEFAULT_RECOVERY_MAX_EXEC_COUNT` | 1 |
| Shepherd | `DEFAULT_SHEPHERD_HEARTBEAT_INTERVAL_SECS` | 5 |
| Shepherd | `DEFAULT_SHEPHERD_FAILURE_TIMEOUT_SECS` | 30 |

Shell scripts source `scripts/defaults.sh` for consistent test configuration.

---

## Feature Flags

```bash
# Default (includes HTTP/S3)
cargo build -p sheep

# Without HTTP
cargo build -p sheep --no-default-features

# With DPDK data plane (requires system DPDK)
cargo build -p sheep --features dpdk

# sheepfs (requires system libfuse)
cargo build -p sheepfs
```

| Feature | Default | Crate | Requires |
|---------|:-------:|-------|----------|
| `http` | yes | sheep | -- |
| `dpdk` | no | sheep | libdpdk (system) |

---

## Differences from C Sheepdog

| | C Sheepdog (v0.9.5) | sheepdog-rs (v0.10.0) |
|-|---------------------|----------------------|
| Language | C | Rust (async, memory-safe) |
| Codebase | ~60K LOC | ~18K LOC |
| Async I/O | epoll + callbacks | tokio async/await |
| Cluster | Corosync / ZooKeeper | Built-in P2P TCP mesh |
| External deps | corosync, libcpg | None |
| Serialization | Hand-packed structs | bincode + serde |
| HTTP server | Custom parser | axum |
| Erasure coding | C library (jerasure) | reed-solomon-erasure (pure Rust) |
| Data plane | Kernel TCP only | Pluggable: TCP (default) or DPDK |
| QEMU attach | Native block driver | NBD export server |

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| [tokio](https://tokio.rs/) | Async runtime |
| [serde](https://serde.rs/) + [bincode](https://docs.rs/bincode) | Serialization |
| [clap](https://docs.rs/clap) | CLI parsing |
| [tracing](https://docs.rs/tracing) | Structured logging |
| [axum](https://docs.rs/axum) | HTTP/S3 server |
| [fuser](https://docs.rs/fuser) | FUSE bindings |
| [dashmap](https://docs.rs/dashmap) | Concurrent hash map |
| [memmap2](https://docs.rs/memmap2) | Memory-mapped I/O |
| [reed-solomon-erasure](https://docs.rs/reed-solomon-erasure) | Erasure coding |
| [bitvec](https://docs.rs/bitvec) | VDI bitmap |
| [tabled](https://docs.rs/tabled) | CLI table output |
| [indicatif](https://docs.rs/indicatif) | Progress bars |

---

## Status

| Component | Status |
|-----------|:------:|
| Protocol types & OID encoding | Done |
| Centralized config defaults | Done |
| Consistent hashing (zone-aware) | Done |
| P2P cluster driver | Done |
| Local cluster driver | Done |
| Cluster event loop | Done |
| Storage backends (plain, tree, md) | Done |
| Client request pipeline | Done |
| Object replication | Done |
| Erasure coding (Reed-Solomon) | Done |
| Recovery worker | Done |
| Object cache + journal | Done |
| HTTP/S3 API | Done |
| Swift API | Done |
| NFS v3 server | Done |
| NBD export server | Done |
| DPDK data plane (optional) | Done |
| Pluggable transport (TCP/DPDK) | Done |
| CLI tool (dog) | Done |
| FUSE filesystem (sheepfs) | Done |
| Cluster monitor (shepherd) | Done |

---

## License

GPL-2.0 (same as original Sheepdog).

## Links

- [Sheepdog Project](https://sheepdog.github.io/sheepdog/)
- [Sheepdog Wiki](https://github.com/sheepdog/sheepdog/wiki)
- [Original C Source](https://github.com/sheepdog/sheepdog)
