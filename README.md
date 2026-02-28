# sheepdog-rs

A **Rust** rewrite of [Sheepdog](https://sheepdog.github.io/sheepdog/) — distributed block storage for QEMU/KVM virtual machines.

> 17,500+ lines of async Rust across 7 crates. No external cluster dependencies.
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

Options: `--nbd`, `--nfs`, `--format`, `--copies N`, `--http`.

### 6. Recovery test

A test script validates node failure, recovery, and rebalance:

```bash
# Run full recovery test (automatic cleanup)
scripts/test-recovery.sh

# Keep data for inspection
scripts/test-recovery.sh --keep
```

**Test flow:**

| Phase | What happens |
|-------|-------------|
| 1. Setup | Start 3-node cluster, write data via NBD |
| 2. Kill | SIGKILL node 2 (crash simulation) |
| 3. Recovery | Cluster detects failure (~15s), data still readable (2-copy) |
| 4. Add node | New node 3 joins, epoch bumps |
| 5. Rebalance | Verify data integrity after rebalance |

---

## Erasure Coding

Sheepdog-rs supports **Reed-Solomon erasure coding** as an alternative to simple replication. EC splits each 4 MB object into D data strips and P parity strips — recovering data even if any P strips are lost.

### Create an EC VDI

```bash
# Erasure coded 2+1 (2 data strips + 1 parity strip, needs ≥3 nodes)
dog vdi create myvdi 20G --copy-policy 2:1

# Erasure coded 4+2 (4 data + 2 parity, needs ≥6 nodes)
dog vdi create myvdi 100G --copy-policy 4:2

# Standard replication (default)
dog vdi create myvdi 20G --copies 3
```

### How it works

```
  4 MB Object
  ┌──────────────────┐
  │     data         │
  └──────┬───────────┘
         │ split into D strips
         ▼
  ┌──────┬──────┐
  │ D₁   │ D₂   │  (data strips)
  └──┬───┴──┬───┘
     │ Reed-Solomon encode
     ▼
  ┌──────┬──────┬──────┐
  │ D₁   │ D₂   │ P₁   │  (D + P strips)
  └──┬───┴──┬───┴──┬───┘
     │      │      │
  node-A  node-B  node-C
```

**Write path**: Object → split into D strips → RS encode to D+P strips → distribute to D+P nodes via hash ring.

**Read path**: Read D strips from nodes → if any are missing, fetch parity → RS reconstruct → reassemble.

**Partial write**: Read full object (reconstruct from strips) → apply update at offset → re-encode → redistribute all strips.

### Policy byte format

The copy policy is encoded as a single byte: `(D << 4) | P`.

| Policy | D:P | Total strips | Storage overhead |
|--------|-----|:------------:|:----------------:|
| `0x21` | 2:1 | 3 | 1.5× |
| `0x41` | 4:1 | 5 | 1.25× |
| `0x42` | 4:2 | 6 | 1.5× |
| `0x12` | 1:2 | 3 | 3× |

Compare with replication: 3-copy replication = 3× overhead. EC 4:2 gives the same fault tolerance with only 1.5× overhead.

---

## DPDK Data Plane (Optional)

Sheepdog-rs supports **DPDK** (Data Plane Development Kit) for kernel-bypass peer-to-peer I/O. DPDK replaces kernel TCP with user-space UDP on a dedicated NIC, significantly reducing latency and increasing throughput for the data path.

The control plane (cluster mesh, dog CLI, HTTP, NFS, NBD) stays on kernel TCP. Only peer data I/O (gateway forwarding, recovery fetch, replica repair) uses the DPDK transport.

### Architecture

```
  Control plane (TCP, kernel)          Data plane (UDP, DPDK)
  ────────────────────────────         ──────────────────────────
  Client → sheep:7000 (gateway)        sheep ←→ sheep via :7100
  dog → sheep:7000 (CLI)               rte_eth_rx/tx_burst()
  sheep ←→ sheep:7001 (cluster mesh)   Poll-mode driver (PMD)
  NBD :10809, NFS :2049, HTTP :8000    Dedicated CPU core
```

### Build with DPDK

```bash
# Requires DPDK installed (pkg-config --libs libdpdk must work)
cargo build -p sheep --features dpdk

# Without DPDK (default) — pure TCP, no system deps
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
                        ├── request_id   (u64)
                        ├── flags        (u16)  FIRST/LAST/RESPONSE/SINGLE
                        ├── frag_index   (u16)
                        ├── total_frags  (u16)
                        └── payload_len  (u16)
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
├── crates/
│   ├── sheepdog-proto/   1,462 LOC   Wire protocol, types, constants
│   ├── sheepdog-core/      650 LOC   Consistent hashing, EC, transport traits
│   ├── sheep/           11,500 LOC   Storage daemon
│   ├── dog/              2,728 LOC   CLI admin tool
│   ├── shepherd/           344 LOC   Cluster monitor
│   ├── sheepdog-dpdk/      560 LOC   DPDK data plane (optional)
│   └── sheepfs/            554 LOC   FUSE filesystem (optional)
├── scripts/
│   ├── cluster.sh          353 LOC   3-node cluster management
│   └── test-recovery.sh    310 LOC   Recovery & rebalance test
└── Cargo.toml                        Workspace root (v0.10.0)
```

### sheepdog-proto — Protocol Library

Wire types shared by all components:

- `SdRequest` / `ResponseResult` — 30+ request variants (read, write, VDI ops, cluster ops)
- `ObjectId` — 64-bit OID encoding (8-bit flags + 24-bit VDI ID + 32-bit object index)
- `SdNode` / `NodeId` — Node identity (IP + port + zone)
- `ClusterInfo` / `ClusterStatus` — Cluster metadata and state machine
- `SdError` — Typed error enum with 30+ variants
- `SdInode` — On-disk inode structure (name, size, data map)
- `VdiState` / `LockState` — Runtime VDI state and locking

### sheepdog-core — Core Library

- **Consistent hashing** — Virtual node ring with zone-aware placement
- **Erasure coding** — Reed-Solomon via `reed-solomon-erasure` (encode, reconstruct, ec_policy_to_dp)
- **Networking** — Async TCP helpers, socket FD caching
- **Transport abstraction** — `PeerTransport` / `PeerListener` / `PeerResponder` traits for pluggable data plane (TCP or DPDK)
- **TcpTransport** — Default transport using kernel TCP with connection pooling (`SockfdCache`)

### sheep — Storage Daemon

The main daemon. Handles object I/O, replication, erasure coding, recovery, and exposes multiple server interfaces.

```
sheep startup
  ├── ClusterDriver.init()    Listen on cluster port
  ├── ClusterDriver.join()    Connect to seeds, exchange members
  │
  ├── cluster_event_loop()    Join / Leave / Notify / Block / Unblock
  ├── accept_loop()           Client TCP → dispatch to ops handler
  ├── recovery_worker()       Background object migration
  │
  ├── http_server()           S3/Swift on :8000 (optional)
  ├── nfs_server()            NFS v3 on :2049 (optional)
  └── nbd_server()            NBD export on :10809 (optional)
```

**Request pipeline:**

```
Client TCP → read_request() → dispatch(SdRequest)
                                 ├── Gateway  → forward via hash ring
                                 │              ├── replicated write/read
                                 │              └── EC write/read (RS encode/decode)
                                 ├── Peer     → local object I/O
                                 ├── Cluster  → VDI create/delete, format
                                 └── Local    → node info, stat queries
```

**Largest modules:**

| Module | Lines | Purpose |
|--------|------:|---------|
| `cluster/sdcluster.rs` | 1,293 | P2P TCP mesh driver |
| `nbd/mod.rs` | 844 | NBD export server |
| `ops/gateway.rs` | 667 | Gateway I/O (replication + EC) |
| `recovery.rs` | 662 | Background object migration |
| `store/md.rs` | 568 | Multi-disk storage backend |
| `object_cache.rs` | 542 | LRU object cache |
| `ops/peer.rs` | 450 | Peer-to-peer I/O |
| `ops/cluster.rs` | 440 | Cluster-wide operations |
| `journal.rs` | 440 | Write-ahead journal |
| `nfs/*.rs` | 1,139 | NFS v3 server (ONC RPC) |
| `http/*.rs` | 691 | HTTP/S3/Swift API |

### dog — CLI Admin Tool

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
dog vdi create <name> <size>                  # Create VDI (replicated)
dog vdi create <name> <size> --copy-policy 2:1  # Create VDI (EC 2+1)
dog vdi delete <name>                         # Delete VDI
dog vdi list                                  # List all VDIs
dog vdi snapshot <name> -s <tag>              # Take snapshot
dog vdi clone <src> <dst>                     # Clone VDI
dog vdi resize <name> <size>                  # Resize VDI
dog vdi object <name>                         # Show object map
dog vdi tree                                  # Show snapshot/clone tree
dog vdi lock list                             # Show locks
dog vdi lock unlock <name>                    # Force unlock
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

### shepherd — Cluster Monitor

Heartbeat monitor for production clusters:

```bash
shepherd -b 0.0.0.0 -p 7100 --heartbeat-interval 5 --failure-timeout 30
```

### sheepfs — FUSE Filesystem

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
  sheep:7000 ◄─────────────► sheep:7002
  cluster:7001                cluster:7003
      ▲  ╲                      ╱  ▲
      │    ╲   Heartbeat(5s)  ╱    │
      │      ╲  Join/Leave  ╱      │
      ▼        ▼           ▼       ▼
              sheep:7004
              cluster:7005
```

**How it works:**

1. New node connects to a **seed**, receives member list
2. New node opens TCP to **every** existing member (full mesh)
3. **Heartbeat** every 5 seconds; peer declared dead after 15s silence
4. **Leader** = node with smallest `NodeId` (IP:port)
5. **Two-phase updates**: Leader sends `Block` → all pause → `Unblock` with result

**Cluster messages:**

| Message | Purpose |
|---------|---------|
| `Join { node }` | Join request (new → seed) |
| `JoinResponse { members }` | Member list (seed → new) |
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

When erasure coding is active, each strip is stored with an `_N` suffix on the target node:

```
node-A/obj/00ab000100000000_1   ← data strip 1
node-B/obj/00ab000100000000_2   ← data strip 2
node-C/obj/00ab000100000000_3   ← parity strip 1
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
curl http://localhost:8000/                         # List buckets
curl -X PUT http://localhost:8000/mybucket           # Create bucket
curl -X PUT http://localhost:8000/mybucket/key -d "data"  # Upload
curl http://localhost:8000/mybucket/key               # Download
curl -X DELETE http://localhost:8000/mybucket/key      # Delete
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
| HTTP/S3 | HTTP/1.1 + JSON | — | 8000 |
| NFS v3 | XDR / ONC RPC | RPC record mark | 2049 |
| NBD | Big-endian binary | Fixed headers | 10809 |

---

## Key Constants

| Constant | Value |
|----------|-------|
| `SD_DATA_OBJ_SIZE` | 4 MB |
| `SD_LISTEN_PORT` | 7000 |
| `NBD_DEFAULT_PORT` | 10809 |
| `SD_DEFAULT_COPIES` | 3 |
| `SD_MAX_NODES` | 6,144 |
| `SD_NR_VDIS` | 16,777,216 |
| `SD_DEFAULT_VNODES` | 128 |
| `SD_EC_MAX_STRIP` | 16 |
| `HEARTBEAT_INTERVAL` | 5 s |
| `HEARTBEAT_TIMEOUT` | 15 s |

---

## Feature Flags

```bash
# Default (includes HTTP/S3)
cargo build -p sheep

# Without HTTP
cargo build -p sheep --no-default-features

# With NFS
cargo build -p sheep --features nfs

# With DPDK data plane (requires system DPDK)
cargo build -p sheep --features dpdk

# sheepfs (requires system libfuse)
cargo build -p sheepfs
```

| Feature | Default | Crate | Requires |
|---------|:-------:|-------|----------|
| `http` | yes | sheep | — |
| `nfs` | no | sheep | — |
| `dpdk` | no | sheep | libdpdk (system) |

---

## Differences from C Sheepdog

| | C Sheepdog (v0.9.5) | sheepdog-rs (v0.10.0) |
|-|---------------------|----------------------|
| Language | C | Rust (async, memory-safe) |
| Codebase | ~60K LOC | ~16.8K LOC |
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
