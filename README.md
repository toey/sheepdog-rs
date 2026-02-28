# sheepdog-rs

Rust implementation of [Sheepdog](https://sheepdog.github.io/sheepdog/) — a distributed block storage system for QEMU/KVM.

Sheepdog provides highly available block-level storage volumes that can be attached to QEMU/KVM virtual machines. It uses consistent hashing to distribute data across cluster nodes without any centralized metadata server.

> **15,500+ lines of Rust** across 6 crates — fully async (tokio), memory-safe, zero external cluster dependencies.

## Architecture

```
                       +-----------+
                       |  QEMU/KVM |
                       +-----+-----+
                             |  nbd://host:10809/vdi-name
              +--------------+--------------+
              |              |              |
         +----+----+   +----+----+   +-----+----+
         |  sheep  |   |  sheep  |   |  sheep   |
         | node 0  |   | node 1  |   | node 2   |
         +----+----+   +----+----+   +-----+----+
              |              |              |
              +--------------+--------------+
              P2P TCP Mesh + Consistent Hash Ring
```

**Sheepdog** distributes virtual disk images (VDIs) as 4 MB data objects across a cluster of **sheep** daemons. Objects are replicated (or erasure-coded) to multiple nodes for fault tolerance. There is no single point of failure — every sheep node can serve any client request by forwarding it to the correct peer through the hash ring.

## Workspace

```
sheepdog-rs/
├── Cargo.toml                      # workspace root (v0.10.0, edition 2021)
├── crates/
│   ├── sheepdog-proto/  (1,451 LOC)  # wire protocol, types, constants
│   ├── sheepdog-core/   (  414 LOC)  # consistent hash, erasure coding, networking
│   ├── sheep/           (10,067 LOC) # storage daemon
│   ├── dog/             (2,679 LOC)  # CLI admin tool
│   ├── shepherd/        (  344 LOC)  # cluster coordinator
│   └── sheepfs/         (  554 LOC)  # FUSE filesystem
```

| Crate | Binary | Description | LOC |
|-------|--------|-------------|----:|
| `sheepdog-proto` | *(library)* | Wire protocol, object IDs, error types, constants | 1,451 |
| `sheepdog-core` | *(library)* | Consistent hashing, erasure coding, networking | 414 |
| `sheep` | `sheep` | Storage daemon — object I/O, replication, recovery, HTTP/S3, NFS | 10,067 |
| `dog` | `dog` | CLI admin tool — VDI / node / cluster management | 2,679 |
| `shepherd` | `shepherd` | Cluster coordinator — heartbeat monitoring | 344 |
| `sheepfs` | `sheepfs` | FUSE filesystem — mount VDIs as local files | 554 |

## Building

```bash
# Build all default crates (excludes sheepfs which needs libfuse)
cargo build --release

# Build with sheepfs (requires libfuse/macFUSE)
cargo build --release -p sheepfs
```

**Requirements**: Rust 1.70+ (edition 2021). Linux, macOS, or FreeBSD. libfuse / macFUSE only for `sheepfs`.

**Binaries** are placed in `target/release/`:

```
sheep      ~4.7 MB   storage daemon
dog        ~3.1 MB   CLI tool
shepherd   ~2.4 MB   cluster coordinator
```

## Quick Start

### Single-node (development)

```bash
# Start sheep with default local driver
sheep /tmp/sheep/store

# Format the cluster and create a VDI
dog cluster format -c 1
dog vdi create my-disk 10G
dog vdi list
dog node list
```

### Multi-node cluster (production)

```bash
# Node 0 — first node (no seeds needed)
sheep --cluster-driver sdcluster -b 10.0.0.1 -p 7000 /data/sheep

# Node 1 — joins via seed
sheep --cluster-driver sdcluster -b 10.0.0.2 -p 7000 \
      --seed 10.0.0.1:7000 /data/sheep

# Node 2 — multiple seeds for redundancy
sheep --cluster-driver sdcluster -b 10.0.0.3 -p 7000 \
      --seed 10.0.0.1:7000 --seed 10.0.0.2:7000 /data/sheep

# Format with 3-way replication
dog -a 10.0.0.1 cluster format -c 3
dog -a 10.0.0.1 node list
```

### Localhost multi-node (testing)

```bash
# 3 nodes on localhost with different ports
sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7000 \
      --http-port 8000 /tmp/sheep/node0

sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7002 \
      --seed 127.0.0.1:7000 --http-port 8002 /tmp/sheep/node1

sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7004 \
      --seed 127.0.0.1:7000 --http-port 8004 /tmp/sheep/node2

# Verify
dog node list
# +----+-----------+------+--------+------+-------+--------+
# | Id | Host      | Port | VNodes | Zone | Space | Status |
# +----+-----------+------+--------+------+-------+--------+
# | 0  | 127.0.0.1 | 7000 | 128    | 0    | 0 B   | alive  |
# | 1  | 127.0.0.1 | 7002 | 128    | 0    | 0 B   | alive  |
# | 2  | 127.0.0.1 | 7004 | 128    | 0    | 0 B   | alive  |
# +----+-----------+------+--------+------+-------+--------+
```

### Use with QEMU (via NBD)

QEMU removed the native sheepdog block driver in v6.0+. Use the built-in **NBD export server** instead:

```bash
# Start sheep with NBD enabled
sheep --nbd /data/sheep

# Create a VDI
dog cluster format --copies 1
dog vdi create my-disk 20G

# Use with QEMU via NBD
qemu-system-x86_64 \
  -drive file=nbd://127.0.0.1:10809/my-disk,format=raw,if=virtio \
  -m 1024 ...

# Or create/inspect with qemu-img
qemu-img info nbd://127.0.0.1:10809/my-disk
qemu-img create -f raw nbd://127.0.0.1:10809/my-disk 20G
```

## Components

### sheep — Storage Daemon

The main daemon that stores data objects and serves client requests.

```
sheep [OPTIONS] <DIR>

Arguments:
  <DIR>                          Data directory for object storage

Options:
  -b, --bind-addr <ADDR>         Listen address [default: 0.0.0.0]
  -p, --port <PORT>              Listen port [default: 7000]
  -g, --gateway                  Gateway mode (no local storage)
  -c, --copies <N>               Number of replicas
  -z, --zone <ID>                Fault zone ID [default: 0]
  -v, --vnodes <N>               Virtual nodes per physical node [default: 128]
  -j, --journal <DIR>            Journal directory
  -w, --cache                    Enable object cache
      --cache-size <MB>          Object cache size [default: 256]
      --directio                 Enable direct I/O
      --http-port <PORT>         HTTP/S3 API port [default: 8000]
      --nfs                      Enable NFS server
      --nfs-port <PORT>          NFS port [default: 2049]
      --nbd                      Enable NBD export server
      --nbd-port <PORT>          NBD port [default: 10809]
  -l, --log-level <LEVEL>        Log level [default: info]
      --cluster-driver <NAME>    Cluster driver: local or sdcluster [default: local]
      --seed <HOST:PORT>         Seed node address (repeatable, sdcluster only)
      --cluster-port-offset <N>  Cluster port = listen port + offset [default: 1]
```

#### Internal Architecture

```
sheep startup
  │
  ├── Create ClusterDriver (local or sdcluster)
  ├── driver.init()       → listen on cluster port, start heartbeat/reaper
  ├── driver.join()       → connect to seeds, exchange member list
  │
  ├── cluster_event_loop()
  │     Join(node)        → group::handle_node_join()    → bump epoch
  │     Leave(node)       → group::handle_node_leave()   → bump epoch
  │     Notify(data)      → handle_cluster_notify()      → format/shutdown/alter-copy
  │     Block             → pause for two-phase update
  │     Unblock(data)     → resume + apply
  │
  ├── accept_loop()       → client request pipeline
  │     read_request()    → dispatch to ops/{gateway,peer,cluster,local}
  │
  ├── http_server()       → S3/Swift API on :8000 (optional, axum)
  ├── nfs_server()        → NFS v3 on :2049 (optional, ONC RPC)
  ├── nbd_server()        → NBD export on :10809 (optional, for QEMU)
  │
  shutdown:
    ├── driver.leave()    → announce departure to all peers
    └── save_config()     → persist cluster state to disk
```

#### Request Pipeline

```
Client TCP → accept_loop() → handle_client()
                                  │
                           read_request() [u32 len + bincode]
                                  │
                           dispatch(SdRequest)
                              ├── OpType::Gateway  → forward to correct node via hash ring
                              ├── OpType::Peer     → local object I/O for peer requests
                              ├── OpType::Cluster  → cluster-wide ops (format, VDI create)
                              └── OpType::Local    → node-local queries (info, stat)
                                  │
                           send_response() [bincode]
```

#### Storage Backends

| Backend | Layout | Use case |
|---------|--------|----------|
| `plain` | `/obj/{oid_hex}` | Simple flat directory |
| `tree` | `/obj/{vid_hex}/{oid_hex}` | Hierarchical, better for large VDI counts |
| `md` | Multi-disk with balancing | Production with multiple drives |

#### Feature Modules (sheep)

| Module | LOC | Purpose |
|--------|----:|---------|
| `cluster/sdcluster.rs` | 1,293 | P2P TCP mesh driver |
| `recovery.rs` | 662 | Background object migration on topology change |
| `store/md.rs` | 568 | Multi-disk storage backend |
| `object_cache.rs` | 542 | LRU object cache (dashmap) |
| `ops/peer.rs` | 443 | Peer-to-peer I/O operations |
| `journal.rs` | 440 | Write-ahead logging (memmap2) |
| `ops/cluster.rs` | 427 | Cluster-wide operations (format, VDI create/delete) |
| `nfs/mod.rs` + `handler.rs` | 748 | NFS v3 server (ONC RPC over TCP) |
| `nbd/mod.rs` | 844 | NBD export server (for QEMU/qemu-img) |
| `http/*.rs` | 691 | HTTP/S3/Swift API (axum) |

### dog — CLI Admin Tool

```
dog [OPTIONS] <COMMAND>

Commands:
  vdi       VDI (Virtual Disk Image) management
  node      Cluster node management
  cluster   Cluster-wide operations
  upgrade   Cluster upgrade utilities

Options:
  -a, --address <ADDR>     Sheep daemon address [default: 127.0.0.1]
  -p, --port <PORT>        Sheep daemon port [default: 7000]
  -v, --verbose            Verbose output
```

**VDI commands:**

| Command | Description |
|---------|-------------|
| `dog vdi create <name> <size>` | Create a new VDI |
| `dog vdi delete <name>` | Delete a VDI |
| `dog vdi list` | List all VDIs |
| `dog vdi snapshot <name> -s <tag>` | Create a snapshot |
| `dog vdi clone <src> <dst>` | Clone a VDI or snapshot |
| `dog vdi resize <name> <size>` | Resize a VDI |
| `dog vdi object <name>` | Show object layout map |
| `dog vdi tree` | Show snapshot/clone tree |
| `dog vdi setattr <name> <key> <val>` | Set VDI attribute |
| `dog vdi getattr <name> <key>` | Get VDI attribute |
| `dog vdi lock list` | List VDI locks |
| `dog vdi lock unlock <name>` | Force-unlock a VDI |

**Node commands:**

| Command | Description |
|---------|-------------|
| `dog node list` | List cluster nodes with status |
| `dog node info` | Detailed node information |
| `dog node recovery` | Show recovery progress |
| `dog node md info` | Multi-disk layout |
| `dog node md plug <path>` | Add a disk online |
| `dog node md unplug <path>` | Remove a disk online |

**Cluster commands:**

| Command | Description |
|---------|-------------|
| `dog cluster info` | Cluster status and config |
| `dog cluster format -c <N>` | Format with N replicas |
| `dog cluster shutdown` | Graceful cluster-wide shutdown |
| `dog cluster check` | Health and consistency check |
| `dog cluster alter-copy -c <N>` | Change default replica count |
| `dog cluster recover enable/disable` | Toggle auto-recovery |

### sheepfs — FUSE Filesystem

Mount sheepdog VDIs as local files:

```bash
sheepfs /mnt/sheepdog -a 127.0.0.1 -p 7000
ls /mnt/sheepdog/vdi/
cat /mnt/sheepdog/vdi/my-disk > disk.img
```

Options: `-f` foreground, `--cache-timeout <sec>`. Requires libfuse (Linux) or macFUSE (macOS).

### shepherd — Cluster Coordinator

Optional heartbeat monitor for production:

```bash
shepherd -b 0.0.0.0 -p 7100 --heartbeat-interval 5 --failure-timeout 30
```

## Cluster Membership

### P2P TCP Mesh (`sdcluster` driver)

Unlike the original C implementation which relied on external cluster engines (Corosync/ZooKeeper), the Rust port includes a **built-in P2P TCP mesh** with zero external dependencies.

```
       sheep:7000 ◄────────────► sheep:7002
       cluster:7001              cluster:7003
           ▲ ╲                    ╱ ▲
           │   ╲  Heartbeat     ╱   │
           │    ╲  Join/Leave  ╱    │
           │     ╲ Notify     ╱     │
           │      ╲          ╱      │
           ▼       ▼        ▼       ▼
              sheep:7004
              cluster:7005
```

**Topology**: Full mesh — every node maintains a TCP connection to every other node.

**How it works:**

1. **Seed discovery**: New node connects to a seed, receives the full member list
2. **Mesh expansion**: New node connects to every discovered member
3. **Heartbeat**: Every 5 seconds, each node pings all peers
4. **Failure detection**: If no heartbeat for 15 seconds, peer is declared dead
5. **Leader election**: Node with the smallest `NodeId` (IP:port) is leader
6. **Two-phase updates**: `Block` → pause all nodes → `Unblock` with result

**Cluster messages:**

| Message | Direction | Purpose |
|---------|-----------|---------|
| `Join { node }` | new → seed | Join request |
| `JoinResponse { members }` | seed → new | Current member list |
| `Leave { node }` | node → all | Graceful departure |
| `Heartbeat { node }` | node ↔ node | Keepalive (5s) |
| `Notify { data }` | leader → all | Broadcast (format, shutdown, alter-copy) |
| `Block` | leader → all | Two-phase update phase 1 |
| `Unblock { data }` | leader → all | Two-phase update phase 2 |
| `Election { candidate }` | node → all | Leader election |
| `ElectionResponse { leader }` | node → node | Election result |

**Wire format**: 4-byte little-endian length prefix + bincode-encoded `ClusterMessage`.

### Local driver

Single-node, in-process channel-based driver for development and testing. Selected by default (`--cluster-driver local`).

### Driver comparison

| | `local` | `sdcluster` |
|-|---------|-------------|
| Use case | Development, testing | Production clusters |
| Nodes | 1 | Unlimited |
| Transport | In-process mpsc | TCP sockets |
| Failure detection | N/A | Heartbeat (5s/15s) |
| Leader election | Self | Deterministic (min NodeId) |
| External deps | None | None |

## Wire Protocol

All sheepdog components communicate over TCP using a binary protocol:

```
+------------------+-----------------------------------+
| u32 length       | bincode(RequestHeader, SdRequest) |
+------------------+-----------------------------------+
```

| Protocol | Length prefix | Serialization | Port |
|----------|-------------|---------------|------|
| Client I/O | 4-byte **big-endian** | bincode | 7000 |
| Cluster mesh | 4-byte **little-endian** | bincode | 7001 |
| HTTP/S3 | HTTP/1.1 | JSON/binary | 8000 |
| NFS v3 | ONC RPC record mark | XDR | 2049 |
| NBD | Fixed newstyle + 28-byte request header | big-endian binary | 10809 |

### Object Addressing

Each data object is identified by a 64-bit **Object ID (OID)**:

```
  63       56 55      32 31                 0
  +----------+----------+-------------------+
  |  flags   |  VDI ID  |   object index    |
  +----------+----------+-------------------+
```

- **VDI ID**: 24-bit virtual disk identifier (up to 16M VDIs)
- **Object index**: 32-bit index within the VDI
- **Object size**: 4 MB (`SD_DATA_OBJ_SIZE`)
- **Max VDI size**: 16 EB (4 MB × 2³² objects)

## HTTP/S3 API

The sheep daemon exposes an S3-compatible HTTP API on port 8000 (default).

```bash
# List buckets
curl http://localhost:8000/

# Create a bucket
curl -X PUT http://localhost:8000/my-bucket

# Upload an object
curl -X PUT http://localhost:8000/my-bucket/my-key -d "hello"

# Download an object
curl http://localhost:8000/my-bucket/my-key

# Delete an object
curl -X DELETE http://localhost:8000/my-bucket/my-key
```

**OpenStack Swift API** is also available under `/v1/{account}/`:

```bash
curl http://localhost:8000/v1/AUTH_test/container/object
```

## NFS v3

Export VDIs as NFS files (ONC RPC over TCP):

```bash
sheep --nfs --nfs-port 2049 /data/sheep
mount -t nfs -o port=2049,mountport=2050,nfsvers=3,tcp localhost:/ /mnt/sheep
```

Implements: NULL, GETATTR, SETATTR, LOOKUP, READ, WRITE, CREATE, REMOVE, MKDIR, READDIR, FSSTAT, FSINFO, PATHCONF.

## NBD Export Server

The sheep daemon includes an NBD (Network Block Device) server that exports VDIs as block devices. This allows QEMU and other NBD clients to connect directly — no native sheepdog block driver required.

```bash
# Start sheep with NBD
sheep --nbd /data/sheep

# List available exports
qemu-nbd --list -k 127.0.0.1:10809

# Use with QEMU
qemu-system-x86_64 -drive file=nbd://127.0.0.1:10809/my-disk,format=raw,if=virtio ...

# Read/write with qemu-io
qemu-io -f raw -c "write -P 0xAB 0 4096" nbd://127.0.0.1:10809/my-disk
qemu-io -f raw -c "read -P 0xAB 0 4096" nbd://127.0.0.1:10809/my-disk
```

**Protocol**: Fixed newstyle negotiation (RFC-compliant). Supports `NBD_OPT_LIST`, `NBD_OPT_GO`, `NBD_OPT_INFO`, `NBD_OPT_EXPORT_NAME`.

**Commands**: `READ`, `WRITE`, `FLUSH`, `TRIM`, `WRITE_ZEROES`, `DISC`.

**Export names**: Each VDI name is an export name. The default port is 10809 (IANA-reserved).

| Wire format | Value |
|-------------|-------|
| Default port | 10809 |
| Handshake | Fixed newstyle + NO_ZEROES |
| Transmission flags | HAS_FLAGS, SEND_FLUSH, SEND_FUA, SEND_TRIM, SEND_WRITE_ZEROES, CAN_MULTI_CONN |
| Block size | min=512, preferred=4MB (= SD_DATA_OBJ_SIZE), max=4MB |

## Key Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `SD_DATA_OBJ_SIZE` | 4 MB | Size of each data object |
| `SD_LISTEN_PORT` | 7000 | Default daemon port |
| `NBD_DEFAULT_PORT` | 10809 | Default NBD export port |
| `SD_DEFAULT_COPIES` | 3 | Default replica count |
| `SD_MAX_NODES` | 6144 | Maximum cluster nodes |
| `SD_NR_VDIS` | 16M | Maximum VDI count |
| `SD_DEFAULT_VNODES` | 128 | Virtual nodes per physical node |
| `HEARTBEAT_INTERVAL` | 5s | Cluster heartbeat interval |
| `HEARTBEAT_TIMEOUT` | 15s | Peer failure detection timeout |
| `MAX_MESSAGE_SIZE` | 8 MB | Maximum cluster wire message size |

## Feature Flags

The `sheep` crate supports optional features:

| Feature | Default | Description |
|---------|---------|-------------|
| `http` | yes | HTTP/S3 and Swift API (axum) |
| `nfs` | no | NFS v3 server |

```bash
# Build without HTTP
cargo build -p sheep --no-default-features

# Build with NFS
cargo build -p sheep --features nfs
```

## Comparison with C Sheepdog

| Feature | C Sheepdog (v0.9.5) | sheepdog-rs (v0.10.0) |
|---------|---------------------|----------------------|
| Language | C | Rust (async, memory-safe) |
| Cluster membership | Corosync / ZooKeeper | Built-in P2P TCP mesh |
| External dependencies | corosync, libcpg | None |
| Async I/O | epoll + callbacks | tokio async/await |
| Serialization | Custom binary structs | bincode + serde |
| HTTP API | Custom HTTP parser | axum |
| Leader election | Corosync CPG | Deterministic (min NodeId) |
| Atomic updates | Corosync two-phase | Block/Unblock messages |
| Codebase | ~60K LOC | ~15.5K LOC |

## Dependencies

| Dependency | Purpose |
|------------|---------|
| [tokio](https://tokio.rs/) | Async runtime (full features) |
| [serde](https://serde.rs/) + [bincode](https://docs.rs/bincode) | Serialization |
| [clap](https://docs.rs/clap) | CLI argument parsing |
| [tracing](https://docs.rs/tracing) | Structured logging |
| [axum](https://docs.rs/axum) | HTTP/S3 server (optional) |
| [fuser](https://docs.rs/fuser) | FUSE bindings (sheepfs only) |
| [dashmap](https://docs.rs/dashmap) | Concurrent hash map (object cache) |
| [memmap2](https://docs.rs/memmap2) | Memory-mapped I/O (journal) |
| [reed-solomon-erasure](https://docs.rs/reed-solomon-erasure) | Erasure coding |
| [bitvec](https://docs.rs/bitvec) | VDI usage bitmap |
| [tabled](https://docs.rs/tabled) | Table formatting (dog CLI) |
| [indicatif](https://docs.rs/indicatif) | Progress bars (dog CLI) |

## Project Status

Rust port of [C Sheepdog v0.9.5](https://github.com/sheepdog/sheepdog).

| Component | Status | Notes |
|-----------|--------|-------|
| Protocol types & constants | ✅ Complete | All request/response types, OID encoding |
| Consistent hashing | ✅ Complete | Virtual node ring with zone awareness |
| P2P cluster driver (sdcluster) | ✅ Complete | TCP mesh, heartbeat, leader election, two-phase |
| Local cluster driver | ✅ Complete | Single-node with in-process channels |
| Cluster event loop | ✅ Complete | Join/Leave/Notify/Block/Unblock dispatch |
| Storage backends | ✅ Complete | plain, tree, md drivers |
| Client request pipeline | ✅ Complete | Accept, dispatch, gateway forwarding |
| Object replication | ✅ Complete | Synchronous multi-copy writes via hash ring |
| Recovery worker | ✅ Complete | Background object migration on topology change |
| Object cache | ✅ Complete | LRU with dashmap |
| Write-ahead journal | ✅ Complete | Memory-mapped WAL (memmap2) |
| HTTP/S3 API | ✅ Complete | axum-based S3-compatible interface |
| OpenStack Swift API | ✅ Complete | Swift-compatible container/object interface |
| NFS v3 server | ✅ Complete | ONC RPC framing + NFS3 procedures |
| CLI tool (dog) | ✅ Complete | All subcommands: vdi, node, cluster, upgrade |
| FUSE filesystem (sheepfs) | ✅ Complete | Mount VDIs as local files |
| Cluster coordinator (shepherd) | ✅ Complete | Heartbeat monitoring + health status |
| NBD export server | ✅ Complete | Fixed newstyle negotiation, READ/WRITE/FLUSH/TRIM |
| Erasure coding | 🔶 Partial | reed-solomon-erasure integrated, wiring incomplete |
| QEMU integration | ✅ Via NBD | `nbd://host:10809/vdi-name` (native driver removed from QEMU v6.0+) |

## License

GPL-2.0 — same as the original Sheepdog project.

## References

- [Sheepdog Project](https://sheepdog.github.io/sheepdog/)
- [Sheepdog Wiki](https://github.com/sheepdog/sheepdog/wiki)
- [QEMU Sheepdog Documentation](https://www.qemu.org/docs/master/system/devices/sheepdog.html)
