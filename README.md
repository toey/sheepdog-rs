# sheepdog-rs

Rust implementation of [Sheepdog](https://sheepdog.github.io/sheepdog/) — a distributed block storage system for QEMU/KVM.

Sheepdog provides highly available block-level storage volumes that can be attached to QEMU/KVM virtual machines. It uses consistent hashing to distribute data across cluster nodes without any centralized metadata server.

## Architecture

```
                       +-----------+
                       |  QEMU/KVM |
                       +-----+-----+
                             |
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

### Cluster Membership (P2P TCP Mesh)

Unlike the original C implementation which relied on external cluster engines (Corosync/ZooKeeper), the Rust port includes a **built-in P2P TCP mesh** (`sdcluster` driver) with zero external dependencies:

```
       sheep:7000 ◄──────► sheep:7000
       cluster:7001        cluster:7001
           ▲                   ▲
           │    Heartbeat      │
           │    Join/Leave     │
           │    Notify         │
           ▼    Block/Unblock  ▼
       sheep:7000 ◄──────► sheep:7000
       cluster:7001        cluster:7001
```

- **Full mesh topology**: every node maintains TCP connections to every other node
- **Heartbeat-based failure detection**: 5s interval, 15s timeout
- **Deterministic leader election**: node with smallest `NodeId` wins
- **Two-phase atomic updates**: Block/Unblock for cluster-wide state changes
- **Seed-based discovery**: new nodes connect to seeds, receive full member list

Two cluster drivers are available:

| Driver | Use case |
|--------|----------|
| `local` | Single-node development/testing (default) |
| `sdcluster` | Multi-node production clusters (P2P TCP mesh) |

## Workspace Crates

| Crate | Binary | Description |
|-------|--------|-------------|
| `sheepdog-proto` | *(library)* | Wire protocol, object IDs, error types, constants |
| `sheepdog-core` | *(library)* | Consistent hashing, async networking, erasure coding |
| `sheep` | `sheep` | Storage daemon — object I/O, replication, recovery |
| `dog` | `dog` | CLI admin tool — VDI/node/cluster management |
| `sheepfs` | `sheepfs` | FUSE filesystem to mount VDIs as local files |
| `shepherd` | `shepherd` | Cluster coordinator — heartbeat monitoring |

## Building

```bash
# Build all default crates (excludes sheepfs which needs libfuse)
cargo build --release

# Build with sheepfs (requires libfuse/macFUSE)
cargo build --release -p sheepfs
```

### Requirements

- Rust 1.70+ (edition 2021)
- Linux, macOS, or FreeBSD
- libfuse / macFUSE (only for `sheepfs`)

## Quick Start

### Single-node (development)

```bash
# Start sheep with default local driver
sheep /tmp/sheep/0

# Format and create a VDI
dog cluster format -c 1
dog vdi create my-disk 10G
dog vdi list
```

### Multi-node cluster

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
dog -a 10.0.0.1 cluster info

# Create a VDI
dog -a 10.0.0.1 vdi create my-disk 100G
```

### Local multi-node (testing)

```bash
# 3 nodes on localhost with different ports
sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7000 /tmp/sheep/0
sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7002 --seed 127.0.0.1:7000 /tmp/sheep/1
sheep --cluster-driver sdcluster -b 127.0.0.1 -p 7004 --seed 127.0.0.1:7000 /tmp/sheep/2

dog cluster format -c 3
```

### Use with QEMU

```bash
qemu-system-x86_64 \
  -drive file=sheepdog:my-disk,if=virtio \
  -m 1024 ...
```

## Components

### sheep — Storage Daemon

The main daemon that stores data objects and serves client requests.

```
sheep [OPTIONS] <DIR>

Arguments:
  <DIR>                        Data directory for object storage

Options:
  -b, --bind-addr <ADDR>       Listen address [default: 0.0.0.0]
  -p, --port <PORT>            Listen port [default: 7000]
  -g, --gateway                Gateway mode (no local storage)
  -c, --copies <N>             Number of replicas
  -z, --zone <ID>              Fault zone ID [default: 0]
  -v, --vnodes <N>             Virtual nodes per physical node [default: 128]
  -j, --journal <DIR>          Journal directory
  -w, --cache                  Enable object cache
      --cache-size <MB>        Object cache size [default: 256]
      --directio               Enable direct I/O
      --http-port <PORT>       HTTP/S3 API port [default: 8000]
      --nfs                    Enable NFS server
      --nfs-port <PORT>        NFS port [default: 2049]
  -l, --log-level <LEVEL>      Log level [default: info]
      --cluster-driver <NAME>  Cluster driver: local or sdcluster [default: local]
      --seed <HOST:PORT>       Seed node address (repeatable, sdcluster only)
      --cluster-port-offset <N> Cluster port = listen port + offset [default: 1]
```

**Features:**

- **Cluster drivers**: `local` (single-node) or `sdcluster` (P2P TCP mesh)
- **Object store backends**: `plain` (flat), `tree` (hierarchical by VDI), `md` (multi-disk)
- **Replication**: synchronous writes to N replicas via consistent hash ring
- **Recovery**: automatic background object migration when nodes join/leave
- **Gateway mode**: forward-only node with no local storage (for load balancing)
- **HTTP/S3 API**: Amazon S3-compatible object interface (via axum)
- **OpenStack Swift API**: Swift-compatible container/object interface
- **NFS v3**: export VDIs as NFS files (ONC RPC over TCP)
- **Object cache**: LRU cache for frequently accessed objects
- **Journal**: write-ahead logging with memory-mapped files

#### Cluster Event Flow

```
sheep startup
  |
  +-- Create ClusterDriver (local or sdcluster)
  +-- driver.init()     Listen on cluster port, start heartbeat/reaper
  +-- driver.join()     Connect to seeds, exchange member list
  |
  +-- cluster_event_loop() --+
  |     Join(node)           +-->  group::handle_node_join()   bump epoch
  |     Leave(node)          +-->  group::handle_node_leave()  bump epoch
  |     Notify(data)         +-->  handle_cluster_notify()     format/shutdown/etc
  |     Block                +-->  pause for two-phase update
  |     Unblock(data)        +-->  resume + apply
  |
  +-- accept_loop()     Client request handling
  +-- http_server()     S3/Swift API (optional)
  +-- nfs_server()      NFS v3 (optional)
  |
  shutdown:
    +-- driver.leave()       Announce departure to all peers
    +-- save_config()        Persist cluster state
```

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
| `dog vdi object <name>` | Show object map |
| `dog vdi tree` | Show snapshot/clone tree |
| `dog vdi setattr <name> <key> <val>` | Set VDI attribute |
| `dog vdi getattr <name> <key>` | Get VDI attribute |
| `dog vdi lock list` | List VDI locks |
| `dog vdi lock unlock <name>` | Force-unlock a VDI |

**Node commands:**

| Command | Description |
|---------|-------------|
| `dog node list` | List cluster nodes |
| `dog node info` | Show detailed node info |
| `dog node recovery` | Show recovery status |
| `dog node md info` | Show multi-disk info |
| `dog node md plug <path>` | Add a disk |
| `dog node md unplug <path>` | Remove a disk |

**Cluster commands:**

| Command | Description |
|---------|-------------|
| `dog cluster info` | Show cluster status |
| `dog cluster format -c <N>` | Format cluster with N copies |
| `dog cluster shutdown` | Shutdown entire cluster |
| `dog cluster check` | Check cluster health |
| `dog cluster alter-copy -c <N>` | Change default copies |
| `dog cluster recover enable/disable` | Control auto-recovery |

### sheepfs — FUSE Filesystem

Mount sheepdog VDIs as local files:

```bash
sheepfs /mnt/sheepdog -a 127.0.0.1 -p 7000
ls /mnt/sheepdog/vdi/      # list VDI files
cat /mnt/sheepdog/vdi/my-disk > disk.img   # read VDI data
```

Requires `libfuse` (Linux) or `macFUSE` (macOS).

### shepherd — Cluster Coordinator

Optional heartbeat monitor for production deployments:

```bash
shepherd -b 0.0.0.0 -p 7100 --heartbeat-interval 5 --failure-timeout 30
```

Tracks sheep node health and reports failures.

## Wire Protocol

All sheepdog components communicate over TCP using a binary protocol:

```
+------------------+-----------------------------------+
| u32 length       | bincode(RequestHeader, SdRequest) |
+------------------+-----------------------------------+
```

- **Client I/O framing**: 4-byte big-endian length prefix + bincode payload
- **Cluster mesh framing**: 4-byte little-endian length prefix + bincode `ClusterMessage`
- **Serialization**: [bincode](https://docs.rs/bincode) with serde
- **Protocol version**: `0x02` (client), `0x09` (inter-sheep)

### Cluster Messages (P2P Mesh)

| Message | Direction | Purpose |
|---------|-----------|---------|
| `Join { node }` | node &rarr; seed | Request to join the cluster |
| `JoinResponse { members }` | seed &rarr; node | Current member list |
| `Leave { node }` | node &rarr; all | Graceful departure |
| `Heartbeat { node }` | node &harr; node | Periodic keepalive (5s) |
| `Notify { data }` | leader &rarr; all | Broadcast command (format, etc.) |
| `Block` | leader &rarr; all | Two-phase update phase 1 |
| `Unblock { data }` | leader &rarr; all | Two-phase update phase 2 |
| `Election { candidate }` | node &rarr; all | Leader election |
| `ElectionResponse { leader }` | node &rarr; node | Election result |

### Object Addressing

Each data object is identified by a 64-bit **Object ID (OID)**:

```
  63       56 55      32 31                 0
  +----------+----------+-------------------+
  |  flags   |   VDI ID |   object index    |
  +----------+----------+-------------------+
```

- **VDI ID**: 24-bit virtual disk identifier (up to 16M VDIs)
- **Object index**: 32-bit index within the VDI
- **Object size**: 4 MB (`SD_DATA_OBJ_SIZE`)
- **Max VDI size**: 16 EB (4 MB x 2^32 objects)

## Key Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `SD_DATA_OBJ_SIZE` | 4 MB | Size of each data object |
| `SD_LISTEN_PORT` | 7000 | Default daemon port |
| `SD_DEFAULT_COPIES` | 3 | Default replica count |
| `SD_MAX_NODES` | 6144 | Maximum cluster nodes |
| `SD_NR_VDIS` | 16M | Maximum VDI count |
| `SD_DEFAULT_VNODES` | 128 | Virtual nodes per physical node |
| `HEARTBEAT_INTERVAL` | 5s | P2P mesh heartbeat interval |
| `HEARTBEAT_TIMEOUT` | 15s | Peer failure detection timeout |

## Feature Flags

The `sheep` crate supports optional features:

| Feature | Default | Description |
|---------|---------|-------------|
| `http` | yes | HTTP/S3 and Swift API (requires axum) |
| `nfs` | no | NFS v3 server |

```bash
# Build without HTTP
cargo build -p sheep --no-default-features

# Build with NFS
cargo build -p sheep --features nfs
```

## Comparison with C Sheepdog

| Feature | C Sheepdog | sheepdog-rs |
|---------|------------|-------------|
| Language | C | Rust (async, memory-safe) |
| Cluster membership | Corosync / ZooKeeper | Built-in P2P TCP mesh |
| External dependencies | corosync, libcpg | None |
| Async I/O | epoll + callbacks | tokio async/await |
| Serialization | Custom binary | bincode + serde |
| HTTP API | Custom HTTP parser | axum |
| Leader election | Corosync CPG | Deterministic (min NodeId) |
| Atomic updates | Corosync two-phase | Block/Unblock messages |

## Project Status

This is a Rust port of the [C Sheepdog project](https://github.com/sheepdog/sheepdog) (v0.9.5). The core protocol, data structures, and algorithms have been ported with the following status:

| Component | Status | Notes |
|-----------|--------|-------|
| Protocol types | Complete | All request/response types, OID encoding |
| Consistent hashing | Complete | Virtual node ring with zone awareness |
| P2P cluster driver | Complete | TCP mesh with heartbeat, leader election |
| Cluster event loop | Complete | Join/Leave/Notify/Block/Unblock dispatch |
| Storage backends | Complete | plain, tree, md drivers |
| Client request pipeline | Complete | accept, dispatch, gateway forwarding |
| Object replication | Complete | Synchronous multi-copy writes |
| Recovery worker | Complete | Peer object list query + migration |
| Object cache | Complete | LRU with dashmap |
| Journal | Complete | Memory-mapped WAL |
| HTTP/S3 API | Complete | axum-based S3-compatible interface |
| Swift API | Complete | OpenStack Swift-compatible interface |
| NFS v3 | Complete | ONC RPC framing + NFS3 procedures |
| CLI (dog) | Complete | All subcommands wired |
| FUSE (sheepfs) | Complete | Real sheep daemon connectivity |
| Shepherd | Complete | Heartbeat monitoring + status |
| Erasure coding | Partial | reed-solomon-erasure integrated |
| QEMU block driver | Not ported | Requires QEMU C plugin |

## Dependencies

Core dependencies:

- [tokio](https://tokio.rs/) — async runtime
- [serde](https://serde.rs/) + [bincode](https://docs.rs/bincode) — serialization
- [clap](https://docs.rs/clap) — CLI argument parsing
- [tracing](https://docs.rs/tracing) — structured logging
- [axum](https://docs.rs/axum) — HTTP/S3 server (optional)
- [fuser](https://docs.rs/fuser) — FUSE bindings (sheepfs only)
- [dashmap](https://docs.rs/dashmap) — concurrent hash map
- [memmap2](https://docs.rs/memmap2) — memory-mapped I/O
- [reed-solomon-erasure](https://docs.rs/reed-solomon-erasure) — erasure coding

## License

GPL-2.0 — same as the original Sheepdog project.

## References

- [Sheepdog Project](https://sheepdog.github.io/sheepdog/)
- [Sheepdog Wiki](https://github.com/sheepdog/sheepdog/wiki)
- [QEMU Sheepdog Documentation](https://www.qemu.org/docs/master/system/devices/sheepdog.html)
