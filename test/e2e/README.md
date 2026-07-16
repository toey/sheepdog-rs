# Sheepdog-rs E2E Testing Infrastructure

This directory contains Docker-based End-to-End (E2E) testing infrastructure for the sheepdog-rs project. It provides automated testing for all major functionalities including cluster management, NBD, NFS, HTTP/S3, iSCSI, and recovery scenarios.

## Overview

The E2E testing approach uses Docker containers to simulate a multi-node sheepdog cluster in an isolated environment. This allows for:

- **Isolation**: Tests run in a clean, reproducible environment without affecting the host system
- **Automation**: All test cases can be run with a single command
- **Comprehensive Coverage**: Tests cluster formation, data operations, recovery, and various export protocols
- **No QEMU Required**: Uses Docker networking instead of QEMU for NBD testing

## Architecture

The E2E environment consists of:

- **3 Node Containers**: Simulate a 3-node sheepdog cluster
  - Node 0: Primary seed node (ports: 7000/cluster, 8000/HTTP, 10809/NBD, 2049/NFS, 2050/MOUNT, 3260/iSCSI)
  - Node 1: Secondary node (ports: 7002/cluster, 8002/HTTP)
  - Node 2: Tertiary node (ports: 7004/cluster, 8004/HTTP)

- **Admin Container**: Contains test binaries and utilities
  - IP: 172.25.0.10
  - Pre-installed binaries: `sheep`, `dog`, `sheepdog-e2e-test`

## Features Tested

### 1. Cluster Management
- Node discovery and cluster formation
- Cluster health monitoring
- Voting protocol and epoch management
- Cluster format operation

### 2. HTTP/S3/Swift API
- Object storage operations
- Bucket management
- Metadata operations
- Swift-compatible API

### 3. NBD Export
- Block device export
- I/O correctness tests
- Cross-boundary writes
- Sparse file writes
- Read/write performance

### 4. NFS Export
- NFS mount operations
- File creation, modification, deletion
- Directory operations
- Permissions

### 5. Recovery
- Node restart scenarios
- Data integrity verification
- Consistency checks

### 6. Erasure Coding (if feature enabled)
- EC write operations
- Data reconstruction

## Prerequisites

- Docker (Docker Compose)
- No external QEMU or qemu-io needed (uses Docker networking)

## Quick Start

### 1. Build the E2E Image

```bash
cd /Users/bobbytables/Workspace/sheepdog-rs/test/e2e
docker compose -f docker-compose-e2e.yml build
```

This builds a Docker image containing:
- The compiled sheepdog-rs binaries (`sheep`, `dog`)
- Runtime dependencies (qemu-utils, nfs-common, open-iscsi, fuse3, netcat)
- Startup scripts for cluster nodes

### 2. Start the Cluster

```bash
docker compose -f docker-compose-e2e.yml up -d node0 node1 node2 admin
```

Wait for the health check to pass (approx. 10-15 seconds).

### 3. Format the Cluster

```bash
docker compose -f docker-compose-e2e.yml exec admin \
  /workspace/target/release/dog -a 172.25.0.2 -p 7000 cluster format --copies 1
```

### 4. Run E2E Tests

```bash
docker compose -f docker-compose-e2e.yml exec admin \
  /workspace/target/release/sheepdog-e2e-test
```

### 5. Stop the Cluster

```bash
docker compose -f docker-compose-e2e.yml down
```

## Complete Test Workflow

```bash
# Build
cd test/e2e
docker compose build

# Start cluster
docker compose up -d node0 node1 node2 admin

# Wait for cluster to be healthy (optional)
sleep 10

# Format cluster
docker compose exec admin /workspace/target/release/dog -a 172.25.0.2 -p 7000 cluster format --copies 1

# Run tests
docker compose exec admin /workspace/target/release/sheepdog-e2e-test

# Stop cluster
docker compose down
```

## Test Script Details

The `sheepdog-e2e-test` binary runs a comprehensive test suite:

```bash
#!/bin/bash
# test/sheepdog-e2e-test

# Test cluster health
test_cluster_health() {
    dog cluster info
    dog cluster list
}

# Test HTTP/S3 API
test_http_api() {
    # Create bucket
    curl -X PUT http://172.25.0.2:8000/bucket/test-bucket
    # List objects
    curl http://172.25.0.2:8000/bucket/test-bucket/
    # Upload object
    curl -X PUT -d "test data" http://172.25.0.2:8000/bucket/test-bucket/test-file
    # Download object
    curl http://172.25.0.2:8000/bucket/test-bucket/test-file
}

# Test NBD export
test_nbd_export() {
    # Connect to NBD device
    qemu-io -c 'info' -c 'format' -c 'write -z 512 0 4k' nbd://172.25.0.2:10809/test
    # Test read/write
    qemu-io -c 'read 0 4k' nbd://172.25.0.2:10809/test
    qemu-io -c 'write -z 512 0 4k' nbd://172.25.0.2:10809/test
    qemu-io -c 'read 0 4k' nbd://172.25.0.2:10809/test
}

# Test NFS export
test_nfs_export() {
    # Create mount directory
    mkdir -p /tmp/mount
    # Mount NFS
    mount -t nfs -o nolock 172.25.0.2:/ nfs-test
    # Test file operations
    echo "test" > /nfs-test/test.txt
    cat /nfs-test/test.txt
    umount /nfs-test
    rmdir /tmp/mount
}

# Test recovery
test_recovery() {
    # Restart a node
    docker compose restart node1
    # Verify cluster health
    dog cluster info
}
```

## Configuration Options

### Enable/Disable Features

Set environment variables before running:

```bash
# Enable NBD export
ENABLE_NBD=true docker compose up -d node0 node1 node2 admin

# Enable NFS export
ENABLE_NFS=true docker compose up -d node0 node1 node2 admin

# Enable iSCSI export
ENABLE_ISCSI=true docker compose up -d node0 node1 node2 admin
```

### Custom Port Configuration

```bash
# Custom sheep port
PORT=7005 docker compose up -d node0 node1 node2 admin
```

## Troubleshooting

### Cluster won't start

1. Check node logs:
   ```bash
   docker compose logs node0 node1 node2
   ```

2. Verify health check passed:
   ```bash
   docker compose ps
   ```

3. Common issues:
   - Port conflicts: Ensure ports 7000-7004, 8000-8004, 10809, 2049, 2050, 3260 are not in use
   - Network issues: Check Docker network connectivity
   - Seed port mismatch: Ensure node0's cluster port is 7001 (sheep port + 1)

### Tests fail

1. Check admin container logs:
   ```bash
   docker compose logs admin
   ```

2. Verify cluster is formatted:
   ```bash
   docker compose exec admin /workspace/target/release/dog -a 172.25.0.2 -p 7000 cluster info
   ```

3. Rebuild the image if binaries are missing:
   ```bash
   docker compose build
   ```

### Port Conflicts

If ports are in use, choose different ports:
```bash
# In docker-compose-e2e.yml, change PORT for each node
- PORT=7100  # Node 0
- PORT=7102  # Node 1
- PORT=7104  # Node 2
```

## Understanding the Cluster Port

Sheepdog uses two ports per node:
- **Sheep Port**: Main service port (HTTP, NFS, NBD, iSCSI)
- **Cluster Port**: Cluster communication port (Sheep Port + 1)

Example:
```
Sheep Port:  7000
Cluster Port: 7001  # 7000 + 1
```

This is critical for proper cluster formation.

## Extending the Test Suite

To add new test cases:

1. Add test functions to `test/sheepdog-e2e-test`:
   ```bash
   test_new_feature() {
       # Your test logic here
   }
   
   # Run the new test
   test_new_feature
   ```

2. Add assertions for expected behavior:
   ```bash
   assert_success $?
   # Or check specific outputs
   dog cluster info | grep "Nodes: 3"
   ```

## Alternative: Running Tests Manually

Instead of using the test binary, you can run tests manually:

```bash
# Test HTTP API
curl -X PUT http://172.25.0.2:8000/bucket/test
curl http://172.25.0.2:8000/bucket/test/

# Test NBD
qemu-io -c 'write -z 512 0 4k' nbd://172.25.0.2:10809/test
qemu-io -c 'read 0 4k' nbd://172.25.0.2:10809/test

# Test NFS
mount -t nfs -o nolock 172.25.0.2:/ nfs-test
echo "hello" > /nfs-test/file.txt
cat /nfs-test/file.txt
umount nfs-test

# Test cluster
dog cluster info
dog cluster list
```

## Cleanup

To completely clean up the E2E environment:

```bash
cd test/e2e
docker compose down -v  # Remove volumes too
docker network rm e2e_sheepdog-net 2>/dev/null || true
```

## Integration with CI/CD

For CI/CD integration:

```yaml
# .github/workflows/e2e-test.yml
- name: Build E2E image
  run: |
    cd test/e2e
    docker compose build

- name: Start cluster
  run: |
    cd test/e2e
    docker compose up -d node0 node1 node2 admin
    sleep 15

- name: Format cluster
  run: |
    cd test/e2e
    docker compose exec admin /workspace/target/release/dog -a 172.25.0.2 -p 7000 cluster format --copies 1

- name: Run E2E tests
  run: |
    cd test/e2e
    docker compose exec admin /workspace/target/release/sheepdog-e2e-test

- name: Stop cluster
  run: |
    cd test/e2e
    docker compose down
```

## License

This testing infrastructure is provided as-is for testing sheepdog-rs functionality.
