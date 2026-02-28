#!/usr/bin/env bash
# ─── Shared defaults for sheepdog-rs test scripts ────────────────────────────
#
# Keep in sync with: crates/sheepdog-proto/src/defaults.rs
#                     crates/sheepdog-proto/src/constants.rs
#
# Usage:
#   source "$(dirname "$0")/defaults.sh"
#
# All variables use the ${VAR:-default} pattern so scripts can override
# them BEFORE sourcing this file.

# ── Network / Port Defaults ──────────────────────────────────────────
BIND="${BIND:-127.0.0.1}"
BASE_PORT="${BASE_PORT:-7000}"          # SD_LISTEN_PORT
HTTP_BASE_PORT="${HTTP_BASE_PORT:-8000}" # DEFAULT_HTTP_PORT
NBD_PORT="${NBD_PORT:-10809}"           # DEFAULT_NBD_PORT
NFS_PORT="${NFS_PORT:-2049}"            # DEFAULT_NFS_PORT
NFS_MOUNT_PORT="${NFS_MOUNT_PORT:-2050}" # DEFAULT_NFS_MOUNT_PORT

# ── Cluster Defaults ─────────────────────────────────────────────────
NUM_NODES="${NUM_NODES:-3}"
DATA_ROOT="${DATA_ROOT:-/tmp/sheepdog-cluster}"
