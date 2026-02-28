//! DPDK user-space data plane transport for sheepdog.
//!
//! This crate provides a high-performance transport that bypasses the kernel
//! network stack using Intel DPDK (Data Plane Development Kit). It communicates
//! over UDP using a poll-mode driver for minimal latency.
//!
//! # Requirements
//!
//! - Linux (DPDK is not available on macOS/Windows)
//! - DPDK 21.11+ development libraries installed
//! - Huge pages configured (e.g. `echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages`)
//! - NIC bound to DPDK-compatible driver (vfio-pci, igb_uio, etc.)
//!
//! # Build
//!
//! ```bash
//! # Install DPDK dev libraries
//! apt install dpdk-dev libdpdk-dev    # Debian/Ubuntu
//!
//! # Build sheep with DPDK support
//! cargo build -p sheep --features dpdk
//! ```
//!
//! # Usage
//!
//! ```bash
//! sheep --dpdk --dpdk-addr 10.0.0.1 --dpdk-port 7100 \
//!       --dpdk-eal-args "-l 0-3 -n 4" /data/sheep
//! ```

pub mod eal;
pub mod packet;
pub mod transport;

pub use eal::DpdkConfig;
pub use transport::DpdkTransport;
