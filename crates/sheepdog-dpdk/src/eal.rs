//! DPDK Environment Abstraction Layer (EAL) initialization.
//!
//! Handles:
//! - EAL init with user-provided arguments (core mask, memory, hugepages)
//! - Memory pool (mempool) creation for packet buffers (mbufs)
//! - Ethernet port configuration, RX/TX queue setup, and port start
//!
//! On systems without DPDK installed, this module provides stub types
//! that allow the code to compile (used for type checking only).

use std::net::IpAddr;

/// Configuration for DPDK initialization.
#[derive(Debug, Clone)]
pub struct DpdkConfig {
    /// EAL arguments (e.g. ["-l", "0-3", "-n", "4", "--file-prefix", "sheep"])
    pub eal_args: Vec<String>,
    /// Number of RX/TX queues per port
    pub nr_queues: u16,
    /// Number of mbufs in the memory pool
    pub nr_mbufs: u32,
    /// Per-core mbuf cache size
    pub mbuf_cache_size: u32,
    /// DPDK port IDs to initialize
    pub port_ids: Vec<u16>,
    /// UDP port for the sheepdog data plane
    pub data_port: u16,
    /// Local IP address assigned to the DPDK interface
    pub local_ip: IpAddr,
}

/// DPDK runtime context (opaque handle).
///
/// On systems with DPDK, this holds raw pointers to the mempool and
/// port state. On systems without DPDK, this is a stub.
pub struct DpdkContext {
    pub config: DpdkConfig,

    // --- Fields below are only populated when DPDK is available ---
    #[cfg(dpdk_available)]
    pub mempool: *mut std::ffi::c_void,
    #[cfg(dpdk_available)]
    pub mac_addrs: Vec<[u8; 6]>,
}

// SAFETY: The DPDK mempool and port handles are thread-safe (DPDK
// documents that rte_mempool is safe for concurrent access).
unsafe impl Send for DpdkContext {}
unsafe impl Sync for DpdkContext {}

impl DpdkContext {
    /// Initialize DPDK EAL, create mempool, configure ports.
    ///
    /// On systems without DPDK this returns an error.
    #[cfg(dpdk_available)]
    pub fn init(config: DpdkConfig) -> Result<Self, String> {
        // Include the generated bindings
        #[allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code)]
        mod ffi {
            include!(concat!(env!("OUT_DIR"), "/dpdk_bindings.rs"));
        }

        use std::ffi::CString;

        // Prepare EAL arguments: "sheep" (program name) + user args
        let mut c_args: Vec<CString> = Vec::new();
        c_args.push(CString::new("sheep").unwrap());
        for arg in &config.eal_args {
            c_args.push(CString::new(arg.as_str()).map_err(|e| format!("invalid EAL arg: {}", e))?);
        }
        let mut c_ptrs: Vec<*mut std::os::raw::c_char> = c_args
            .iter()
            .map(|s| s.as_ptr() as *mut _)
            .collect();

        // Initialize EAL
        let ret = unsafe {
            ffi::rte_eal_init(c_ptrs.len() as i32, c_ptrs.as_mut_ptr())
        };
        if ret < 0 {
            return Err(format!("rte_eal_init failed (ret={})", ret));
        }

        // Create memory pool
        let pool_name = CString::new("sheep_mbuf_pool").unwrap();
        let mempool = unsafe {
            ffi::rte_pktmbuf_pool_create(
                pool_name.as_ptr(),
                config.nr_mbufs,
                config.mbuf_cache_size,
                0,    // priv_size
                ffi::RTE_MBUF_DEFAULT_BUF_SIZE as u16,
                ffi::rte_socket_id() as i32,
            )
        };
        if mempool.is_null() {
            return Err("rte_pktmbuf_pool_create failed".to_string());
        }

        // Configure each port
        let mut mac_addrs = Vec::new();
        for &port_id in &config.port_ids {
            // Configure port with nr_queues RX + TX
            let mut port_conf: ffi::rte_eth_conf = unsafe { std::mem::zeroed() };
            let ret = unsafe {
                ffi::rte_eth_dev_configure(
                    port_id,
                    config.nr_queues,
                    config.nr_queues,
                    &port_conf as *const _ as *mut _,
                )
            };
            if ret != 0 {
                return Err(format!("rte_eth_dev_configure({}) failed: {}", port_id, ret));
            }

            // Setup RX queues
            for q in 0..config.nr_queues {
                let ret = unsafe {
                    ffi::rte_eth_rx_queue_setup(
                        port_id,
                        q,
                        256,  // nb_rx_desc
                        ffi::rte_eth_dev_socket_id(port_id) as u32,
                        std::ptr::null(),
                        mempool,
                    )
                };
                if ret != 0 {
                    return Err(format!("rte_eth_rx_queue_setup({}, {}) failed: {}", port_id, q, ret));
                }
            }

            // Setup TX queues
            for q in 0..config.nr_queues {
                let ret = unsafe {
                    ffi::rte_eth_tx_queue_setup(
                        port_id,
                        q,
                        256,  // nb_tx_desc
                        ffi::rte_eth_dev_socket_id(port_id) as u32,
                        std::ptr::null(),
                    )
                };
                if ret != 0 {
                    return Err(format!("rte_eth_tx_queue_setup({}, {}) failed: {}", port_id, q, ret));
                }
            }

            // Start the port
            let ret = unsafe { ffi::rte_eth_dev_start(port_id) };
            if ret != 0 {
                return Err(format!("rte_eth_dev_start({}) failed: {}", port_id, ret));
            }

            // Read MAC address
            let mut mac: ffi::rte_ether_addr = unsafe { std::mem::zeroed() };
            unsafe { ffi::rte_eth_macaddr_get(port_id, &mut mac) };
            mac_addrs.push(mac.addr_bytes);

            tracing::info!(
                "DPDK port {} started: MAC={:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                port_id,
                mac.addr_bytes[0], mac.addr_bytes[1], mac.addr_bytes[2],
                mac.addr_bytes[3], mac.addr_bytes[4], mac.addr_bytes[5],
            );
        }

        Ok(Self {
            config,
            mempool: mempool as *mut _,
            mac_addrs,
        })
    }

    /// Stub init for systems without DPDK.
    #[cfg(dpdk_stub)]
    pub fn init(config: DpdkConfig) -> Result<Self, String> {
        Err("DPDK libraries not found. Install dpdk-dev and rebuild with: cargo build -p sheep --features dpdk".to_string())
    }
}

impl Drop for DpdkContext {
    fn drop(&mut self) {
        #[cfg(dpdk_available)]
        {
            // Stop ports
            #[allow(non_upper_case_globals, non_camel_case_types, non_snake_case, dead_code)]
            mod ffi {
                include!(concat!(env!("OUT_DIR"), "/dpdk_bindings.rs"));
            }

            for &port_id in &self.config.port_ids {
                unsafe {
                    ffi::rte_eth_dev_stop(port_id);
                    ffi::rte_eth_dev_close(port_id);
                }
            }

            // EAL cleanup
            unsafe {
                ffi::rte_eal_cleanup();
            }
        }
    }
}
