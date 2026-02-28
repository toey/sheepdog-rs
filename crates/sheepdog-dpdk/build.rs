//! Build script for sheepdog-dpdk.
//!
//! Locates the system DPDK installation via pkg-config and generates
//! Rust FFI bindings using bindgen.
//!
//! Requirements:
//! - DPDK 21.11+ installed (e.g. `apt install dpdk-dev` or built from source)
//! - pkg-config can find `libdpdk`

fn main() {
    // Try to find DPDK via pkg-config
    let dpdk = match pkg_config::Config::new()
        .atleast_version("21.11")
        .probe("libdpdk")
    {
        Ok(lib) => lib,
        Err(e) => {
            // If DPDK is not installed, print a helpful message but don't fail.
            // The crate compiles with stub implementations when DPDK headers
            // aren't available (see lib.rs `dpdk_available` cfg).
            eprintln!("cargo:warning=DPDK not found via pkg-config: {}", e);
            eprintln!("cargo:warning=Building sheepdog-dpdk with stub implementation.");
            eprintln!("cargo:warning=Install DPDK dev libraries for full support:");
            eprintln!("cargo:warning=  Ubuntu/Debian: apt install dpdk-dev libdpdk-dev");
            eprintln!("cargo:warning=  Fedora/RHEL:   dnf install dpdk-devel");
            eprintln!("cargo:warning=  From source:   meson build && ninja -C build install");
            println!("cargo:rustc-cfg=dpdk_stub");
            return;
        }
    };

    println!("cargo:rustc-cfg=dpdk_available");

    // Generate bindings for the DPDK headers we need
    let mut builder = bindgen::Builder::default()
        .header_contents(
            "dpdk_wrapper.h",
            r#"
            #include <rte_eal.h>
            #include <rte_ethdev.h>
            #include <rte_mbuf.h>
            #include <rte_mempool.h>
            #include <rte_ring.h>
            #include <rte_ip.h>
            #include <rte_udp.h>
            #include <rte_ether.h>
            #include <rte_lcore.h>
            #include <rte_launch.h>
            "#,
        )
        .allowlist_function("rte_.*")
        .allowlist_type("rte_.*")
        .allowlist_var("RTE_.*")
        .derive_default(true)
        .derive_debug(true);

    // Add DPDK include paths
    for path in &dpdk.include_paths {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }

    let bindings = builder
        .generate()
        .expect("failed to generate DPDK bindings");

    let out_dir = std::env::var("OUT_DIR").unwrap();
    bindings
        .write_to_file(std::path::Path::new(&out_dir).join("dpdk_bindings.rs"))
        .expect("failed to write DPDK bindings");
}
