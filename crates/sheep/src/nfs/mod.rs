//! NFS server — provides NFS3 access to sheepdog VDIs.
//!
//! Enabled by the `nfs` Cargo feature. Implements a minimal NFS3/MOUNT
//! protocol server that exposes each VDI as an NFS export.
//!
//! The server implements ONC RPC (RFC 1831) framing over TCP for both
//! the MOUNT program (program 100005, version 3) and the NFS program
//! (program 100003, version 3).

pub mod handler;
pub mod xdr;
pub mod fs;
pub mod mount;

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use self::fs::NfsFilesystem;
use self::handler::Nfs3Proc;
use self::mount::ExportEntry;
use self::xdr::{XdrDecoder, XdrEncoder};

/// ONC RPC message types.
const RPC_CALL: u32 = 0;
const RPC_REPLY: u32 = 1;

/// RPC reply status.
const MSG_ACCEPTED: u32 = 0;

/// Accept status.
const ACCEPT_SUCCESS: u32 = 0;
const ACCEPT_PROG_UNAVAIL: u32 = 1;

/// ONC RPC program numbers.
const NFS_PROGRAM: u32 = 100003;
const MOUNT_PROGRAM: u32 = 100005;

/// NFS server configuration.
pub struct NfsConfig {
    /// Listen port for NFS (default: 2049).
    pub port: u16,
    /// Listen port for the MOUNT protocol (default: 2050).
    pub mount_port: u16,
}

impl Default for NfsConfig {
    fn default() -> Self {
        Self {
            port: 2049,
            mount_port: 2050,
        }
    }
}

/// Shared NFS state across connections.
struct NfsState {
    /// Per-VID filesystem state.
    filesystems: BTreeMap<u32, NfsFilesystem>,
    /// Export list.
    exports: Vec<ExportEntry>,
}

impl NfsState {
    fn new() -> Self {
        Self {
            filesystems: BTreeMap::new(),
            exports: Vec::new(),
        }
    }
}

/// Start the NFS server.
///
/// Launches two TCP listeners: one for the NFS3 program (default port 2049)
/// and one for the MOUNT program (default port 2050). Both use ONC RPC
/// framing over TCP (record marking with 4-byte fragment headers).
pub async fn start_nfs_server(
    sys: crate::daemon::SharedSys,
    config: NfsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Build export list from in-use VDIs
    let mut state = NfsState::new();
    {
        let s = sys.read().await;
        for (vid, _vdi_state) in &s.vdi_state {
            let path = format!("/sheepdog/{:#x}", vid);
            state.exports.push(ExportEntry {
                path,
                vid: *vid,
            });
            state.filesystems.insert(*vid, NfsFilesystem::new(*vid));
        }
    }

    let state = Arc::new(RwLock::new(state));

    let nfs_addr = format!("0.0.0.0:{}", config.port);
    let mount_addr = format!("0.0.0.0:{}", config.mount_port);

    let nfs_listener = TcpListener::bind(&nfs_addr).await?;
    let mount_listener = TcpListener::bind(&mount_addr).await?;

    info!("NFS3 server listening on {}", nfs_addr);
    info!("MOUNT server listening on {}", mount_addr);

    let shutdown = sys.read().await.shutdown_notify.clone();
    let shutdown2 = shutdown.clone();
    let state2 = state.clone();

    // Spawn NFS3 accept loop
    let nfs_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = nfs_listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            debug!("NFS3 connection from {}", peer);
                            let st = state.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_rpc_connection(stream, st).await {
                                    debug!("NFS3 client {} error: {}", peer, e);
                                }
                            });
                        }
                        Err(e) => error!("NFS3 accept error: {}", e),
                    }
                }
                _ = shutdown.notified() => break,
            }
        }
    });

    // Spawn MOUNT accept loop
    let mount_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = mount_listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            debug!("MOUNT connection from {}", peer);
                            let st = state2.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_rpc_connection(stream, st).await {
                                    debug!("MOUNT client {} error: {}", peer, e);
                                }
                            });
                        }
                        Err(e) => error!("MOUNT accept error: {}", e),
                    }
                }
                _ = shutdown2.notified() => break,
            }
        }
    });

    let _ = tokio::join!(nfs_handle, mount_handle);
    info!("NFS server stopped");
    Ok(())
}

/// Handle a single RPC-over-TCP connection.
///
/// ONC RPC over TCP uses record marking: each record is preceded by a
/// 4-byte header where the high bit indicates "last fragment" and the
/// lower 31 bits give the fragment length.
async fn handle_rpc_connection(
    mut stream: TcpStream,
    state: Arc<RwLock<NfsState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        // Read record mark (4 bytes: last_fragment bit + 31-bit length)
        let record_mark = match stream.read_u32().await {
            Ok(v) => v,
            Err(_) => return Ok(()), // Client disconnected
        };

        let _last_fragment = (record_mark & 0x80000000) != 0;
        let frag_len = (record_mark & 0x7FFFFFFF) as usize;

        if frag_len > 1024 * 1024 {
            warn!("RPC fragment too large: {}", frag_len);
            return Ok(());
        }

        let mut buf = vec![0u8; frag_len];
        stream.read_exact(&mut buf).await?;

        // Parse RPC call header
        let mut dec = XdrDecoder::new(&buf);
        let xid = dec.decode_u32().map_err(|_| "truncated xid")?;
        let msg_type = dec.decode_u32().map_err(|_| "truncated msg_type")?;

        if msg_type != RPC_CALL {
            warn!("unexpected RPC message type: {}", msg_type);
            continue;
        }

        let rpc_version = dec.decode_u32().map_err(|_| "truncated rpc_version")?;
        if rpc_version != 2 {
            let reply = build_rpc_mismatch_reply(xid);
            send_rpc_reply(&mut stream, &reply).await?;
            continue;
        }

        let program = dec.decode_u32().map_err(|_| "truncated program")?;
        let version = dec.decode_u32().map_err(|_| "truncated version")?;
        let procedure = dec.decode_u32().map_err(|_| "truncated procedure")?;

        // Skip auth (credential + verifier)
        let _cred_flavor = dec.decode_u32().unwrap_or(0);
        let _cred_body = dec.decode_opaque().unwrap_or_default();
        let _verf_flavor = dec.decode_u32().unwrap_or(0);
        let _verf_body = dec.decode_opaque().unwrap_or_default();

        // Remaining bytes are procedure arguments
        let remaining = dec.remaining();
        let args = &buf[buf.len() - remaining..];

        debug!("RPC call: prog={} ver={} proc={}", program, version, procedure);

        let reply = match (program, version) {
            (NFS_PROGRAM, 3) => {
                let body = dispatch_nfs3_rpc(procedure, args, &state).await;
                build_accepted_reply(xid, &body)
            }
            (MOUNT_PROGRAM, 3) => {
                let body = dispatch_mount_rpc(procedure, args, &state).await;
                build_accepted_reply(xid, &body)
            }
            _ => build_prog_unavail(xid),
        };

        send_rpc_reply(&mut stream, &reply).await?;
    }
}

/// Dispatch to NFS3 handler.
async fn dispatch_nfs3_rpc(
    procedure: u32,
    args: &[u8],
    state: &Arc<RwLock<NfsState>>,
) -> Vec<u8> {
    let proc_num = match Nfs3Proc::from_u32(procedure) {
        Some(p) => p,
        None => {
            let mut enc = XdrEncoder::new();
            enc.encode_u32(handler::Nfs3Status::Notsupp as u32);
            return enc.into_bytes();
        }
    };

    let st = state.read().await;
    // Use first available filesystem as default
    let fs = match st.filesystems.values().next() {
        Some(fs) => fs,
        None => {
            let mut enc = XdrEncoder::new();
            enc.encode_u32(handler::Nfs3Status::Serverfault as u32);
            return enc.into_bytes();
        }
    };

    match handler::dispatch_nfs3(proc_num, args, fs).await {
        Ok(data) => data,
        Err(e) => {
            warn!("NFS3 proc {:?} error: {}", proc_num, e);
            let mut enc = XdrEncoder::new();
            enc.encode_u32(handler::Nfs3Status::Serverfault as u32);
            enc.into_bytes()
        }
    }
}

/// Dispatch to MOUNT handler.
async fn dispatch_mount_rpc(
    procedure: u32,
    args: &[u8],
    state: &Arc<RwLock<NfsState>>,
) -> Vec<u8> {
    let mut enc = XdrEncoder::new();

    match procedure {
        // NULL
        0 => {}
        // MNT
        1 => {
            let mut dec = XdrDecoder::new(args);
            let path = dec.decode_string().unwrap_or_default();
            let st = state.read().await;
            match mount::handle_mnt(&path, &st.exports) {
                Ok(fh_bytes) => {
                    enc.encode_u32(0); // MNT3_OK
                    enc.encode_opaque(&fh_bytes);
                    enc.encode_u32(1); // 1 auth flavor
                    enc.encode_u32(0); // AUTH_NONE
                }
                Err(_) => {
                    enc.encode_u32(13); // MNT3ERR_ACCES
                }
            }
        }
        // DUMP
        2 => {
            enc.encode_bool(false); // empty dump list
        }
        // UMNT
        3 | 4 => {
            // No-op for UMNT / UMNTALL
        }
        // EXPORT
        5 => {
            let st = state.read().await;
            let paths = mount::handle_export(&st.exports);
            for path in &paths {
                enc.encode_bool(true);
                enc.encode_string(path);
                enc.encode_bool(false); // no groups
            }
            enc.encode_bool(false); // end of exports
        }
        _ => {
            warn!("MOUNT: unsupported proc {}", procedure);
        }
    }

    enc.into_bytes()
}

/// Build an RPC ACCEPTED reply wrapping a procedure result.
fn build_accepted_reply(xid: u32, body: &[u8]) -> Vec<u8> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(xid);
    enc.encode_u32(RPC_REPLY);
    enc.encode_u32(MSG_ACCEPTED);
    // NULL verifier
    enc.encode_u32(0); // AUTH_NONE
    enc.encode_opaque(&[]);
    enc.encode_u32(ACCEPT_SUCCESS);
    // Procedure-specific result
    enc.encode_fixed_opaque(body);
    enc.into_bytes()
}

/// Build an RPC PROG_UNAVAIL reply.
fn build_prog_unavail(xid: u32) -> Vec<u8> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(xid);
    enc.encode_u32(RPC_REPLY);
    enc.encode_u32(MSG_ACCEPTED);
    enc.encode_u32(0);
    enc.encode_opaque(&[]);
    enc.encode_u32(ACCEPT_PROG_UNAVAIL);
    enc.into_bytes()
}

/// Build an RPC_MISMATCH reply.
fn build_rpc_mismatch_reply(xid: u32) -> Vec<u8> {
    let mut enc = XdrEncoder::new();
    enc.encode_u32(xid);
    enc.encode_u32(RPC_REPLY);
    enc.encode_u32(1); // MSG_DENIED
    enc.encode_u32(0); // RPC_MISMATCH
    enc.encode_u32(2); // low version
    enc.encode_u32(2); // high version
    enc.into_bytes()
}

/// Send an RPC reply with record marking header.
async fn send_rpc_reply(
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let record_mark = 0x80000000 | (data.len() as u32);
    stream.write_u32(record_mark).await?;
    stream.write_all(data).await?;
    Ok(())
}
