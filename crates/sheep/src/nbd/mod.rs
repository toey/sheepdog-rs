//! NBD (Network Block Device) export server.
//!
//! Exposes sheepdog VDIs as NBD block devices, allowing QEMU and other
//! NBD clients to connect via `nbd://host:10809/vdi-name`.
//!
//! Implements the NBD fixed newstyle negotiation protocol:
//! - Handshake with FIXED_NEWSTYLE + NO_ZEROES flags
//! - Option negotiation: LIST, GO, EXPORT_NAME, ABORT
//! - Transmission commands: READ, WRITE, DISC, FLUSH, TRIM
//!
//! Wire format reference: <https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md>

use std::io;
use std::net::SocketAddr;

use sheepdog_proto::constants::SD_DATA_OBJ_SIZE;
use sheepdog_proto::defaults::{DEFAULT_NBD_MAX_EXPORT_NAME_LEN, DEFAULT_NBD_MAX_PAYLOAD};
use sheepdog_proto::error::SdError;
use sheepdog_proto::oid::ObjectId;
use sheepdog_proto::request::{RequestHeader, ResponseResult, SdRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::daemon::SharedSys;

// ─── NBD Protocol Constants ────────────────────────────────────────────────

/// Initial server magic: ASCII "NBDMAGIC"
const NBDMAGIC: u64 = 0x4e42444d41474943;
/// Fixed newstyle negotiation magic: ASCII "IHAVEOPT"
const IHAVEOPT: u64 = 0x49484156454f5054;
/// Server option reply magic
const NBD_OPT_REPLY_MAGIC: u64 = 0x3e889045565a9;
/// Transmission request magic
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
/// Simple reply magic
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;

// Handshake flags (server → client, 16-bit)
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 0x0001;
const NBD_FLAG_NO_ZEROES: u16 = 0x0002;

// Client flags (client → server, 32-bit)
const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 0x0001;
const NBD_FLAG_C_NO_ZEROES: u32 = 0x0002;

// Transmission flags (sent with export info, 16-bit)
const NBD_FLAG_HAS_FLAGS: u16 = 0x0001;
const NBD_FLAG_SEND_FLUSH: u16 = 0x0004;
const NBD_FLAG_SEND_FUA: u16 = 0x0008;
const NBD_FLAG_SEND_TRIM: u16 = 0x0020;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 0x0040;
const NBD_FLAG_CAN_MULTI_CONN: u16 = 0x0100;

// Option types (32-bit)
const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_INFO: u32 = 6;
const NBD_OPT_GO: u32 = 7;

// Option reply types (32-bit)
const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x80000001;
const NBD_REP_ERR_INVALID: u32 = 0x80000003;
const NBD_REP_ERR_UNKNOWN: u32 = 0x80000006;

// Info types for NBD_OPT_INFO / NBD_OPT_GO replies
const NBD_INFO_EXPORT: u16 = 0;
const NBD_INFO_BLOCK_SIZE: u16 = 3;

// Command types (16-bit)
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

// Command flags (16-bit)
const NBD_CMD_FLAG_FUA: u16 = 0x0001;

// Error codes (32-bit)
const NBD_OK: u32 = 0;
const NBD_EIO: u32 = 5;
const NBD_ENOMEM: u32 = 12;
const NBD_EINVAL: u32 = 22;
const NBD_ENOSPC: u32 = 28;
const NBD_ENOTSUP: u32 = 95;
const NBD_ESHUTDOWN: u32 = 108;

/// Maximum export name length
const MAX_EXPORT_NAME_LEN: usize = DEFAULT_NBD_MAX_EXPORT_NAME_LEN;

/// Maximum NBD request payload (32 MB)
const MAX_NBD_PAYLOAD: u32 = DEFAULT_NBD_MAX_PAYLOAD;

// ─── Export State ───────────────────────────────────────────────────────────

/// Cached state for a connected NBD export (one VDI).
struct NbdExport {
    /// VDI name
    name: String,
    /// VDI ID
    vid: u32,
    /// Export size in bytes
    size: u64,
    /// Number of replica copies
    nr_copies: u8,
    /// Copy policy
    copy_policy: u8,
}

// ─── Public API ─────────────────────────────────────────────────────────────

/// Start the NBD export server.
///
/// Listens on the given port and spawns a handler task for each client.
/// Each client negotiates an export name (= VDI name) and then enters
/// the transmission phase for block I/O.
pub async fn start_nbd_server(
    sys: SharedSys,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("NBD server listening on {}", addr);

    let shutdown = {
        let s = sys.read().await;
        s.shutdown_notify.clone()
    };

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        info!("NBD client connected from {}", peer);
                        let sys_clone = sys.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_nbd_client(sys_clone, stream, peer).await {
                                debug!("NBD client {} disconnected: {}", peer, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("NBD accept error: {}", e);
                    }
                }
            }
            _ = shutdown.notified() => {
                info!("NBD server shutting down");
                break;
            }
        }
    }

    Ok(())
}

// ─── Per-Client Handler ─────────────────────────────────────────────────────

/// Handle a single NBD client connection from handshake through transmission.
async fn handle_nbd_client(
    sys: SharedSys,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> io::Result<()> {
    // Phase 1: Fixed newstyle handshake
    let no_zeroes = handshake(&mut stream).await?;

    // Phase 2: Option negotiation — returns the selected export
    let export = match negotiate_options(&mut stream, &sys, no_zeroes).await? {
        Some(exp) => exp,
        None => {
            // Client aborted or sent NBD_OPT_EXPORT_NAME that failed
            return Ok(());
        }
    };

    info!(
        "NBD export '{}' (vid={:#x}, size={}) active for {}",
        export.name, export.vid, export.size, peer
    );

    // Phase 3: Transmission — process I/O commands
    transmission_loop(&mut stream, &sys, &export).await?;

    info!("NBD client {} finished", peer);
    Ok(())
}

// ─── Phase 1: Handshake ─────────────────────────────────────────────────────

/// Send the fixed newstyle server greeting. Returns whether NO_ZEROES was negotiated.
async fn handshake(stream: &mut TcpStream) -> io::Result<bool> {
    // Server greeting: NBDMAGIC + IHAVEOPT + flags
    let server_flags: u16 = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
    stream.write_u64(NBDMAGIC).await?;
    stream.write_u64(IHAVEOPT).await?;
    stream.write_u16(server_flags).await?;

    // Read client flags
    let client_flags = stream.read_u32().await?;

    if client_flags & NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "client does not support fixed newstyle",
        ));
    }

    let no_zeroes = client_flags & NBD_FLAG_C_NO_ZEROES != 0;
    debug!("NBD handshake complete: no_zeroes={}", no_zeroes);
    Ok(no_zeroes)
}

// ─── Phase 2: Option Negotiation ────────────────────────────────────────────

/// Negotiate options until the client selects an export or aborts.
/// Returns `Some(NbdExport)` when an export is selected, `None` on abort.
async fn negotiate_options(
    stream: &mut TcpStream,
    sys: &SharedSys,
    no_zeroes: bool,
) -> io::Result<Option<NbdExport>> {
    loop {
        // Read option header
        let magic = stream.read_u64().await?;
        if magic != IHAVEOPT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad option magic: {:#x}", magic),
            ));
        }

        let option = stream.read_u32().await?;
        let data_len = stream.read_u32().await? as usize;

        // Read option data
        if data_len > MAX_EXPORT_NAME_LEN + 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("option data too large: {}", data_len),
            ));
        }
        let mut data = vec![0u8; data_len];
        if data_len > 0 {
            stream.read_exact(&mut data).await?;
        }

        match option {
            NBD_OPT_EXPORT_NAME => {
                let name = String::from_utf8_lossy(&data).to_string();
                debug!("NBD_OPT_EXPORT_NAME: '{}'", name);

                match resolve_export(sys, &name).await {
                    Ok(export) => {
                        // Send export info directly (no reply magic for EXPORT_NAME)
                        stream.write_u64(export.size).await?;
                        let trans_flags = transmission_flags();
                        stream.write_u16(trans_flags).await?;
                        if !no_zeroes {
                            stream.write_all(&[0u8; 124]).await?;
                        }
                        return Ok(Some(export));
                    }
                    Err(_) => {
                        // For EXPORT_NAME, the only error path is to close the connection
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("export '{}' not found", name),
                        ));
                    }
                }
            }

            NBD_OPT_GO | NBD_OPT_INFO => {
                let is_go = option == NBD_OPT_GO;
                let label = if is_go { "GO" } else { "INFO" };

                // Parse: u32 name_len + name + u16 nr_info_requests + info_request_types
                if data.len() < 6 {
                    send_opt_reply(stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }

                let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if 4 + name_len + 2 > data.len() {
                    send_opt_reply(stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }

                let name = String::from_utf8_lossy(&data[4..4 + name_len]).to_string();
                debug!("NBD_OPT_{}: '{}'", label, name);

                match resolve_export(sys, &name).await {
                    Ok(export) => {
                        // Send NBD_REP_INFO with export info
                        let mut info_data = Vec::with_capacity(12);
                        info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                        info_data.extend_from_slice(&export.size.to_be_bytes());
                        let trans_flags = transmission_flags();
                        info_data.extend_from_slice(&trans_flags.to_be_bytes());
                        send_opt_reply(stream, option, NBD_REP_INFO, &info_data).await?;

                        // Send block size info
                        let mut bs_data = Vec::with_capacity(14);
                        bs_data.extend_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
                        bs_data.extend_from_slice(&512u32.to_be_bytes()); // minimum
                        bs_data.extend_from_slice(&(SD_DATA_OBJ_SIZE as u32).to_be_bytes()); // preferred
                        bs_data.extend_from_slice(&(SD_DATA_OBJ_SIZE as u32).to_be_bytes()); // maximum
                        send_opt_reply(stream, option, NBD_REP_INFO, &bs_data).await?;

                        // Send final ACK
                        send_opt_reply(stream, option, NBD_REP_ACK, &[]).await?;

                        if is_go {
                            return Ok(Some(export));
                        }
                        // For INFO, continue option negotiation
                    }
                    Err(_) => {
                        let msg = format!("export '{}' not found", name);
                        send_opt_reply(stream, option, NBD_REP_ERR_UNKNOWN, msg.as_bytes())
                            .await?;
                    }
                }
            }

            NBD_OPT_LIST => {
                debug!("NBD_OPT_LIST");
                let exports = list_exports(sys).await;

                for (name, _vid, _size) in &exports {
                    // Each entry: u32 name_len + name
                    let mut entry = Vec::with_capacity(4 + name.len());
                    entry.extend_from_slice(&(name.len() as u32).to_be_bytes());
                    entry.extend_from_slice(name.as_bytes());
                    send_opt_reply(stream, option, NBD_REP_SERVER, &entry).await?;
                }

                send_opt_reply(stream, option, NBD_REP_ACK, &[]).await?;
            }

            NBD_OPT_ABORT => {
                debug!("NBD_OPT_ABORT");
                send_opt_reply(stream, option, NBD_REP_ACK, &[]).await?;
                return Ok(None);
            }

            _ => {
                debug!("unsupported NBD option: {}", option);
                send_opt_reply(stream, option, NBD_REP_ERR_UNSUP, &[]).await?;
            }
        }
    }
}

/// Send an option reply.
async fn send_opt_reply(
    stream: &mut TcpStream,
    option: u32,
    reply_type: u32,
    data: &[u8],
) -> io::Result<()> {
    stream.write_u64(NBD_OPT_REPLY_MAGIC).await?;
    stream.write_u32(option).await?;
    stream.write_u32(reply_type).await?;
    stream.write_u32(data.len() as u32).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    Ok(())
}

/// Get the transmission flags we advertise.
fn transmission_flags() -> u16 {
    NBD_FLAG_HAS_FLAGS
        | NBD_FLAG_SEND_FLUSH
        | NBD_FLAG_SEND_FUA
        | NBD_FLAG_SEND_TRIM
        | NBD_FLAG_SEND_WRITE_ZEROES
        | NBD_FLAG_CAN_MULTI_CONN
}

// ─── Export Resolution ──────────────────────────────────────────────────────

/// Resolve an export name to VDI metadata.
///
/// The export name is the VDI name. We look up the VDI by name in the
/// in-memory VDI state (which stores name, size, copies). Falls back
/// to reading the inode from the store if the state has no size info.
async fn resolve_export(sys: &SharedSys, name: &str) -> Result<NbdExport, SdError> {
    debug!("resolve_export: looking up VDI '{}'", name);

    // First try in-memory VDI state
    let found = {
        let s = sys.read().await;
        debug!(
            "resolve_export: vdi_state has {} entries",
            s.vdi_state.len(),
        );

        // Search by name in vdi_state (direct name match)
        let by_name = s.vdi_state.values().find(|st| st.name == name);
        if let Some(state) = by_name {
            debug!(
                "resolve_export: found by name, vid={:#x}, size={}",
                state.vid, state.vdi_size
            );
            Some(NbdExport {
                name: state.name.clone(),
                vid: state.vid,
                size: state.vdi_size,
                nr_copies: state.nr_copies,
                copy_policy: state.copy_policy,
            })
        } else {
            // Fallback: hash-based lookup
            match crate::vdi::lookup_vdi_by_name(&s, name) {
                Ok(vid) => {
                    if let Some(state) = s.vdi_state.get(&vid) {
                        debug!("resolve_export: found by hash, vid={:#x}", vid);
                        Some(NbdExport {
                            name: if state.name.is_empty() {
                                name.to_string()
                            } else {
                                state.name.clone()
                            },
                            vid,
                            size: state.vdi_size,
                            nr_copies: state.nr_copies,
                            copy_policy: state.copy_policy,
                        })
                    } else {
                        None
                    }
                }
                Err(_) => None,
            }
        }
    };

    match found {
        Some(export) if export.size > 0 => Ok(export),
        Some(export) => {
            // Size not in state, try reading inode
            debug!(
                "resolve_export: no size in state, reading inode for vid={:#x}",
                export.vid
            );
            match crate::vdi::read_inode(sys, export.vid).await {
                Ok(inode) => Ok(NbdExport {
                    name: inode.name.clone(),
                    vid: export.vid,
                    size: inode.vdi_size,
                    nr_copies: export.nr_copies,
                    copy_policy: export.copy_policy,
                }),
                Err(e) => {
                    warn!("resolve_export: read_inode failed: {}", e);
                    Err(e)
                }
            }
        }
        None => {
            warn!("resolve_export: VDI '{}' not found", name);
            Err(SdError::NoVdi)
        }
    }
}

/// List all available exports (VDIs) — returns (name, vid, size) tuples.
async fn list_exports(sys: &SharedSys) -> Vec<(String, u32, u64)> {
    let s = sys.read().await;
    s.vdi_state
        .iter()
        .filter(|(_, state)| !state.snapshot && state.vdi_size > 0)
        .map(|(vid, state)| {
            let name = if state.name.is_empty() {
                format!("vdi-{:#x}", vid)
            } else {
                state.name.clone()
            };
            (name, *vid, state.vdi_size)
        })
        .collect()
}

// ─── Phase 3: Transmission ──────────────────────────────────────────────────

/// Main I/O loop: read NBD requests, dispatch to sheepdog, send replies.
async fn transmission_loop(
    stream: &mut TcpStream,
    sys: &SharedSys,
    export: &NbdExport,
) -> io::Result<()> {
    loop {
        // Read request header (28 bytes)
        let magic = stream.read_u32().await?;
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad request magic: {:#x}", magic),
            ));
        }

        let cmd_flags = stream.read_u16().await?;
        let cmd_type = stream.read_u16().await?;
        let cookie = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await?;

        match cmd_type {
            NBD_CMD_READ => {
                let result = nbd_read(sys, export, offset, length).await;
                match result {
                    Ok(data) => {
                        send_simple_reply(stream, cookie, NBD_OK, Some(&data)).await?;
                    }
                    Err(e) => {
                        let err_code = sd_error_to_nbd(e);
                        // For read errors, we still need to send `length` bytes
                        let zeros = vec![0u8; length as usize];
                        send_simple_reply(stream, cookie, err_code, Some(&zeros)).await?;
                    }
                }
            }

            NBD_CMD_WRITE => {
                // Read the write payload
                if length > MAX_NBD_PAYLOAD {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "write payload too large",
                    ));
                }
                let mut payload = vec![0u8; length as usize];
                stream.read_exact(&mut payload).await?;

                let fua = cmd_flags & NBD_CMD_FLAG_FUA != 0;
                let result = nbd_write(sys, export, offset, &payload, fua).await;
                let err_code = match result {
                    Ok(()) => NBD_OK,
                    Err(e) => sd_error_to_nbd(e),
                };
                send_simple_reply(stream, cookie, err_code, None).await?;
            }

            NBD_CMD_DISC => {
                debug!("NBD disconnect");
                // No reply for DISC
                return Ok(());
            }

            NBD_CMD_FLUSH => {
                // Sheepdog writes are already sync (sync_all in peer write)
                send_simple_reply(stream, cookie, NBD_OK, None).await?;
            }

            NBD_CMD_TRIM => {
                let result = nbd_trim(sys, export, offset, length).await;
                let err_code = match result {
                    Ok(()) => NBD_OK,
                    Err(e) => sd_error_to_nbd(e),
                };
                send_simple_reply(stream, cookie, err_code, None).await?;
            }

            NBD_CMD_WRITE_ZEROES => {
                let fua = cmd_flags & NBD_CMD_FLAG_FUA != 0;
                let result = nbd_write_zeroes(sys, export, offset, length, fua).await;
                let err_code = match result {
                    Ok(()) => NBD_OK,
                    Err(e) => sd_error_to_nbd(e),
                };
                send_simple_reply(stream, cookie, err_code, None).await?;
            }

            _ => {
                warn!("unsupported NBD command: {}", cmd_type);
                // Read any payload for write-like commands (shouldn't happen)
                send_simple_reply(stream, cookie, NBD_ENOTSUP, None).await?;
            }
        }
    }
}

/// Send a simple reply.
async fn send_simple_reply(
    stream: &mut TcpStream,
    cookie: u64,
    error: u32,
    data: Option<&[u8]>,
) -> io::Result<()> {
    stream.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
    stream.write_u32(error).await?;
    stream.write_u64(cookie).await?;
    if let Some(d) = data {
        stream.write_all(d).await?;
    }
    Ok(())
}

// ─── Block I/O Operations ───────────────────────────────────────────────────

/// Read data from the VDI at the given byte offset.
///
/// Maps the byte range to sheepdog 4MB data objects and reads from each.
async fn nbd_read(
    sys: &SharedSys,
    export: &NbdExport,
    offset: u64,
    length: u32,
) -> Result<Vec<u8>, SdError> {
    if offset + length as u64 > export.size {
        return Err(SdError::InvalidParms);
    }

    let mut result = Vec::with_capacity(length as usize);
    let mut remaining = length as u64;
    let mut pos = offset;

    while remaining > 0 {
        let obj_idx = pos / SD_DATA_OBJ_SIZE;
        let obj_offset = (pos % SD_DATA_OBJ_SIZE) as u32;
        let chunk_len = std::cmp::min(
            remaining,
            SD_DATA_OBJ_SIZE - obj_offset as u64,
        ) as u32;

        let oid = ObjectId::from_vid_data(export.vid, obj_idx);

        let req = SdRequest::ReadObj {
            oid,
            offset: obj_offset,
            length: chunk_len,
        };

        let header = RequestHeader {
            proto_ver: sheepdog_proto::constants::SD_PROTO_VER,
            epoch: sys.read().await.epoch(),
            id: 0,
        };

        let request = crate::request::Request {
            req,
            header,
            local: true,
        };

        match crate::ops::process(sys.clone(), request).await {
            Ok(ResponseResult::Data(data)) => {
                result.extend_from_slice(&data);
            }
            Ok(_) => {
                // Object doesn't exist yet — return zeros (sparse)
                result.extend(std::iter::repeat(0u8).take(chunk_len as usize));
            }
            Err(SdError::NoObj) => {
                // Sparse region — return zeros
                result.extend(std::iter::repeat(0u8).take(chunk_len as usize));
            }
            Err(e) => {
                return Err(e);
            }
        }

        pos += chunk_len as u64;
        remaining -= chunk_len as u64;
    }

    Ok(result)
}

/// Write data to the VDI at the given byte offset.
///
/// Maps the byte range to sheepdog 4MB data objects and writes to each.
async fn nbd_write(
    sys: &SharedSys,
    export: &NbdExport,
    offset: u64,
    data: &[u8],
    _fua: bool,
) -> Result<(), SdError> {
    let length = data.len() as u64;
    if offset + length > export.size {
        return Err(SdError::InvalidParms);
    }

    let mut remaining = length;
    let mut pos = offset;
    let mut data_pos: usize = 0;

    while remaining > 0 {
        let obj_idx = pos / SD_DATA_OBJ_SIZE;
        let obj_offset = (pos % SD_DATA_OBJ_SIZE) as u32;
        let chunk_len = std::cmp::min(
            remaining,
            SD_DATA_OBJ_SIZE - obj_offset as u64,
        ) as usize;

        let oid = ObjectId::from_vid_data(export.vid, obj_idx);
        let chunk_data = data[data_pos..data_pos + chunk_len].to_vec();

        // Use WriteObj (gateway operation) — it handles replication
        let req = SdRequest::WriteObj {
            oid,
            offset: obj_offset,
            data: chunk_data,
        };

        let header = RequestHeader {
            proto_ver: sheepdog_proto::constants::SD_PROTO_VER,
            epoch: sys.read().await.epoch(),
            id: 0,
        };

        let request = crate::request::Request {
            req,
            header,
            local: true,
        };

        match crate::ops::process(sys.clone(), request).await {
            Ok(_) => {}
            Err(SdError::NoObj) => {
                // Object doesn't exist yet — create it
                let req = SdRequest::CreateAndWriteObj {
                    oid,
                    cow_oid: ObjectId::new(0),
                    copies: export.nr_copies,
                    copy_policy: export.copy_policy,
                    offset: obj_offset,
                    data: data[data_pos..data_pos + chunk_len].to_vec(),
                };

                let header = RequestHeader {
                    proto_ver: sheepdog_proto::constants::SD_PROTO_VER,
                    epoch: sys.read().await.epoch(),
                    id: 0,
                };

                let request = crate::request::Request {
                    req,
                    header,
                    local: true,
                };

                crate::ops::process(sys.clone(), request).await?;
            }
            Err(e) => return Err(e),
        }

        pos += chunk_len as u64;
        data_pos += chunk_len;
        remaining -= chunk_len as u64;
    }

    Ok(())
}

/// Trim (discard) a range. Advisory — we zero the range.
async fn nbd_trim(
    sys: &SharedSys,
    export: &NbdExport,
    offset: u64,
    length: u32,
) -> Result<(), SdError> {
    if offset + length as u64 > export.size {
        return Err(SdError::InvalidParms);
    }

    let mut remaining = length as u64;
    let mut pos = offset;

    while remaining > 0 {
        let obj_idx = pos / SD_DATA_OBJ_SIZE;
        let obj_offset = (pos % SD_DATA_OBJ_SIZE) as u32;
        let chunk_len = std::cmp::min(
            remaining,
            SD_DATA_OBJ_SIZE - obj_offset as u64,
        ) as u32;

        let oid = ObjectId::from_vid_data(export.vid, obj_idx);

        let req = SdRequest::DiscardObj {
            oid,
            offset: obj_offset,
            length: chunk_len,
        };

        let header = RequestHeader {
            proto_ver: sheepdog_proto::constants::SD_PROTO_VER,
            epoch: sys.read().await.epoch(),
            id: 0,
        };

        let request = crate::request::Request {
            req,
            header,
            local: true,
        };

        // Best effort — ignore errors on trim
        let _ = crate::ops::process(sys.clone(), request).await;

        pos += chunk_len as u64;
        remaining -= chunk_len as u64;
    }

    Ok(())
}

/// Write zeros to a range.
async fn nbd_write_zeroes(
    sys: &SharedSys,
    export: &NbdExport,
    offset: u64,
    length: u32,
    fua: bool,
) -> Result<(), SdError> {
    let zeros = vec![0u8; length as usize];
    nbd_write(sys, export, offset, &zeros, fua).await
}

/// Map a sheepdog error to an NBD error code.
fn sd_error_to_nbd(err: SdError) -> u32 {
    match err {
        SdError::InvalidParms => NBD_EINVAL,
        SdError::NoSpace | SdError::FullVdi => NBD_ENOSPC,
        SdError::NoSupport => NBD_ENOTSUP,
        SdError::Shutdown | SdError::Killed => NBD_ESHUTDOWN,
        SdError::NoMem => NBD_ENOMEM,
        _ => NBD_EIO,
    }
}
