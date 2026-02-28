//! Async TCP/Unix socket I/O for sheepdog.

use sheepdog_proto::{SdError, SdResult};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tracing::{debug, error};

/// Connect to a remote sheepdog node by address and port.
pub async fn connect_to(addr: &str, port: u16) -> SdResult<TcpStream> {
    let target = format!("{}:{}", addr, port);
    debug!("connecting to {}", target);

    let stream = TcpStream::connect(&target)
        .await
        .map_err(|_| SdError::NetworkError)?;

    stream.set_nodelay(true).ok();
    Ok(stream)
}

/// Connect to a socket address.
pub async fn connect_to_addr(addr: SocketAddr) -> SdResult<TcpStream> {
    debug!("connecting to {}", addr);
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|_| SdError::NetworkError)?;

    stream.set_nodelay(true).ok();
    Ok(stream)
}

/// Create a TCP listener on the given address and port.
pub async fn create_listen_socket(
    bind_addr: &str,
    port: u16,
) -> SdResult<tokio::net::TcpListener> {
    let addr = format!("{}:{}", bind_addr, port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| {
            error!("failed to bind to {}: {}", addr, e);
            SdError::SystemError
        })?;

    debug!("listening on {}", addr);
    Ok(listener)
}
