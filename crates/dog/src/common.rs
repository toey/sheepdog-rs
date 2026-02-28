//! Shared utilities for the dog CLI tool.
//!
//! Provides connection helpers, request/response framing, and formatting
//! functions used across all subcommands.

use sheepdog_proto::error::{SdError, SdResult};
use sheepdog_proto::request::{RequestHeader, SdRequest, SdResponse, ResponseResult};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connect to a sheep daemon at the given address and port.
pub async fn connect_to_sheep(addr: &str, port: u16) -> SdResult<TcpStream> {
    let target = format!("{}:{}", addr, port);
    let stream = TcpStream::connect(&target)
        .await
        .map_err(|e| {
            eprintln!("Failed to connect to {}: {}", target, e);
            SdError::NetworkError
        })?;
    stream.set_nodelay(true).ok();
    Ok(stream)
}

/// Send a request to the sheep daemon and read back the response.
///
/// Wire format (matching sheep's `handle_client`):
///   Request:  [u32 length][bincode((RequestHeader, SdRequest))]
///   Response: [u32 length][bincode(SdResponse)]
pub async fn send_request(
    stream: &mut TcpStream,
    req: SdRequest,
) -> SdResult<SdResponse> {
    let header = RequestHeader {
        proto_ver: sheepdog_proto::constants::SD_PROTO_VER,
        epoch: 0,
        id: 1,
    };

    // Serialize (header, request) tuple
    let payload = bincode::serialize(&(&header, &req)).map_err(|e| {
        eprintln!("Failed to serialize request: {}", e);
        SdError::SystemError
    })?;

    // Write length-prefixed frame
    stream
        .write_u32(payload.len() as u32)
        .await
        .map_err(|_| SdError::NetworkError)?;
    stream
        .write_all(&payload)
        .await
        .map_err(|_| SdError::NetworkError)?;

    // Read response length
    let resp_len = stream
        .read_u32()
        .await
        .map_err(|_| SdError::NetworkError)? as usize;

    if resp_len > 64 * 1024 * 1024 {
        eprintln!("Response too large: {} bytes", resp_len);
        return Err(SdError::SystemError);
    }

    // Read response body
    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .await
        .map_err(|_| SdError::NetworkError)?;

    // Deserialize response
    let response: SdResponse = bincode::deserialize(&resp_buf).map_err(|e| {
        eprintln!("Failed to deserialize response: {}", e);
        SdError::SystemError
    })?;

    Ok(response)
}

/// Send a request and check for success, returning the ResponseResult.
pub async fn send_request_ok(
    stream: &mut TcpStream,
    req: SdRequest,
) -> SdResult<ResponseResult> {
    let resp = send_request(stream, req).await?;
    match resp.result {
        ResponseResult::Error(e) => {
            eprintln!("Error: {}", e);
            Err(e)
        }
        other => Ok(other),
    }
}

/// Format a byte count as a human-readable size string.
///
/// Examples: "1.0 GB", "512.0 MB", "4.0 TB"
pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;
    const PB: u64 = 1024 * TB;

    if bytes >= PB {
        format!("{:.1} PB", bytes as f64 / PB as f64)
    } else if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format an epoch-seconds timestamp as a human-readable string.
pub fn format_time(epoch_secs: u64) -> String {
    if epoch_secs == 0 {
        return "-".to_string();
    }
    // Use a simple manual formatting since we don't want to pull in chrono
    let secs_per_min = 60u64;
    let secs_per_hour = 3600u64;
    let secs_per_day = 86400u64;

    // Days since epoch
    let days = epoch_secs / secs_per_day;
    let remainder = epoch_secs % secs_per_day;
    let hours = remainder / secs_per_hour;
    let minutes = (remainder % secs_per_hour) / secs_per_min;
    let seconds = remainder % secs_per_min;

    // Simple year/month/day calculation from days since epoch (1970-01-01)
    let (year, month, day) = days_to_date(days);

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Parse a size string with optional K/M/G/T suffix.
///
/// Examples: "100G" = 100 * 1024^3, "512M" = 512 * 1024^2, "1T" = 1024^4
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }

    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1024u64),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1024u64 * 1024),
        Some(b'G' | b'g') => (&s[..s.len() - 1], 1024u64 * 1024 * 1024),
        Some(b'T' | b't') => (&s[..s.len() - 1], 1024u64 * 1024 * 1024 * 1024),
        _ => (s, 1u64),
    };

    let num: u64 = num_str
        .parse()
        .map_err(|e| format!("invalid size '{}': {}", s, e))?;

    num.checked_mul(multiplier)
        .ok_or_else(|| format!("size '{}' overflows u64", s))
}

/// Print a success message.
pub fn print_success(msg: &str) {
    println!("{}", msg);
}

/// Print an error message and exit.
pub fn exit_error(msg: &str) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

/// Cluster status as a display string.
pub fn cluster_status_str(status: &sheepdog_proto::node::ClusterStatus) -> &'static str {
    use sheepdog_proto::node::ClusterStatus;
    match status {
        ClusterStatus::Ok => "running",
        ClusterStatus::WaitForFormat => "waiting for cluster to be formatted",
        ClusterStatus::WaitForJoin => "waiting for other nodes to join",
        ClusterStatus::Shutdown => "shutting down",
        ClusterStatus::Killed => "killed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100G").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("1T").unwrap(), 1024u64 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("2048K").unwrap(), 2048 * 1024);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1023), "1023 B");
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1024 * 1024), "1.0 MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_format_time() {
        assert_eq!(format_time(0), "-");
        // 2024-01-01 00:00:00 UTC = 1704067200
        assert_eq!(format_time(1704067200), "2024-01-01 00:00:00");
    }
}
