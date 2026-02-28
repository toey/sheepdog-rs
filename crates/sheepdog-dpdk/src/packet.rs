//! DPDK packet framing — fragmentation and reassembly for peer I/O over UDP.
//!
//! Wire format for each UDP packet:
//!
//! ```text
//! +-------------------+---------------------------+
//! | DpdkPeerHeader    | Payload (up to ~8960 B)   |
//! | (16 bytes)        |                           |
//! +-------------------+---------------------------+
//! ```
//!
//! For messages larger than a single UDP packet (e.g. 4 MB data objects),
//! the payload is fragmented across multiple packets. The receiver reassembles
//! fragments using the `request_id` and `frag_index` fields.

use std::collections::HashMap;
use std::time::Instant;

/// Maximum payload bytes per UDP fragment.
///
/// With jumbo frames (MTU 9000): 9000 - 14 (eth) - 20 (IP) - 8 (UDP) - 16 (header) = 8942
/// With standard MTU (1500): 1500 - 14 - 20 - 8 - 16 = 1442
pub const MAX_FRAGMENT_PAYLOAD: usize = 8942;

/// Standard MTU fragment payload size.
pub const STD_FRAGMENT_PAYLOAD: usize = 1442;

/// Header prepended to each DPDK peer packet.
#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct DpdkPeerHeader {
    /// Unique request ID for matching request/response and fragment reassembly.
    pub request_id: u64,
    /// Flags: FIRST_FRAG | LAST_FRAG | IS_RESPONSE
    pub flags: u16,
    /// Fragment index within this message (0-based).
    pub frag_index: u16,
    /// Total number of fragments in this message.
    pub total_frags: u16,
    /// Length of the payload in this specific packet.
    pub payload_len: u16,
}

/// Header flag: this is the first fragment.
pub const FLAG_FIRST_FRAG: u16 = 0x01;
/// Header flag: this is the last fragment.
pub const FLAG_LAST_FRAG: u16 = 0x02;
/// Header flag: this packet is a response (not a request).
pub const FLAG_IS_RESPONSE: u16 = 0x04;
/// Header flag: single packet (not fragmented).
pub const FLAG_SINGLE: u16 = FLAG_FIRST_FRAG | FLAG_LAST_FRAG;

impl DpdkPeerHeader {
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Serialize the header to bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8..10].copy_from_slice(&self.flags.to_le_bytes());
        buf[10..12].copy_from_slice(&self.frag_index.to_le_bytes());
        buf[12..14].copy_from_slice(&self.total_frags.to_le_bytes());
        buf[14..16].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    /// Deserialize the header from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        Some(Self {
            request_id: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            flags: u16::from_le_bytes(buf[8..10].try_into().ok()?),
            frag_index: u16::from_le_bytes(buf[10..12].try_into().ok()?),
            total_frags: u16::from_le_bytes(buf[12..14].try_into().ok()?),
            payload_len: u16::from_le_bytes(buf[14..16].try_into().ok()?),
        })
    }

    /// Check if this is a single (non-fragmented) message.
    pub fn is_single(&self) -> bool {
        self.flags & FLAG_SINGLE == FLAG_SINGLE
    }

    /// Check if this is a response packet.
    pub fn is_response(&self) -> bool {
        self.flags & FLAG_IS_RESPONSE != 0
    }
}

/// Fragment a message into a series of (header, payload) pairs.
pub fn fragment_message(
    request_id: u64,
    data: &[u8],
    is_response: bool,
    max_payload: usize,
) -> Vec<(DpdkPeerHeader, Vec<u8>)> {
    if data.is_empty() {
        // Empty message: single packet with no payload
        let flags = FLAG_SINGLE | if is_response { FLAG_IS_RESPONSE } else { 0 };
        return vec![(
            DpdkPeerHeader {
                request_id,
                flags,
                frag_index: 0,
                total_frags: 1,
                payload_len: 0,
            },
            Vec::new(),
        )];
    }

    let total_frags = (data.len() + max_payload - 1) / max_payload;
    let mut fragments = Vec::with_capacity(total_frags);

    for i in 0..total_frags {
        let start = i * max_payload;
        let end = (start + max_payload).min(data.len());
        let chunk = data[start..end].to_vec();

        let mut flags = if is_response { FLAG_IS_RESPONSE } else { 0 };
        if i == 0 {
            flags |= FLAG_FIRST_FRAG;
        }
        if i == total_frags - 1 {
            flags |= FLAG_LAST_FRAG;
        }

        fragments.push((
            DpdkPeerHeader {
                request_id,
                flags,
                frag_index: i as u16,
                total_frags: total_frags as u16,
                payload_len: chunk.len() as u16,
            },
            chunk,
        ));
    }

    fragments
}

/// Reassembly buffer for incoming fragmented messages.
pub struct ReassemblyBuffer {
    /// In-flight reassemblies keyed by request_id.
    pending: HashMap<u64, PendingMessage>,
    /// Timeout for incomplete messages (seconds).
    timeout_secs: u64,
}

struct PendingMessage {
    total_frags: u16,
    received: u16,
    fragments: Vec<Option<Vec<u8>>>,
    is_response: bool,
    created: Instant,
}

impl ReassemblyBuffer {
    pub fn new(timeout_secs: u64) -> Self {
        Self {
            pending: HashMap::new(),
            timeout_secs,
        }
    }

    /// Insert a received fragment. Returns the reassembled message if complete.
    pub fn insert(&mut self, header: &DpdkPeerHeader, payload: Vec<u8>) -> Option<(u64, Vec<u8>, bool)> {
        // Single-packet message: return immediately
        if header.is_single() {
            return Some((header.request_id, payload, header.is_response()));
        }

        let entry = self
            .pending
            .entry(header.request_id)
            .or_insert_with(|| PendingMessage {
                total_frags: header.total_frags,
                received: 0,
                fragments: vec![None; header.total_frags as usize],
                is_response: header.is_response(),
                created: Instant::now(),
            });

        let idx = header.frag_index as usize;
        if idx < entry.fragments.len() && entry.fragments[idx].is_none() {
            entry.fragments[idx] = Some(payload);
            entry.received += 1;
        }

        // Check if complete
        if entry.received >= entry.total_frags {
            let msg = self.pending.remove(&header.request_id)?;
            let mut data = Vec::new();
            for frag in msg.fragments {
                if let Some(f) = frag {
                    data.extend_from_slice(&f);
                }
            }
            return Some((header.request_id, data, msg.is_response));
        }

        None
    }

    /// Remove timed-out incomplete messages.
    pub fn gc(&mut self) {
        let timeout = std::time::Duration::from_secs(self.timeout_secs);
        self.pending.retain(|_, msg| msg.created.elapsed() < timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let hdr = DpdkPeerHeader {
            request_id: 0x1234_5678_9abc_def0,
            flags: FLAG_SINGLE,
            frag_index: 0,
            total_frags: 1,
            payload_len: 42,
        };
        let bytes = hdr.to_bytes();
        let hdr2 = DpdkPeerHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr2.request_id, hdr.request_id);
        assert_eq!(hdr2.flags, hdr.flags);
        assert_eq!(hdr2.payload_len, 42);
    }

    #[test]
    fn test_fragment_small() {
        let data = vec![0xABu8; 100];
        let frags = fragment_message(1, &data, false, 1000);
        assert_eq!(frags.len(), 1);
        assert!(frags[0].0.is_single());
        assert_eq!(frags[0].1, data);
    }

    #[test]
    fn test_fragment_large() {
        let data = vec![0xCDu8; 3000];
        let frags = fragment_message(2, &data, true, 1000);
        assert_eq!(frags.len(), 3);
        assert!(frags[0].0.is_response());
        assert_eq!(frags[0].0.frag_index, 0);
        assert_eq!(frags[2].0.frag_index, 2);
        assert!(frags[2].0.flags & FLAG_LAST_FRAG != 0);
    }

    #[test]
    fn test_reassembly() {
        let data = vec![0xEFu8; 2500];
        let frags = fragment_message(42, &data, false, 1000);

        let mut buf = ReassemblyBuffer::new(10);
        assert!(buf.insert(&frags[0].0, frags[0].1.clone()).is_none());
        assert!(buf.insert(&frags[1].0, frags[1].1.clone()).is_none());
        let result = buf.insert(&frags[2].0, frags[2].1.clone());
        assert!(result.is_some());
        let (req_id, reassembled, is_resp) = result.unwrap();
        assert_eq!(req_id, 42);
        assert!(!is_resp);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_reassembly_single() {
        let data = vec![0x11u8; 50];
        let frags = fragment_message(99, &data, true, 1000);
        let mut buf = ReassemblyBuffer::new(10);
        let result = buf.insert(&frags[0].0, frags[0].1.clone());
        assert!(result.is_some());
        let (_, reassembled, is_resp) = result.unwrap();
        assert!(is_resp);
        assert_eq!(reassembled, data);
    }
}
