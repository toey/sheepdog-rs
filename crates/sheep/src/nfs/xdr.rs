//! XDR (External Data Representation) encoding/decoding.
//!
//! NFS uses XDR for wire format serialization. This module provides
//! basic XDR encoding and decoding primitives.

use sheepdog_proto::error::{SdError, SdResult};

/// XDR encoder — writes values in network byte order with 4-byte alignment.
pub struct XdrEncoder {
    buf: Vec<u8>,
}

impl XdrEncoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }

    pub fn encode_u32(&mut self, val: u32) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn encode_u64(&mut self, val: u64) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn encode_i32(&mut self, val: i32) {
        self.buf.extend_from_slice(&val.to_be_bytes());
    }

    pub fn encode_bool(&mut self, val: bool) {
        self.encode_u32(if val { 1 } else { 0 });
    }

    /// Encode a variable-length opaque (bytes) with length prefix and padding.
    pub fn encode_opaque(&mut self, data: &[u8]) {
        self.encode_u32(data.len() as u32);
        self.buf.extend_from_slice(data);
        // Pad to 4-byte boundary
        let pad = (4 - (data.len() % 4)) % 4;
        for _ in 0..pad {
            self.buf.push(0);
        }
    }

    /// Encode a string (XDR string = length + bytes + padding).
    pub fn encode_string(&mut self, s: &str) {
        self.encode_opaque(s.as_bytes());
    }

    /// Encode a fixed-length opaque (no length prefix).
    pub fn encode_fixed_opaque(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        let pad = (4 - (data.len() % 4)) % 4;
        for _ in 0..pad {
            self.buf.push(0);
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

/// XDR decoder — reads values in network byte order.
pub struct XdrDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> XdrDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn decode_u32(&mut self) -> SdResult<u32> {
        if self.pos + 4 > self.data.len() {
            return Err(SdError::BufferSmall);
        }
        let val = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(val)
    }

    pub fn decode_u64(&mut self) -> SdResult<u64> {
        if self.pos + 8 > self.data.len() {
            return Err(SdError::BufferSmall);
        }
        let val = u64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(val)
    }

    pub fn decode_i32(&mut self) -> SdResult<i32> {
        let v = self.decode_u32()?;
        Ok(v as i32)
    }

    pub fn decode_bool(&mut self) -> SdResult<bool> {
        let v = self.decode_u32()?;
        Ok(v != 0)
    }

    /// Decode a variable-length opaque.
    pub fn decode_opaque(&mut self) -> SdResult<Vec<u8>> {
        let len = self.decode_u32()? as usize;
        if self.pos + len > self.data.len() {
            return Err(SdError::BufferSmall);
        }
        let val = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        // Skip padding
        let pad = (4 - (len % 4)) % 4;
        self.pos += pad;
        Ok(val)
    }

    /// Decode a string.
    pub fn decode_string(&mut self) -> SdResult<String> {
        let bytes = self.decode_opaque()?;
        String::from_utf8(bytes).map_err(|_| SdError::InvalidParms)
    }

    /// Decode fixed-length opaque.
    pub fn decode_fixed_opaque(&mut self, len: usize) -> SdResult<Vec<u8>> {
        if self.pos + len > self.data.len() {
            return Err(SdError::BufferSmall);
        }
        let val = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        let pad = (4 - (len % 4)) % 4;
        self.pos += pad;
        Ok(val)
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdr_round_trip() {
        let mut enc = XdrEncoder::new();
        enc.encode_u32(42);
        enc.encode_u64(0xDEADBEEF);
        enc.encode_string("hello");
        enc.encode_bool(true);

        let bytes = enc.into_bytes();
        let mut dec = XdrDecoder::new(&bytes);

        assert_eq!(dec.decode_u32().unwrap(), 42);
        assert_eq!(dec.decode_u64().unwrap(), 0xDEADBEEF);
        assert_eq!(dec.decode_string().unwrap(), "hello");
        assert_eq!(dec.decode_bool().unwrap(), true);
    }
}
