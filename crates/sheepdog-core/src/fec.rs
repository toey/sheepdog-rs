//! Forward Error Correction (erasure coding) wrapper.
//!
//! Wraps the `reed-solomon-erasure` crate to provide the same erasure
//! coding functionality as the C version's ISA-L based implementation.

use reed_solomon_erasure::galois_8::ReedSolomon;
use sheepdog_proto::error::{SdError, SdResult};

/// Erasure coder for a specific data/parity configuration.
pub struct ErasureCoder {
    rs: ReedSolomon,
    data_strips: usize,
    parity_strips: usize,
}

impl ErasureCoder {
    /// Create a new erasure coder with the given number of data and parity strips.
    pub fn new(data_strips: usize, parity_strips: usize) -> SdResult<Self> {
        let rs = ReedSolomon::new(data_strips, parity_strips)
            .map_err(|_| SdError::InvalidParms)?;
        Ok(Self {
            rs,
            data_strips,
            parity_strips,
        })
    }

    /// Total number of strips (data + parity).
    pub fn total_strips(&self) -> usize {
        self.data_strips + self.parity_strips
    }

    /// Encode data strips, producing parity strips.
    /// Input: vector of `data_strips` data shards.
    /// Output: vector of `parity_strips` parity shards appended.
    pub fn encode(&self, data: &mut Vec<Vec<u8>>) -> SdResult<()> {
        // Add empty parity shards
        let shard_len = data[0].len();
        for _ in 0..self.parity_strips {
            data.push(vec![0u8; shard_len]);
        }

        self.rs.encode(data).map_err(|_| SdError::Eio)?;
        Ok(())
    }

    /// Reconstruct missing shards.
    /// `shards[i]` is `None` if the shard is missing.
    pub fn reconstruct(&self, shards: &mut Vec<Option<Vec<u8>>>) -> SdResult<()> {
        self.rs
            .reconstruct(shards)
            .map_err(|_| SdError::Eio)?;
        Ok(())
    }
}

/// Decode a copy policy byte into (data_strips, parity_strips).
/// Policy byte format: high nibble = data, low nibble = parity.
pub fn ec_policy_to_dp(policy: u8) -> (usize, usize) {
    let d = ((policy >> 4) & 0x0F) as usize;
    let p = (policy & 0x0F) as usize;
    if d == 0 {
        (1, 0) // No erasure coding
    } else {
        (d, p)
    }
}

/// Encode (data_strips, parity_strips) to a copy policy byte.
pub fn dp_to_ec_policy(data_strips: usize, parity_strips: usize) -> u8 {
    ((data_strips as u8) << 4) | (parity_strips as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let ec = ErasureCoder::new(4, 2).unwrap();

        let shard_size = 1024;
        let mut shards: Vec<Vec<u8>> = (0..4)
            .map(|i| vec![(i + 1) as u8; shard_size])
            .collect();

        ec.encode(&mut shards).unwrap();
        assert_eq!(shards.len(), 6);

        // Simulate losing 2 shards
        let original_0 = shards[0].clone();
        let original_3 = shards[3].clone();

        let mut opt_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        opt_shards[0] = None;
        opt_shards[3] = None;

        ec.reconstruct(&mut opt_shards).unwrap();

        assert_eq!(opt_shards[0].as_ref().unwrap(), &original_0);
        assert_eq!(opt_shards[3].as_ref().unwrap(), &original_3);
    }

    #[test]
    fn test_ec_policy() {
        assert_eq!(ec_policy_to_dp(0x42), (4, 2));
        assert_eq!(dp_to_ec_policy(4, 2), 0x42);
    }
}
