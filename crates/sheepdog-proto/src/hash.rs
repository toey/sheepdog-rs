/// FNV-1a hash functions for sheepdog.
///
/// These implement the same hashing algorithm as the C version, ensuring
/// consistent hash ring placement.

use crate::constants::SD_NR_VDIS;

/// FNV-1a 64-bit initial basis.
const FNV1A_64_INIT: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_64_PRIME: u64 = 0x0100_0000_01b3;

/// Compute FNV-1a hash over a byte buffer.
#[inline]
pub fn fnv_64a_buf(buf: &[u8], mut hval: u64) -> u64 {
    for &byte in buf {
        hval ^= byte as u64;
        hval = hval.wrapping_mul(FNV_64_PRIME);
    }
    hval
}

/// Compute FNV-1a hash over a single u64 value (faster than buf for u64).
#[inline]
pub fn fnv_64a_64(oid: u64, mut hval: u64) -> u64 {
    for i in 0..8 {
        hval ^= (oid >> (i * 8)) & 0xff;
        hval = hval.wrapping_mul(FNV_64_PRIME);
    }
    hval
}

/// Hash a byte buffer to a u64 (double-hash for better distribution).
#[inline]
pub fn sd_hash(buf: &[u8]) -> u64 {
    let hval = fnv_64a_buf(buf, FNV1A_64_INIT);
    fnv_64a_64(hval, hval)
}

/// Hash a u64 value (double-hash for better distribution).
#[inline]
pub fn sd_hash_64(oid: u64) -> u64 {
    let hval = fnv_64a_64(oid, FNV1A_64_INIT);
    fnv_64a_64(hval, hval)
}

/// Hash the next value in a chain.
#[inline]
pub fn sd_hash_next(hval: u64) -> u64 {
    fnv_64a_64(hval, hval)
}

/// Hash an object ID.
#[inline]
pub fn sd_hash_oid(oid: u64) -> u64 {
    sd_hash_64(oid)
}

/// Hash a VDI name to a 24-bit VDI id space.
#[inline]
pub fn sd_hash_vdi(name: &str) -> u32 {
    let hval = fnv_64a_buf(name.as_bytes(), FNV1A_64_INIT);
    (hval as u32) & (SD_NR_VDIS - 1)
}

/// Generic hash_64 with bit truncation (for hash tables).
#[inline]
pub fn hash_64(val: u64, bits: u32) -> u64 {
    sd_hash_64(val) >> (64 - bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fnv_deterministic() {
        let h1 = sd_hash(b"hello");
        let h2 = sd_hash(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_fnv_different_inputs() {
        let h1 = sd_hash(b"hello");
        let h2 = sd_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_sd_hash_vdi() {
        let h = sd_hash_vdi("test_vdi");
        assert!(h < SD_NR_VDIS);
    }

    #[test]
    fn test_sd_hash_oid_deterministic() {
        let h1 = sd_hash_oid(0x1234_5678_9ABC_DEF0);
        let h2 = sd_hash_oid(0x1234_5678_9ABC_DEF0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_64_bits() {
        let h = hash_64(42, 16);
        assert!(h < (1 << 16));
    }
}
