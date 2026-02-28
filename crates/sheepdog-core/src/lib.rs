//! # sheepdog-core
//!
//! Core shared library for the sheepdog distributed storage system.
//! Provides async networking, consistent hashing, inode operations,
//! and erasure coding.

pub mod consistent_hash;
pub mod fec;
pub mod inode;
pub mod net;
pub mod sockfd_cache;
pub mod tcp_transport;
pub mod transport;
