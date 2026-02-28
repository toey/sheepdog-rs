//! # sheepdog-proto
//!
//! Protocol types, constants, and data structures for the sheepdog
//! distributed storage system.
//!
//! This crate defines the wire protocol, object ID manipulation,
//! error types, and core data structures shared by all sheepdog components.

pub mod constants;
pub mod defaults;
pub mod error;
pub mod hash;
pub mod inode;
pub mod node;
pub mod oid;
pub mod request;
pub mod vdi;

// Re-export commonly used types at the crate root
pub use error::{SdError, SdResult};
pub use node::{ClusterInfo, ClusterStatus, NodeId, SdNode};
pub use oid::ObjectId;
pub use request::{SdRequest, SdResponse};
