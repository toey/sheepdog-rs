/// Sheepdog error types.
///
/// All error codes from both the public protocol (0x00-0x1D) and the internal
/// protocol (0x81-0x93) are represented as a single enum.

use serde::{Deserialize, Serialize};

/// Unified error type for all sheepdog operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, thiserror::Error)]
pub enum SdError {
    #[error("unknown error")]
    Unknown,
    #[error("no object found")]
    NoObj,
    #[error("I/O error")]
    Eio,
    #[error("VDI exists already")]
    VdiExist,
    #[error("invalid parameters")]
    InvalidParms,
    #[error("system error")]
    SystemError,
    #[error("VDI is locked")]
    VdiLocked,
    #[error("no VDI found")]
    NoVdi,
    #[error("no base VDI found")]
    NoBaseVdi,
    #[error("cannot read requested VDI")]
    VdiRead,
    #[error("cannot write requested VDI")]
    VdiWrite,
    #[error("cannot read base VDI")]
    BaseVdiRead,
    #[error("cannot write base VDI")]
    BaseVdiWrite,
    #[error("requested tag not found")]
    NoTag,
    #[error("sheepdog is starting up")]
    Startup,
    #[error("VDI is not locked")]
    VdiNotLocked,
    #[error("sheepdog is shutting down")]
    Shutdown,
    #[error("cannot allocate memory")]
    NoMem,
    #[error("maximum VDIs reached")]
    FullVdi,
    #[error("protocol version mismatch")]
    VerMismatch,
    #[error("no space available")]
    NoSpace,
    #[error("waiting for format")]
    WaitForFormat,
    #[error("waiting for nodes to join")]
    WaitForJoin,
    #[error("join failed")]
    JoinFailed,
    #[error("sheepdog is halted")]
    Halt,
    #[error("object is read-only")]
    ReadOnly,
    #[error("object upload incomplete")]
    Incomplete,
    #[error("collecting cluster info")]
    CollectingCinfo,
    #[error("inode invalidated")]
    InodeInvalidated,

    // Internal errors (inter-sheep protocol)
    #[error("request has old epoch")]
    OldNodeVer,
    #[error("request has new epoch")]
    NewNodeVer,
    #[error("sheepdog not formatted")]
    NotFormatted,
    #[error("invalid creation time")]
    InvalidCtime,
    #[error("invalid epoch")]
    InvalidEpoch,
    #[error("network error between sheep")]
    NetworkError,
    #[error("no cache object found")]
    NoCache,
    #[error("buffer too small")]
    BufferSmall,
    #[error("force recover not allowed")]
    ForceRecover,
    #[error("no targeted store")]
    NoStore,
    #[error("operation not supported")]
    NoSupport,
    #[error("node is in recovery")]
    NodeInRecovery,
    #[error("node is killed")]
    Killed,
    #[error("object ID already exists")]
    OidExist,
    #[error("try again")]
    Again,
    #[error("object may be stale")]
    StaleObj,
    #[error("cluster driver error")]
    ClusterError,
    #[error("VDI is not empty")]
    VdiNotEmpty,
    #[error("target not found")]
    NotFound,
}

impl SdError {
    /// Convert from a raw protocol error code to SdError.
    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            0x00 => None, // Success
            0x01 => Some(Self::Unknown),
            0x02 => Some(Self::NoObj),
            0x03 => Some(Self::Eio),
            0x04 => Some(Self::VdiExist),
            0x05 => Some(Self::InvalidParms),
            0x06 => Some(Self::SystemError),
            0x07 => Some(Self::VdiLocked),
            0x08 => Some(Self::NoVdi),
            0x09 => Some(Self::NoBaseVdi),
            0x0A => Some(Self::VdiRead),
            0x0B => Some(Self::VdiWrite),
            0x0C => Some(Self::BaseVdiRead),
            0x0D => Some(Self::BaseVdiWrite),
            0x0E => Some(Self::NoTag),
            0x0F => Some(Self::Startup),
            0x10 => Some(Self::VdiNotLocked),
            0x11 => Some(Self::Shutdown),
            0x12 => Some(Self::NoMem),
            0x13 => Some(Self::FullVdi),
            0x14 => Some(Self::VerMismatch),
            0x15 => Some(Self::NoSpace),
            0x16 => Some(Self::WaitForFormat),
            0x17 => Some(Self::WaitForJoin),
            0x18 => Some(Self::JoinFailed),
            0x19 => Some(Self::Halt),
            0x1A => Some(Self::ReadOnly),
            0x1B => Some(Self::Incomplete),
            0x1C => Some(Self::CollectingCinfo),
            0x1D => Some(Self::InodeInvalidated),
            0x81 => Some(Self::OldNodeVer),
            0x82 => Some(Self::NewNodeVer),
            0x83 => Some(Self::NotFormatted),
            0x84 => Some(Self::InvalidCtime),
            0x85 => Some(Self::InvalidEpoch),
            0x86 => Some(Self::NetworkError),
            0x87 => Some(Self::NoCache),
            0x88 => Some(Self::BufferSmall),
            0x89 => Some(Self::ForceRecover),
            0x8A => Some(Self::NoStore),
            0x8B => Some(Self::NoSupport),
            0x8C => Some(Self::NodeInRecovery),
            0x8D => Some(Self::Killed),
            0x8E => Some(Self::OidExist),
            0x8F => Some(Self::Again),
            0x90 => Some(Self::StaleObj),
            0x91 => Some(Self::ClusterError),
            0x92 => Some(Self::VdiNotEmpty),
            0x93 => Some(Self::NotFound),
            _ => Some(Self::Unknown),
        }
    }

    /// Convert to raw protocol error code.
    pub fn to_code(self) -> u32 {
        match self {
            Self::Unknown => 0x01,
            Self::NoObj => 0x02,
            Self::Eio => 0x03,
            Self::VdiExist => 0x04,
            Self::InvalidParms => 0x05,
            Self::SystemError => 0x06,
            Self::VdiLocked => 0x07,
            Self::NoVdi => 0x08,
            Self::NoBaseVdi => 0x09,
            Self::VdiRead => 0x0A,
            Self::VdiWrite => 0x0B,
            Self::BaseVdiRead => 0x0C,
            Self::BaseVdiWrite => 0x0D,
            Self::NoTag => 0x0E,
            Self::Startup => 0x0F,
            Self::VdiNotLocked => 0x10,
            Self::Shutdown => 0x11,
            Self::NoMem => 0x12,
            Self::FullVdi => 0x13,
            Self::VerMismatch => 0x14,
            Self::NoSpace => 0x15,
            Self::WaitForFormat => 0x16,
            Self::WaitForJoin => 0x17,
            Self::JoinFailed => 0x18,
            Self::Halt => 0x19,
            Self::ReadOnly => 0x1A,
            Self::Incomplete => 0x1B,
            Self::CollectingCinfo => 0x1C,
            Self::InodeInvalidated => 0x1D,
            Self::OldNodeVer => 0x81,
            Self::NewNodeVer => 0x82,
            Self::NotFormatted => 0x83,
            Self::InvalidCtime => 0x84,
            Self::InvalidEpoch => 0x85,
            Self::NetworkError => 0x86,
            Self::NoCache => 0x87,
            Self::BufferSmall => 0x88,
            Self::ForceRecover => 0x89,
            Self::NoStore => 0x8A,
            Self::NoSupport => 0x8B,
            Self::NodeInRecovery => 0x8C,
            Self::Killed => 0x8D,
            Self::OidExist => 0x8E,
            Self::Again => 0x8F,
            Self::StaleObj => 0x90,
            Self::ClusterError => 0x91,
            Self::VdiNotEmpty => 0x92,
            Self::NotFound => 0x93,
        }
    }
}

/// Result type alias for sheepdog operations.
pub type SdResult<T> = Result<T, SdError>;

impl From<std::io::Error> for SdError {
    fn from(_: std::io::Error) -> Self {
        SdError::Eio
    }
}
