//! Core traits and types for driving sandboxes across providers.
//!
//! The API is [`contract`]. The types it shares with the API it replaces are
//! exported here.

pub mod contract;
mod id;
#[doc(hidden)]
#[expect(
    clippy::absolute_paths,
    reason = "temporary module: its files keep the paths they had at the crate root"
)]
pub mod legacy;
mod redact;
mod wire_time;

pub use contract::{
    FileKind, Isolation, OutputLoss, SshAccessInfo, StopLevel, VncConnection, WorkspaceOwnership,
    stop_signal,
};
pub use id::{
    EnvironmentName, InvalidIdError, NamespaceId, ProviderKind, ProviderName, SandboxId,
    Sha256Digest, SnapshotId,
};

#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
pub struct ReadmeDoctests;
