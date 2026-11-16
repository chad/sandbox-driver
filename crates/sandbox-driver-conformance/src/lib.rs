//! Black-box conformance suites for sandbox-driver providers.
//!
//! The suite is [`contract`].

mod check;
pub mod contract;
#[doc(hidden)]
pub mod legacy;
mod report;

pub use report::{CheckResult, Outcome, Report};
