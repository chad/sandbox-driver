//! Whole-command helpers over [`Sandbox::exec`]: run a command to completion
//! and keep what it wrote. They are library code, not a provider capability,
//! so they work unchanged on every provider and plugin.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::{fmt, future, mem};

use tokio::time::Instant;

use super::{
    Error, ExecControls, ExecOutcome, ExecSpec, OutputSink, OutputStream, Sandbox, Termination,
};

/// How much of each stream [`run_captured`] and [`run_shell`] keep.
pub const DEFAULT_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

/// One stream of a captured command.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct CapturedOutput {
    /// The stream's first bytes followed by its newest ones. It is the whole
    /// stream when `omitted_bytes` is zero.
    pub bytes:         Vec<u8>,
    /// Bytes dropped between the first and the newest to stay within the
    /// limit.
    pub omitted_bytes: u64,
}

impl CapturedOutput {
    /// Bytes the command wrote to this stream, kept or not.
    pub fn observed_bytes(&self) -> u64 {
        (self.bytes.len() as u64).saturating_add(self.omitted_bytes)
    }
}

// Command output can carry credentials, as `ExecSpec::argv` can.
impl fmt::Debug for CapturedOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedOutput")
            .field("retained_bytes", &self.bytes.len())
            .field("omitted_bytes", &self.omitted_bytes)
            .finish()
    }
}

/// A finished command and the output it wrote.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Captured {
    pub outcome:  ExecOutcome,
    /// Also holds the single terminal stream of a command run with
    /// [`ExecSpec::tty`].
    pub stdout:   CapturedOutput,
    pub stderr:   CapturedOutput,
    /// How long the caller waited for the command: from the request to the
    /// provider until its outcome came back, measured here and not by the
    /// provider. It includes the provider's own setup and the time the
    /// caller's sink took.
    pub duration: Duration,
}

impl Captured {
    /// Whether the command exited on its own with code zero. A command that
    /// was stopped or timed out is not a success, whatever code it reports.
    pub fn success(&self) -> bool {
        self.outcome.termination == Termination::Exited && self.outcome.exit_code == Some(0)
    }

    /// Whether `stdout` and `stderr` hold every byte the command wrote:
    /// nothing was omitted to stay within the limit, and the provider
    /// reported no output loss. Check this before parsing the output.
    pub fn is_complete(&self) -> bool {
        self.stdout.omitted_bytes == 0
            && self.stderr.omitted_bytes == 0
            && !self.outcome.output_loss.is_lossy()
    }
}

/// Runs `spec` to completion and keeps up to [`DEFAULT_CAPTURE_BYTES`] of
/// each stream.
///
/// A command that fails is not an error: the result carries its exit code
/// and output, and [`Captured::success`] says whether it exited with zero.
///
/// # Errors
///
/// Whatever [`Sandbox::exec`] returns. The command may have had effects.
pub async fn run_captured(sandbox: &dyn Sandbox, spec: ExecSpec) -> Result<Captured, Error> {
    run_captured_with(
        sandbox,
        spec,
        ExecControls::default(),
        DEFAULT_CAPTURE_BYTES,
    )
    .await
}

/// [`run_captured`] with the caller's controls and its own limit.
///
/// `limit` is the bytes kept per stream: the first half of it from the start
/// of the stream, the rest from its end. Output beyond it is still read, so
/// the command never blocks on a full buffer, and is counted in
/// [`CapturedOutput::omitted_bytes`]. A limit of zero keeps nothing and
/// counts everything.
///
/// `controls.sink`, when set, also receives every chunk as it arrives, after
/// the chunk is captured. An error from it ends the command as it does for
/// [`Sandbox::exec`].
///
/// # Errors
///
/// Whatever [`Sandbox::exec`] returns. The command may have had effects, and
/// the output captured before the failure is not returned.
pub async fn run_captured_with(
    sandbox: &dyn Sandbox,
    spec: ExecSpec,
    mut controls: ExecControls,
    limit: usize,
) -> Result<Captured, Error> {
    let streams = Arc::new(Mutex::new(Streams::new(limit)));
    let forward = controls.sink.take();
    let sink: OutputSink = {
        let streams = Arc::clone(&streams);
        Arc::new(move |stream, bytes| {
            lock(&streams).push(stream, &bytes);
            match &forward {
                Some(forward) => forward(stream, bytes),
                None => Box::pin(future::ready(Ok(()))),
            }
        })
    };
    controls.sink = Some(sink);
    let started = Instant::now();
    let outcome = sandbox.exec(spec, controls).await?;
    let duration = started.elapsed();
    // A provider may still hold the sink, so the buffers are taken, not
    // unwrapped.
    let mut streams = lock(&streams);
    Ok(Captured {
        outcome,
        stdout: streams.stdout.take(),
        stderr: streams.stderr.take(),
        duration,
    })
}

/// Runs `script` through [`ExecSpec::shell`] and captures it as
/// [`run_captured`] does.
///
/// # Errors
///
/// Whatever [`Sandbox::exec`] returns. The script may have had effects.
pub async fn run_shell(sandbox: &dyn Sandbox, script: &str) -> Result<Captured, Error> {
    run_captured(sandbox, ExecSpec::shell(script)).await
}

/// The buffers hold plain bytes and counts, so a panic elsewhere while the
/// lock was held leaves nothing to repair.
fn lock(streams: &Mutex<Streams>) -> MutexGuard<'_, Streams> {
    streams.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Streams {
    stdout: Retained,
    stderr: Retained,
}

impl Streams {
    fn new(limit: usize) -> Self {
        Self {
            stdout: Retained::new(limit),
            stderr: Retained::new(limit),
        }
    }

    fn push(&mut self, stream: OutputStream, chunk: &[u8]) {
        match stream {
            OutputStream::Stdout | OutputStream::Terminal => self.stdout.push(chunk),
            OutputStream::Stderr => self.stderr.push(chunk),
        }
    }
}

/// A stable head plus a rolling tail. The gap, when there is one, sits
/// between them.
struct Retained {
    head_limit: usize,
    tail_limit: usize,
    head:       Vec<u8>,
    tail:       VecDeque<u8>,
    omitted:    u64,
}

impl Retained {
    fn new(limit: usize) -> Self {
        let head_limit = limit / 2;
        Self {
            head_limit,
            tail_limit: limit - head_limit,
            head: Vec::new(),
            tail: VecDeque::new(),
            omitted: 0,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        let (head, rest) = chunk.split_at(chunk.len().min(self.head_limit - self.head.len()));
        self.head.extend_from_slice(head);
        // Whatever the tail cannot hold once `rest` is appended falls out of
        // its front, or never enters it when `rest` alone overflows it.
        let overflow = (self.tail.len() + rest.len()).saturating_sub(self.tail_limit);
        let evicted = overflow.min(self.tail.len());
        self.tail.drain(..evicted);
        self.tail.extend(&rest[overflow - evicted..]);
        self.omitted = self.omitted.saturating_add(overflow as u64);
    }

    fn take(&mut self) -> CapturedOutput {
        let mut bytes = mem::take(&mut self.head);
        bytes.extend(self.tail.drain(..));
        CapturedOutput {
            bytes,
            omitted_bytes: mem::take(&mut self.omitted),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retain(limit: usize, chunks: &[&[u8]]) -> CapturedOutput {
        let mut retained = Retained::new(limit);
        for chunk in chunks {
            retained.push(chunk);
        }
        retained.take()
    }

    #[test]
    fn output_within_the_limit_is_kept_whole() {
        let output = retain(5, &[b"01", b"234"]);
        assert_eq!(output.bytes, b"01234");
        assert_eq!(output.omitted_bytes, 0);
    }

    #[test]
    fn the_gap_falls_between_the_head_and_the_newest_bytes() {
        // An odd limit gives the tail the extra byte: head 2, tail 3.
        for chunks in [
            &[b"0123456789".as_slice()][..],
            &[b"0", b"1234", b"5", b"6789"],
            &[b"012345", b"67", b"8", b"9"],
        ] {
            let output = retain(5, chunks);
            assert_eq!(output.bytes, b"01789", "{chunks:?}");
            assert_eq!(output.omitted_bytes, 5, "{chunks:?}");
            assert_eq!(output.observed_bytes(), 10, "{chunks:?}");
        }
    }

    #[test]
    fn a_zero_limit_counts_output_without_keeping_it() {
        let output = retain(0, &[b"hello ", b"world"]);
        assert!(output.bytes.is_empty());
        assert_eq!(output.omitted_bytes, 11);
    }

    #[test]
    fn a_limit_of_one_keeps_the_newest_byte() {
        let output = retain(1, &[b"ab", b"c"]);
        assert_eq!(output.bytes, b"c");
        assert_eq!(output.omitted_bytes, 2);
    }
}
