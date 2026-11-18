//! Commands over boxd's bidirectional `Exec` stream.
//!
//! boxd runs the command string with `/bin/sh -c`, so argv is quoted into
//! one string that changes directory and execs the program under `env`. No
//! shell parses the caller's arguments. Cancelling the call makes boxd's
//! in-machine agent signal the command's process group (SIGTERM, then
//! SIGKILL), which is how term, kill, a timeout and a dropped future end a
//! command.

use std::collections::BTreeMap;
use std::time::Duration;
use std::{future, mem};

use sandbox_driver::{
    ByteStream, Error, ExecOutcome, MutationOutcome, OutputLoss, OutputSink, OutputStream,
    Termination, stop_signal,
};
use tokio::io::AsyncReadExt as _;
use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::api::ExecChunk;
use crate::client::{Client, classify};

/// Bytes of stdin sent per message.
const STDIN_CHUNK: usize = 64 * 1024;

/// One command, ready to send.
pub(crate) struct Command {
    pub(crate) machine: String,
    pub(crate) line:    String,
    pub(crate) timeout: Option<Duration>,
}

/// Why a stream ended before boxd reported an exit code.
pub(crate) enum Broken {
    /// boxd reported the failure; its status says why.
    Status(tonic::Status),
    /// The stream closed without an exit code.
    Truncated,
}

pub(crate) struct Controls {
    pub(crate) stdin:  Option<ByteStream>,
    pub(crate) sink:   Option<OutputSink>,
    pub(crate) term:   Option<CancellationToken>,
    pub(crate) kill:   Option<CancellationToken>,
    pub(crate) closed: CancellationToken,
}

/// The shell line that runs `argv` in `directory` with `env` added.
pub(crate) fn line(directory: &str, env: &BTreeMap<String, String>, argv: &[String]) -> String {
    let mut line = format!("cd {} && exec env", quote(directory));
    for (key, value) in env {
        line.push(' ');
        line.push_str(&quote(&format!("{key}={value}")));
    }
    // `env` reads leading NAME=VALUE words as assignments; `--` would not
    // stop that, so a program name containing `=` goes through `sh -c`.
    if argv.first().is_some_and(|program| program.contains('=')) {
        line.push_str(" sh -c 'exec \"$@\"' sh");
    }
    for arg in argv {
        line.push(' ');
        line.push_str(&quote(arg));
    }
    line
}

/// POSIX single-quoting: every byte stands for itself.
pub(crate) fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', r"'\''"))
}

fn outcome(termination: Termination, exit_code: Option<i32>) -> ExecOutcome {
    ExecOutcome {
        termination,
        exit_code,
        signal: None,
        output_loss: OutputLoss::default(),
    }
}

/// Runs `command` and returns its outcome, or why its stream broke. Nothing
/// here decides what a broken stream means; the caller asks boxd.
pub(crate) async fn run(
    client: &Client,
    command: Command,
    controls: Controls,
) -> Result<Result<ExecOutcome, Broken>, Error> {
    let Controls {
        stdin,
        sink,
        term,
        kill,
        closed,
    } = controls;
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ExecChunk {
            vm_id: command.machine,
            command: command.line,
            ..ExecChunk::default()
        })
        .await
        .expect("the receiver is held below");
    let request = client.request(ReceiverStream::new(receiver)).await?;
    let mut api = client.api();
    let expired = async {
        match command.timeout {
            Some(timeout) => time::sleep(timeout).await,
            None => future::pending().await,
        }
    };
    tokio::pin!(expired);
    // Either an end to stop at, or the call's outcome. Dropping the call
    // future cancels the RPC, which boxd turns into signals.
    let stopped = |termination| Ok(Ok(outcome(termination, None)));

    let call = api.exec(request);
    tokio::pin!(call);
    let mut stdin = Stdin::new(stdin, sender);
    let response = loop {
        tokio::select! {
            biased;
            () = closed.cancelled() => return Err(Error::Stale { outcome: MutationOutcome::Unknown }),
            () = stop_signal(kill.as_ref()) => return stopped(Termination::Killed),
            () = stop_signal(term.as_ref()) => return stopped(Termination::Terminated),
            () = &mut expired => return stopped(Termination::TimedOut),
            result = stdin.pump(), if stdin.pending() => result?,
            response = &mut call => break response,
        }
    };
    let mut inbound = match response {
        Ok(response) => response.into_inner(),
        Err(status) => return Ok(Err(Broken::Status(status))),
    };
    let mut exit_code = None;
    loop {
        let message = tokio::select! {
            biased;
            () = closed.cancelled() => return Err(Error::Stale { outcome: MutationOutcome::Unknown }),
            () = stop_signal(kill.as_ref()) => return stopped(Termination::Killed),
            () = stop_signal(term.as_ref()) => return stopped(Termination::Terminated),
            () = &mut expired => return stopped(Termination::TimedOut),
            result = stdin.pump(), if stdin.pending() => {
                result?;
                continue;
            }
            message = inbound.message() => message,
        };
        match message {
            Ok(Some(chunk)) => {
                if chunk.data.is_empty() {
                    exit_code = Some(chunk.exit_code);
                } else {
                    if let Some(sink) = &sink {
                        let stream = if chunk.is_stderr {
                            OutputStream::Stderr
                        } else {
                            OutputStream::Stdout
                        };
                        // A failed sink ends the command: dropping the call
                        // on return cancels it.
                        sink(stream, chunk.data).await?;
                    }
                    exit_code = None;
                }
            }
            Ok(None) => {
                return Ok(match exit_code {
                    Some(code) => Ok(outcome(Termination::Exited, Some(code))),
                    None => Err(Broken::Truncated),
                });
            }
            Err(status) => return Ok(Err(Broken::Status(status))),
        }
    }
}

/// Streams the caller's stdin into the call, then half-closes it, which
/// boxd delivers as end of input.
struct Stdin {
    input:  Option<ByteStream>,
    sender: Option<mpsc::Sender<ExecChunk>>,
}

impl Stdin {
    fn new(input: Option<ByteStream>, sender: mpsc::Sender<ExecChunk>) -> Self {
        // Without input the send half closes at once: the command reads EOF.
        let sender = input.is_some().then_some(sender);
        Self { input, sender }
    }

    fn pending(&self) -> bool {
        self.sender.is_some()
    }

    /// Sends one chunk, or closes the input at its end. A read failure
    /// fails the command rather than delivering a truncated input as EOF.
    async fn pump(&mut self) -> Result<(), Error> {
        let (Some(input), Some(sender)) = (self.input.as_mut(), self.sender.as_ref()) else {
            return future::pending().await;
        };
        let mut buffer = vec![0; STDIN_CHUNK];
        let read = input
            .read(&mut buffer)
            .await
            .map_err(|_| Error::Transport {
                message: "reading stdin failed".to_owned(),
                outcome: MutationOutcome::Unknown,
            })?;
        if read == 0 {
            self.sender = None;
            self.input = None;
            return Ok(());
        }
        buffer.truncate(read);
        let chunk = ExecChunk {
            stdin: true,
            data: mem::take(&mut buffer),
            ..ExecChunk::default()
        };
        if sender.send(chunk).await.is_err() {
            // The call has ended; its outcome arrives on the response.
            self.sender = None;
        }
        Ok(())
    }
}

/// A failure boxd reported for the command itself, before or while it ran.
pub(crate) fn rejected(status: &tonic::Status) -> Option<Error> {
    use tonic::Code;
    match status.code() {
        Code::InvalidArgument
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::ResourceExhausted
        | Code::FailedPrecondition => Some(classify("running command", status)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_is_quoted_so_no_shell_reinterprets_it() {
        let env = BTreeMap::from([("A".to_owned(), "it's $HOME".to_owned())]);
        let argv = [
            "printf".to_owned(),
            "%s\n".to_owned(),
            "a b; rm -rf /".to_owned(),
        ];
        assert_eq!(
            line("/work dir", &env, &argv),
            r"cd '/work dir' && exec env 'A=it'\''s $HOME' 'printf' '%s
' 'a b; rm -rf /'"
        );
    }

    #[test]
    fn a_program_name_with_an_equals_sign_is_not_read_as_an_assignment() {
        let argv = ["./a=b".to_owned()];
        assert!(line("/", &BTreeMap::new(), &argv).ends_with(r#"sh -c 'exec "$@"' sh './a=b'"#));
    }
}
