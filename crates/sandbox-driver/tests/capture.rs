#![cfg(unix)]

use std::os::unix::fs::symlink;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, process};

use sandbox_driver::{
    DEFAULT_CAPTURE_BYTES, Error, ExecControls, ExecSpec, MutationOutcome, OutputSink,
    OutputStream, Sandbox, Sandboxes, Termination, run_captured, run_captured_with, run_shell,
};
use sandbox_driver_testing::{FakeExec, FakeProvider};
use tokio_util::sync::CancellationToken;

/// A sandbox on a fake that answers `argv` with `response`.
async fn scripted(argv: Vec<String>, response: FakeExec) -> (Sandboxes, Box<dyn Sandbox>) {
    let fake = FakeProvider::new();
    fake.script(argv, response).await;
    let catalog = fake.sandboxes();
    let sandbox = catalog
        .builder("default")
        .await
        .expect("builder")
        .create()
        .await
        .expect("create");
    (catalog, sandbox)
}

fn build() -> Vec<String> {
    vec!["build".to_owned()]
}

/// Returns the future unchanged; compiling proves a spawned task or an
/// `async_trait` method can await it.
fn spawnable<F: Future + Send>(future: F) -> F {
    future
}

#[tokio::test]
async fn a_failing_command_is_a_result_with_both_streams_and_its_exit_code() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"compiled 3 files\n".to_vec(),
        stderr: b"error: missing semicolon\n".to_vec(),
        exit_code: 2,
        ..FakeExec::default()
    })
    .await;
    let captured = spawnable(run_captured(sandbox.as_ref(), ExecSpec::argv(build())))
        .await
        .expect("a failing command still completes");
    assert_eq!(captured.outcome.termination, Termination::Exited);
    assert_eq!(captured.outcome.exit_code, Some(2));
    assert_eq!(captured.stdout.bytes, b"compiled 3 files\n");
    assert_eq!(captured.stderr.bytes, b"error: missing semicolon\n");
    assert!(!captured.success());
    assert!(captured.is_complete());
}

#[tokio::test]
async fn output_beyond_the_limit_is_counted_not_kept_and_the_command_still_finishes() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"0123456789".to_vec(),
        stderr: b"ok".to_vec(),
        ..FakeExec::default()
    })
    .await;
    let captured = spawnable(run_captured_with(
        sandbox.as_ref(),
        ExecSpec::argv(build()),
        ExecControls::default(),
        4,
    ))
    .await
    .expect("run");
    assert_eq!(captured.stdout.bytes, b"0189");
    assert_eq!(captured.stdout.omitted_bytes, 6);
    assert_eq!(captured.stdout.observed_bytes(), 10);
    // The limit is per stream: stderr fits and is whole.
    assert_eq!(captured.stderr.bytes, b"ok");
    assert_eq!(captured.stderr.omitted_bytes, 0);
    assert!(captured.success());
    assert!(!captured.is_complete());
    // Output can carry credentials, so `Debug` reports sizes only.
    let debug = format!("{captured:?}");
    assert!(
        !debug.contains(&format!("{:?}", captured.stdout.bytes)),
        "{debug}"
    );
}

#[tokio::test]
async fn the_callers_sink_sees_every_chunk_even_when_nothing_is_kept() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"out".to_vec(),
        stderr: b"err".to_vec(),
        ..FakeExec::default()
    })
    .await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink: OutputSink = {
        let seen = Arc::clone(&seen);
        Arc::new(move |stream, bytes| {
            seen.lock().expect("seen").push((stream, bytes));
            Box::pin(async { Ok(()) })
        })
    };
    let captured = run_captured_with(
        sandbox.as_ref(),
        ExecSpec::argv(build()),
        ExecControls {
            sink: Some(sink),
            ..ExecControls::default()
        },
        0,
    )
    .await
    .expect("run");
    assert_eq!(*seen.lock().expect("seen"), [
        (OutputStream::Stdout, b"out".to_vec()),
        (OutputStream::Stderr, b"err".to_vec()),
    ]);
    assert!(captured.stdout.bytes.is_empty());
    assert_eq!(captured.stdout.omitted_bytes, 3);
    assert_eq!(captured.stderr.omitted_bytes, 3);
}

#[tokio::test]
async fn a_failing_caller_sink_fails_the_run() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"out".to_vec(),
        ..FakeExec::default()
    })
    .await;
    let sink: OutputSink = Arc::new(|_, _| {
        Box::pin(async {
            Err(Error::Transport {
                message: "consumer went away".into(),
                outcome: MutationOutcome::Unknown,
            })
        })
    });
    let result = run_captured_with(
        sandbox.as_ref(),
        ExecSpec::argv(build()),
        ExecControls {
            sink: Some(sink),
            ..ExecControls::default()
        },
        DEFAULT_CAPTURE_BYTES,
    )
    .await;
    assert!(
        matches!(&result, Err(Error::Transport { message, .. }) if message == "consumer went away"),
        "{result:?}"
    );
}

#[tokio::test]
async fn the_callers_stop_token_reaches_the_command_and_its_output_is_kept() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"started\n".to_vec(),
        wait_for_stop: true,
        ..FakeExec::default()
    })
    .await;
    let term = CancellationToken::new();
    let sink: OutputSink = {
        let term = term.clone();
        Arc::new(move |_, _| {
            term.cancel();
            Box::pin(async { Ok(()) })
        })
    };
    let captured = run_captured_with(
        sandbox.as_ref(),
        ExecSpec::argv(build()),
        ExecControls {
            sink: Some(sink),
            term: Some(term),
            ..ExecControls::default()
        },
        DEFAULT_CAPTURE_BYTES,
    )
    .await
    .expect("a stopped command still completes");
    assert_eq!(captured.outcome.termination, Termination::Terminated);
    assert_eq!(captured.stdout.bytes, b"started\n");
    assert!(!captured.success());
}

#[tokio::test]
async fn terminal_output_is_captured_as_stdout() {
    let (_catalog, sandbox) = scripted(build(), FakeExec {
        stdout: b"prompt$ ".to_vec(),
        stderr: b"warning".to_vec(),
        ..FakeExec::default()
    })
    .await;
    let mut spec = ExecSpec::argv(build());
    spec.tty = true;
    let captured = run_captured(sandbox.as_ref(), spec).await.expect("run");
    assert_eq!(captured.stdout.bytes, b"prompt$ warning");
    assert!(captured.stderr.bytes.is_empty());
}

#[tokio::test]
async fn run_shell_runs_the_script_through_the_shell_spec() {
    let script = "mkdir -p \"$1\" && echo made";
    let spec = ExecSpec::shell(script);
    assert_eq!(spec.argv.first().map(String::as_str), Some("sh"));
    assert_eq!(spec.argv.last().map(String::as_str), Some(script));
    spec.validate().expect("a shell spec is a valid exec");
    let (_catalog, sandbox) = scripted(spec.argv, FakeExec {
        stdout: b"made\n".to_vec(),
        ..FakeExec::default()
    })
    .await;
    let captured = spawnable(run_shell(sandbox.as_ref(), script))
        .await
        .expect("run");
    assert_eq!(captured.stdout.bytes, b"made\n");
    assert!(captured.success());
}

/// Runs a spec's argv the way a provider does, with no shell in between, in
/// an environment of exactly `env`.
fn run_argv(spec: &ExecSpec, env: &[(&str, &str)]) -> Output {
    let (program, args) = spec.argv.split_first().expect("argv");
    Command::new(program)
        .args(args)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .expect("the test machine provides sh")
}

#[test]
fn a_shell_spec_prefers_bash_falls_back_to_sh_and_reads_no_startup_file() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let scratch = env::temp_dir().join(format!("shell-spec-{}-{nanos}", process::id()));
    fs::create_dir(&scratch).expect("scratch directory");
    // Bash sources the file `BASH_ENV` names before a `-c` script.
    let startup = scratch.join("startup.sh");
    fs::write(&startup, "echo sourced\n").expect("startup file");
    let startup = startup.to_string_lossy();
    let path = env::var("PATH").expect("PATH");

    // `$0` is the name the script's own shell was started under.
    let spec = ExecSpec::shell("printf '%s' \"$0\"; exit 7");
    let with_bash = run_argv(&spec, &[("PATH", &path), ("BASH_ENV", &startup)]);
    assert_eq!(String::from_utf8_lossy(&with_bash.stdout), "bash");
    assert_eq!(with_bash.status.code(), Some(7));

    // A `PATH` that holds `sh` and nothing else: a workload without Bash.
    let only_sh = scratch.join("bin");
    fs::create_dir(&only_sh).expect("bin directory");
    symlink("/bin/sh", only_sh.join("sh")).expect("sh");
    let only_sh = only_sh.to_string_lossy();
    let without_bash = run_argv(&spec, &[("PATH", &only_sh), ("BASH_ENV", &startup)]);
    assert_eq!(String::from_utf8_lossy(&without_bash.stdout), "sh");
    assert_eq!(without_bash.status.code(), Some(7));

    fs::remove_dir_all(&scratch).expect("cleanup");
}
