# Diagnostics

The library crates emit `tracing` spans and events. Applications choose the
subscriber and output destination. Operation fields include provider kinds,
resource IDs, states, attempts, counts, and durations. They do not include
commands, environment values, tokens, URLs, file paths, request bodies, or
command output.

The Host, Docker, and Daytona provider executables, which are being removed,
configure a stderr subscriber. They use `RUST_LOG` when set and default to
`info`. Protocol messages remain on stdout. The CLI supplies `warn` when its
own `RUST_LOG` is unset; see the
[CLI guide](https://github.com/lithoscomputer/sandbox-driver/blob/main/docs/cli.md#provider-executables).

For error handling and uncertain operation outcomes, see the
[design](https://github.com/lithoscomputer/sandbox-driver/blob/main/docs/design.md#lifecycle-fences-and-uncertain-outcomes).
