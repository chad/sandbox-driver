use std::path::Path;

use anyhow::{Context as _, Result, bail};
use sandbox_driver::contract::{SandboxRef, Sandboxes};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt as _, copy, stdin, stdout};

use super::{attach, resolve_ref};
use crate::cli::{CpArgs, LsArgs, OutputFormat};
use crate::output::{display_ref, write_action, write_files};

/// One side of `cp`: a local path, `-` for stdin or stdout, or
/// `SANDBOX:PATH`.
enum Location<'a> {
    Local(&'a str),
    Remote { sandbox: &'a str, path: &'a str },
}

/// `SANDBOX:PATH` is recognised by a sandbox ref, `<entry>/<id>`, before the
/// first colon. Prefix a local path that looks like one with `./`.
fn location(text: &str) -> Location<'_> {
    match text.split_once(':') {
        Some((sandbox, path))
            if sandbox.contains('/')
                && !sandbox.starts_with(['/', '.', '~'])
                && !path.is_empty() =>
        {
            Location::Remote { sandbox, path }
        }
        _ => Location::Local(text),
    }
}

pub(super) async fn cp(catalog: &Sandboxes, args: &CpArgs, output: OutputFormat) -> Result<u8> {
    match (location(&args.source), location(&args.destination)) {
        (Location::Local(local), Location::Remote { sandbox, path }) => {
            let reference = resolve_ref(catalog, sandbox).await?;
            let handle = attach(catalog, &reference).await?;
            let bytes = if local == "-" {
                Box::pin(stdin()) as _
            } else {
                Box::pin(
                    File::open(local)
                        .await
                        .with_context(|| format!("opening {local}"))?,
                ) as _
            };
            handle
                .files()
                .put(path, bytes)
                .await
                .with_context(|| format!("writing {path} in {sandbox}"))?;
            report(&reference, "copied to", output).await
        }
        (Location::Remote { sandbox, path }, Location::Local(local)) => {
            let reference = resolve_ref(catalog, sandbox).await?;
            let handle = attach(catalog, &reference).await?;
            let mut bytes = handle
                .files()
                .get(path, None)
                .await
                .with_context(|| format!("reading {path} in {sandbox}"))?;
            if local == "-" {
                let mut out = stdout();
                copy(&mut bytes, &mut out).await.context("writing stdout")?;
                out.flush().await.context("flushing stdout")?;
                return Ok(0);
            }
            let local = if Path::new(local).is_dir() {
                let name = Path::new(path)
                    .file_name()
                    .with_context(|| format!("{path} names no file"))?;
                Path::new(local).join(name)
            } else {
                local.into()
            };
            let mut file = File::create(&local)
                .await
                .with_context(|| format!("creating {}", local.display()))?;
            copy(&mut bytes, &mut file)
                .await
                .with_context(|| format!("downloading {path} to {}", local.display()))?;
            file.flush().await.context("flushing the download")?;
            report(&reference, "copied from", output).await
        }
        (Location::Remote { .. }, Location::Remote { .. }) => {
            bail!("copy between two sandboxes through a local file")
        }
        (Location::Local(_), Location::Local(_)) => {
            bail!("one side of cp must be SANDBOX:PATH, such as docker/1a2b:/tmp/file")
        }
    }
}

pub(super) async fn ls(catalog: &Sandboxes, args: &LsArgs, output: OutputFormat) -> Result<u8> {
    let Location::Remote { sandbox, path } = location(&args.path) else {
        bail!("ls takes SANDBOX:PATH, such as docker/1a2b:/tmp");
    };
    let reference = resolve_ref(catalog, sandbox).await?;
    let entries = attach(catalog, &reference)
        .await?
        .files()
        .list(path, args.depth)
        .await
        .with_context(|| format!("listing {path} in {}", display_ref(&reference)))?;
    write_files(&entries, output).await?;
    Ok(0)
}

async fn report(reference: &SandboxRef, action: &str, output: OutputFormat) -> Result<u8> {
    // Plain output stays quiet so `cp` composes in scripts.
    if output != OutputFormat::Table {
        write_action(reference, action, output).await?;
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote(text: &str) -> Option<(&str, &str)> {
        match location(text) {
            Location::Remote { sandbox, path } => Some((sandbox, path)),
            Location::Local(_) => None,
        }
    }

    #[test]
    fn a_ref_before_the_colon_marks_the_sandbox_side() {
        assert_eq!(
            remote("docker/1a2b:/tmp/x:y"),
            Some(("docker/1a2b", "/tmp/x:y"))
        );
        assert_eq!(remote("./out/file:1"), None);
        assert_eq!(remote("/abs/dir:1"), None);
        assert_eq!(remote("notes.txt"), None);
        assert_eq!(remote("docker/1a2b:"), None);
    }
}
