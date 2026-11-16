use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::Path;

use anyhow::{Context as _, Result, bail};
use sandbox_driver::SnapshotId;
use sandbox_driver::contract::{SandboxBuilder, SandboxSpec, Source, parse_size_mib};
use serde_json::Value;
use tokio::fs::{read, read_to_string};
use tokio::io::{AsyncReadExt as _, stdin};

use crate::cli::CreateArgs;

/// Applies the command-line changes to the environment's template. Options
/// that were not given leave the template's value in place.
pub(super) async fn apply(builder: SandboxBuilder, args: &CreateArgs) -> Result<SandboxBuilder> {
    let mut spec = match &args.spec {
        Some(path) => read_spec(path).await?,
        None => builder.resolved_spec().clone(),
    };
    if let Some(image) = &args.image {
        spec.source = Source::Image(image.clone());
    }
    if let Some(path) = &args.dockerfile {
        let content = read_to_string(path)
            .await
            .with_context(|| format!("reading Dockerfile {}", path.display()))?;
        spec.source = Source::Dockerfile(content);
    }
    if let Some(snapshot) = &args.snapshot {
        spec.source = Source::Snapshot(
            SnapshotId::try_new(snapshot.clone()).context("validating --snapshot")?,
        );
    }
    if let Some(name) = &args.name {
        spec.name = Some(name.clone());
    }
    if let Some(cpu) = args.cpu {
        spec.resources.cpu_cores = Some(cpu);
    }
    if let Some(memory) = &args.memory {
        spec.resources.memory_mb = Some(size("--memory", memory)?);
    }
    if let Some(disk) = &args.disk {
        spec.resources.disk_mb = Some(size("--disk", disk)?);
    }
    spec.env.extend(parse_assignments(&args.env, "env")?);
    spec.labels
        .extend(parse_assignments(&args.labels, "label")?);
    if let Some(workspace) = &args.workspace {
        spec.working_directory = Some(workspace.clone());
    }
    if let Some(user) = &args.user {
        spec.user = Some(user.clone());
    }
    if let Some(network) = args.network {
        spec.network = network.into();
    }
    if let Some(config) = &args.provider_config {
        let values: BTreeMap<String, Value> =
            serde_json::from_str(config).context("parsing --provider-config as a JSON object")?;
        spec.provider_config.extend(values);
    }
    Ok(builder.spec(spec))
}

async fn read_spec(path: &Path) -> Result<SandboxSpec> {
    let (contents, source) = if path == Path::new("-") {
        let mut contents = Vec::new();
        stdin()
            .read_to_end(&mut contents)
            .await
            .context("reading stdin")?;
        (contents, "stdin".to_owned())
    } else {
        let contents = read(path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        (contents, path.display().to_string())
    };
    serde_json::from_slice(&contents).with_context(|| format!("parsing SandboxSpec from {source}"))
}

fn size(option: &str, value: &str) -> Result<NonZeroU64> {
    parse_size_mib(value)
        .ok()
        .and_then(NonZeroU64::new)
        .with_context(|| format!("{option} needs a positive size with a unit, such as 4GB"))
}

pub(super) fn parse_assignments(
    values: &[String],
    option: &str,
) -> Result<BTreeMap<String, String>> {
    let mut assignments = BTreeMap::new();
    for value in values {
        let (key, value) = value
            .split_once('=')
            .with_context(|| format!("--{option} must use KEY=VALUE syntax"))?;
        if key.is_empty() {
            bail!("--{option} key must not be empty");
        }
        assignments.insert(key.to_owned(), value.to_owned());
    }
    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignments_split_only_on_the_first_equals_sign() {
        let values = vec!["TOKEN=abc=123".to_owned()];
        let assignments = parse_assignments(&values, "env").expect("assignment parses");
        assert_eq!(assignments["TOKEN"], "abc=123");
    }

    #[test]
    fn assignments_require_a_nonempty_key() {
        let missing_equals =
            parse_assignments(&["TOKEN".to_owned()], "env").expect_err("missing equals fails");
        assert!(missing_equals.to_string().contains("KEY=VALUE"));

        let empty_key =
            parse_assignments(&["=value".to_owned()], "env").expect_err("empty key fails");
        assert!(empty_key.to_string().contains("key must not be empty"));
    }

    #[test]
    fn sizes_need_a_unit_and_read_gigabytes_as_gibibytes() {
        assert_eq!(size("--memory", "4GB").unwrap().get(), 4096);
        assert!(size("--memory", "0GB").is_err());
    }
}
