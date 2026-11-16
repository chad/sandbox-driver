mod exec;
mod files;
mod spec;

use anyhow::{Context as _, Result, bail};
use sandbox_driver::{
    Expose, LabelFilter, ProviderEntry, ProviderName, Sandbox, SandboxId, SandboxRef, Sandboxes,
    parse_duration_ms,
};
use tokio::signal::ctrl_c;

use crate::cli::{Command, CreateArgs, OutputFormat, ProviderCommand};
use crate::output::{
    display_ref, write_action, write_descriptor, write_environments, write_exposure, write_found,
    write_health, write_line, write_providers, write_status, write_stderr,
};

/// Runs one command against an open catalog. `providers` is the entry
/// configuration, which the catalog itself does not hand back.
pub(crate) async fn execute(
    command: &Command,
    catalog: &Sandboxes,
    providers: &[(ProviderName, ProviderEntry)],
    output: OutputFormat,
) -> Result<u8> {
    match command {
        Command::Providers => {
            write_providers(providers, output).await?;
            Ok(0)
        }
        Command::Provider { command } => provider(catalog, command, output).await,
        Command::Environments => {
            let mut environments = Vec::new();
            for name in catalog.environments().await? {
                let environment = catalog.environment(name.as_str()).await?;
                environments.push((name, environment));
            }
            write_environments(&environments, output).await?;
            Ok(0)
        }
        Command::Create(args) => create(catalog, args, output).await,
        Command::Run(args) => {
            require_raw_output(output, "run")?;
            exec::run(catalog, args).await
        }
        Command::Find(args) => {
            let filter = LabelFilter(spec::parse_assignments(&args.labels, "label")?);
            write_found(&catalog.find(filter).await?, output).await?;
            Ok(0)
        }
        Command::Status(args) => {
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            let status = attach(catalog, &reference).await?.status().await?;
            write_status(&status, output).await?;
            Ok(0)
        }
        Command::Start(args) => {
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            attach(catalog, &reference).await?.start().await?;
            write_action(&reference, "started", output).await?;
            Ok(0)
        }
        Command::Stop(args) => {
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            attach(catalog, &reference).await?.stop().await?;
            write_action(&reference, "stopped", output).await?;
            Ok(0)
        }
        Command::Exec(args) => {
            require_raw_output(output, "exec")?;
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            let sandbox = attach(catalog, &reference).await?;
            exec::execute(sandbox.as_ref(), &args.options, &args.command).await
        }
        Command::Shell(args) => {
            require_raw_output(output, "shell")?;
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            exec::shell(attach(catalog, &reference).await?.as_ref()).await
        }
        Command::Cp(args) => files::cp(catalog, args, output).await,
        Command::Ls(args) => files::ls(catalog, args, output).await,
        Command::Expose(args) => {
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            let sandbox = attach(catalog, &reference).await?;
            let ttl_ms = args
                .ttl
                .as_deref()
                .map(|ttl| {
                    parse_duration_ms(ttl)
                        .ok()
                        .context("--ttl needs a duration with a unit, such as 1h")
                })
                .transpose()?;
            let exposure = sandbox
                .ports()
                .expose(args.port, Expose {
                    signed: args.signed,
                    ttl_ms,
                })
                .await?;
            write_exposure(&exposure, output).await?;
            // Some providers serve the exposure from this process, so it
            // lasts as long as the command does.
            write_stderr(b"exposed; press Ctrl-C to release\n").await?;
            ctrl_c().await.context("listening for Ctrl-C")?;
            sandbox.ports().release(args.port).await?;
            Ok(0)
        }
        Command::Destroy(args) => {
            let reference = resolve_ref(catalog, &args.sandbox).await?;
            catalog
                .destroy(&reference)
                .await
                .with_context(|| format!("destroying sandbox {}", display_ref(&reference)))?;
            write_action(&reference, "destroyed", output).await?;
            Ok(0)
        }
    }
}

async fn provider(
    catalog: &Sandboxes,
    command: &ProviderCommand,
    output: OutputFormat,
) -> Result<u8> {
    match command {
        ProviderCommand::Describe(args) => {
            let descriptor = catalog.provider(&args.name)?.describe().await?;
            write_descriptor(&args.name, &descriptor, output).await?;
            Ok(0)
        }
        ProviderCommand::Health(args) => {
            let health = catalog.provider(&args.name)?.health().await?;
            write_health(&args.name, &health, output).await?;
            Ok(u8::from(!health.healthy))
        }
    }
}

async fn create(catalog: &Sandboxes, args: &CreateArgs, output: OutputFormat) -> Result<u8> {
    let builder = catalog.builder(&args.environment).await?;
    let builder = spec::apply(builder, args).await?;
    let entry = catalog.environment(&args.environment).await?.provider;
    require_outlives_command(catalog, entry.as_str(), "create").await?;
    let sandbox = builder
        .create()
        .await
        .with_context(|| format!("creating a sandbox from environment {}", args.environment))?;
    let reference = sandbox.reference();
    match output {
        OutputFormat::Json => write_status(&sandbox.status().await?, output).await?,
        OutputFormat::Table | OutputFormat::Id => write_line(&display_ref(&reference)).await?,
    }
    Ok(0)
}

/// Refuses an operation whose sandbox would be gone when this command
/// exits: a provider of `process` durability ends its sandboxes with the
/// process that made them.
async fn require_outlives_command(catalog: &Sandboxes, entry: &str, what: &str) -> Result<()> {
    let descriptor = catalog.provider(entry)?.describe().await?;
    if !exec::outlives_command(descriptor.durability) {
        bail!(
            "sandboxes of provider {entry} end when this command exits, so {what} would leave \
             nothing behind; use `lithos-sandbox run` without --keep"
        );
    }
    Ok(())
}

/// Reads a sandbox ref: `<entry>/<id>` as `create` prints it, or the JSON
/// reference from `--output json`. The short form takes its namespace from
/// the entry's provider now; the JSON form is checked against it.
pub(super) async fn resolve_ref(catalog: &Sandboxes, text: &str) -> Result<SandboxRef> {
    if text.trim_start().starts_with('{') {
        return serde_json::from_str(text).context("parsing the sandbox reference as JSON");
    }
    let (entry, id) = text
        .split_once('/')
        .filter(|(entry, id)| !entry.is_empty() && !id.is_empty())
        .with_context(|| {
            format!("{text:?} is not a sandbox ref; expected <provider>/<id>, as `create` prints")
        })?;
    let provider = ProviderName::try_new(entry).context("reading the sandbox ref")?;
    let id = SandboxId::try_new(id).context("reading the sandbox ref")?;
    let namespace = catalog.provider(entry)?.describe().await?.namespace.id;
    Ok(SandboxRef {
        provider,
        namespace,
        id,
    })
}

pub(super) async fn attach(
    catalog: &Sandboxes,
    reference: &SandboxRef,
) -> Result<Box<dyn Sandbox>> {
    catalog
        .attach(reference)
        .await
        .with_context(|| format!("attaching sandbox {}", display_ref(reference)))
}

fn require_raw_output(output: OutputFormat, command: &str) -> Result<()> {
    if output != OutputFormat::Table {
        bail!("{command} streams the command's own output and requires --output table");
    }
    Ok(())
}
