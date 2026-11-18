//! Generates the boxd public API from the vendored proto with a pure-Rust
//! protobuf compiler, so building needs no system `protoc`.
//!
//! The client and its messages land in `client/`. The server trait, which
//! only the crate's in-process fake uses, lands in `server/`, reuses the
//! client's message types, and covers only the calls the provider makes;
//! the fake answers any other call with `UNIMPLEMENTED`.

use std::error::Error;
use std::path::PathBuf;
use std::{env, fs};

/// The `BoxdApi` methods the provider calls.
const USED: &[&str] = &[
    "CreateProxy",
    "CreateVm",
    "CreateVmFromSnapshot",
    "DeleteProxy",
    "DestroyVm",
    "Exec",
    "FsList",
    "FsMkdir",
    "FsRead",
    "FsRealpath",
    "FsWrite",
    "GetVm",
    "ListOrgs",
    "ListProxies",
    "ListRegions",
    "ListVms",
    "ResumeVm",
    "SetVmEgressAllow",
    "StartVm",
    "StopVm",
    "UpdateVmLabels",
    "WakeVm",
    "Whoami",
];

fn main() -> Result<(), Box<dyn Error>> {
    let proto = "proto/boxd/api/v1/api.proto";
    println!("cargo:rerun-if-changed={proto}");
    let out = PathBuf::from(env::var("OUT_DIR")?);
    let descriptors = protox::compile([proto], ["proto"])?;

    let client = out.join("client");
    fs::create_dir_all(&client)?;
    tonic_prost_build::configure()
        .out_dir(&client)
        .build_client(true)
        .build_server(false)
        .compile_fds(descriptors.clone())?;

    let mut used = descriptors;
    for file in &mut used.file {
        for service in &mut file.service {
            service
                .method
                .retain(|method| USED.contains(&method.name()));
        }
    }
    let server = out.join("server");
    fs::create_dir_all(&server)?;
    tonic_prost_build::configure()
        .out_dir(&server)
        .build_client(false)
        .build_server(true)
        .extern_path(".boxd.api.v1", "crate::api")
        .compile_fds(used)?;
    Ok(())
}
