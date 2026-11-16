//! The Docker runtime as a third-party plugin of kind `test-docker`.

use std::sync::Arc;

use sandbox_driver::legacy;
use sandbox_driver_docker::DockerFactory;
use sandbox_driver_fixture_plugins::serve_renamed;

#[tokio::main]
async fn main() -> legacy::Result<()> {
    serve_renamed(
        "test-docker",
        DockerFactory::schemas(),
        Arc::new(DockerFactory),
    )
    .await
}
