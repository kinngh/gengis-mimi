use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use gengis_mimi::{Engine, Error, config::Config};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "gengis-mimi",
    version,
    about = "Gengis Mimi — object storage, documents, and vector search"
)]
struct Args {
    /// TOML configuration. Without it, use ./data and 127.0.0.1:7878.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("gengis_mimi=info,slatedb=warn")),
        )
        .init();
    let args = Args::parse();
    let config = Config::load(args.config.as_deref())?;
    let api_token = match std::env::var("GENGIS_MIMI_API_TOKEN") {
        Ok(token) if token.is_empty() || token.bytes().any(|c| !c.is_ascii_graphic()) => {
            return Err(Error::Config(
                "GENGIS_MIMI_API_TOKEN must be nonempty printable ASCII without spaces".into(),
            )
            .into());
        }
        Ok(token) => Some(token),
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    // Bind before opening SlateDB: a port conflict must not fence a healthy writer.
    let listener = tokio::net::TcpListener::bind(config.server.bind).await?;
    let address = listener.local_addr()?;
    let engine = Arc::new(Engine::open(&config).await?);
    tracing::info!(%address, "Gengis Mimi is ready");
    let result = axum::serve(
        listener,
        gengis_mimi::api::router(engine.clone(), api_token),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await;
    let close = engine.close().await;
    result?;
    close?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("draining requests and closing the database");
}
