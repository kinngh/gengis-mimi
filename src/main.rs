use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
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
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the configured server, gateway, or shard worker.
    Serve,
    /// Export a consistent online backup from one active worker.
    Backup {
        #[arg(long)]
        url: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Restore a verified backup into an empty, offline database prefix.
    Restore {
        #[arg(long)]
        input: PathBuf,
    },
    /// Resume the offline, crash-safe v1-to-v2 format migration.
    Migrate,
    /// Build a namespace index with the database offline.
    Reindex { namespace: String },
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
    let mut config = Config::load(args.config.as_deref())?;
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
    match args.command.unwrap_or(Command::Serve) {
        Command::Backup { url, output } => {
            gengis_mimi::backup::download(&url, &output, api_token.as_deref()).await?;
            println!("Backup saved to {}", output.display());
            return Ok(());
        }
        Command::Restore { input } => {
            if config.cluster.is_some() {
                return Err(Error::Config(
                    "restore requires a standalone configuration and stopped workers".into(),
                )
                .into());
            }
            let engine = Engine::open(&config).await?;
            let result = engine.restore(&input).await;
            engine.close().await?;
            println!("Restored {} records", result?);
            return Ok(());
        }
        Command::Migrate => {
            if config.cluster.is_some() {
                return Err(Error::Config(
                    "migration requires a standalone configuration and stopped workers".into(),
                )
                .into());
            }
            config.database.migrate = true;
            let engine = Engine::open(&config).await?;
            engine.close().await?;
            println!("Storage format is v2");
            return Ok(());
        }
        Command::Reindex { namespace } => {
            if config.cluster.is_some() {
                return Err(Error::Config(
                    "use POST /v1/namespaces/{namespace}/index on the active worker".into(),
                )
                .into());
            }
            let engine = Engine::open(&config).await?;
            let result = engine.rebuild_index(&namespace).await;
            engine.close().await?;
            println!("{}", serde_json::to_string_pretty(&result?)?);
            return Ok(());
        }
        Command::Serve => {}
    }
    // Bind before opening SlateDB: a port conflict must not fence a healthy writer.
    let listener = tokio::net::TcpListener::bind(config.server.bind).await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let mut server_stop = stop.subscribe();
    let mut engine = None;
    let mut background = None;
    let router = match &config.cluster {
        Some(gengis_mimi::config::ClusterConfig::Gateway { .. }) => {
            gengis_mimi::cluster::gateway_router(&config, api_token).await?
        }
        Some(gengis_mimi::config::ClusterConfig::Worker { .. }) => {
            if api_token.is_none() {
                return Err(
                    Error::Config("cluster workers require GENGIS_MIMI_API_TOKEN".into()).into(),
                );
            }
            let store = Arc::new(gengis_mimi::cluster::s3_store(&config)?);
            let state = Arc::new(gengis_mimi::cluster::WorkerState::default());
            let router = gengis_mimi::cluster::worker_router(state.clone());
            let worker_config = config.clone();
            let failed = stop.clone();
            background = Some(tokio::spawn(async move {
                let result = gengis_mimi::cluster::run_worker(
                    worker_config,
                    store,
                    state,
                    api_token,
                    stopped,
                )
                .await;
                if let Err(error) = &result {
                    tracing::error!(%error, "worker stopped");
                    let _ = failed.send(true);
                }
                result
            }));
            router
        }
        None => {
            let opened = Arc::new(Engine::open(&config).await?);
            let router = gengis_mimi::api::configured_router(opened.clone(), api_token)?;
            let indexing = opened.clone();
            background = Some(tokio::spawn(async move {
                indexing.run_indexer(stopped).await;
                Ok(())
            }));
            engine = Some(opened);
            router
        }
    };
    tracing::info!(%address, "Gengis Mimi is listening");
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::select! { _ = shutdown_signal() => {}, _ = server_stop.changed() => {} }
        })
        .await;
    let _ = stop.send(true);
    if let Some(task) = background {
        task.await??;
    }
    if let Some(engine) = engine {
        engine.close().await?;
    }
    result?;
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
