//! Fixed namespace shards, an HTTP gateway, and CAS-coordinated active/standby
//! workers. Leases use elapsed observation time, not synchronized wall clocks.
use crate::{
    Engine, Error, Result,
    api::Access,
    config::{ClusterConfig, Config, ShardConfig},
    model::*,
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use slatedb::object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{RwLock, Semaphore, watch};
use tower::ServiceExt;

#[derive(Serialize, Deserialize)]
struct Lease {
    node: String,
    nonce: String,
    ttl_ms: u64,
}
struct Active {
    engine: Arc<Engine>,
    router: Router,
    until: Instant,
}
#[derive(Default)]
pub struct WorkerState {
    active: RwLock<Option<Active>>,
}

pub fn worker_router(state: Arc<WorkerState>) -> Router {
    Router::new()
        .route(
            "/healthz",
            get(|| async { Json(json!({"status":"ok", "role":"worker"})) }),
        )
        .route("/readyz", get(worker_ready))
        .fallback(worker_forward)
        .with_state(state)
}
async fn worker_ready(State(state): State<Arc<WorkerState>>) -> Result<Json<Value>> {
    let state = state.active.read().await;
    let active = state
        .as_ref()
        .filter(|a| Instant::now() < a.until)
        .ok_or(Error::Unavailable)?;
    active.engine.check_ready().await?;
    Ok(Json(
        json!({"status":"ready", "database_identity":active.engine.database_identity}),
    ))
}
async fn worker_forward(
    State(state): State<Arc<WorkerState>>,
    request: Request,
) -> Result<Response> {
    let router = {
        let state = state.active.read().await;
        state
            .as_ref()
            .filter(|a| Instant::now() < a.until && a.engine.db.status().close_reason.is_none())
            .ok_or(Error::Unavailable)?
            .router
            .clone()
    };
    Ok(router
        .oneshot(request)
        .await
        .unwrap_or_else(|never| match never {}))
}

/// Standbys must observe an unchanged lease ETag for its entire TTL before
/// replacing it. Every renewal changes the nonce. Opening SlateDB after winning
/// the CAS fences the previous writer before this node becomes ready.
pub async fn run_worker(
    config: Config,
    store: Arc<dyn ObjectStore>,
    state: Arc<WorkerState>,
    token: Option<String>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let Some(ClusterConfig::Worker { node_id, lease_ms }) = &config.cluster else {
        return Err(Error::Config("worker configuration required".into()));
    };
    let ttl = Duration::from_millis(*lease_ms);
    let path = Path::from(format!(
        "{}/coordination/owner.json",
        config.database.prefix
    ));
    let mut observed: Option<(String, Instant)> = None;
    let mut owned: Option<UpdateVersion> = None;
    let mut indexer: Option<(watch::Sender<bool>, tokio::task::JoinHandle<()>)> = None;
    let mut opening: Option<tokio::task::JoinHandle<Result<Engine>>> = None;
    let mut deadline = Instant::now();
    let mut tick = tokio::time::interval(ttl / 3);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = stop.changed() => break, _ = tick.tick() => {} }
        let active_failed = state
            .active
            .read()
            .await
            .as_ref()
            .is_some_and(|a| a.engine.db.status().close_reason.is_some());
        if owned.is_some() && (Instant::now() >= deadline || active_failed) {
            retire(&state, &mut indexer, &mut opening).await;
            owned = None;
            observed = None;
        }
        let started = Instant::now();
        let mode = if let Some(version) = &owned {
            Some(PutMode::Update(version.clone()))
        } else {
            match store.get(&path).await {
                Ok(object) => {
                    let version = UpdateVersion {
                        e_tag: object.meta.e_tag.clone(),
                        version: object.meta.version.clone(),
                    };
                    let etag = version
                        .e_tag
                        .clone()
                        .ok_or_else(|| Error::Config("lease object requires an ETag".into()))?;
                    let lease: Lease = serde_json::from_slice(&object.bytes().await?)?;
                    if !(3000..=300_000).contains(&lease.ttl_ms) {
                        return Err(Error::Corrupt("invalid lease TTL".into()));
                    }
                    match &observed {
                        Some((old, since))
                            if old == &etag
                                && since.elapsed() >= Duration::from_millis(lease.ttl_ms) =>
                        {
                            Some(PutMode::Update(version))
                        }
                        Some((old, _)) if old == &etag => None,
                        _ => {
                            observed = Some((etag, Instant::now()));
                            None
                        }
                    }
                }
                Err(slatedb::object_store::Error::NotFound { .. }) => Some(PutMode::Create),
                Err(error) => {
                    tracing::warn!(%error, "lease read failed");
                    None
                }
            }
        };
        let Some(mode) = mode else {
            continue;
        };
        let lease = Lease {
            node: node_id.clone(),
            nonce: uuid::Uuid::new_v4().to_string(),
            ttl_ms: *lease_ms,
        };
        let result = tokio::time::timeout(
            ttl / 3,
            store.put_opts(
                &path,
                serde_json::to_vec(&lease)?.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            ),
        )
        .await;
        let was_owner = owned.is_some();
        match result {
            Ok(Ok(version)) if started.elapsed() < ttl => {
                owned = Some(UpdateVersion {
                    e_tag: version.e_tag,
                    version: version.version,
                });
                deadline = started + ttl;
                if was_owner {
                    if let Some(active) = state.active.write().await.as_mut() {
                        active.until = deadline;
                    }
                } else {
                    let mut engine_config = config.clone();
                    engine_config.cluster = None;
                    let opening_store = store.clone();
                    // Renew the lease during WAL replay; readiness waits for a
                    // completed open/fence, however long recovery takes.
                    opening = Some(tokio::spawn(async move {
                        Engine::open_with_store(&engine_config, opening_store).await
                    }));
                }
            }
            _ => {
                // An uncertain renewal is treated as loss immediately. A standby
                // will still wait a full unchanged-ETag interval before takeover.
                retire(&state, &mut indexer, &mut opening).await;
                owned = None;
                observed = None;
            }
        }
        if owned.is_some() && opening.as_ref().is_some_and(|task| task.is_finished()) {
            let result = opening.take().expect("opening task exists").await?;
            match result {
                Ok(engine) if Instant::now() < deadline => {
                    let engine = Arc::new(engine);
                    let router = crate::api::configured_router(engine.clone(), token.clone())?;
                    let (tx, rx) = watch::channel(false);
                    let task_engine = engine.clone();
                    indexer = Some((
                        tx,
                        tokio::spawn(async move { task_engine.run_indexer(rx).await }),
                    ));
                    *state.active.write().await = Some(Active {
                        engine,
                        router,
                        until: deadline,
                    });
                    tracing::info!(node = node_id, "shard owner ready");
                }
                Ok(engine) => {
                    let _ = engine.close().await;
                    owned = None;
                    observed = None;
                }
                Err(error) => {
                    tracing::warn!(%error, "owner initialization failed");
                    owned = None;
                    observed = None;
                }
            }
        }
    }
    retire(&state, &mut indexer, &mut opening).await;
    Ok(())
}
async fn retire(
    state: &WorkerState,
    indexer: &mut Option<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
    opening: &mut Option<tokio::task::JoinHandle<Result<Engine>>>,
) {
    let active = state.active.write().await.take();
    if let Some(task) = opening.take() {
        if !task.is_finished() {
            task.abort();
        }
        if let Ok(Ok(engine)) = task.await {
            let _ = engine.close().await;
        }
    }
    if let Some((stop, task)) = indexer.take() {
        let _ = stop.send(true);
        let _ = task.await;
    }
    if let Some(active) = active
        && let Err(error) = active.engine.close().await
    {
        tracing::warn!(%error, "owner close failed");
    }
}

struct Gateway {
    shards: Vec<ShardConfig>,
    identities: BTreeMap<String, String>,
    owners: RwLock<BTreeMap<String, (String, Instant)>>,
    client: reqwest::Client,
    token: String,
    access: Access,
    requests: Semaphore,
}

pub async fn gateway_router(config: &Config, token: Option<String>) -> Result<Router> {
    let Some(ClusterConfig::Gateway {
        shards,
        upstream_token_env,
    }) = &config.cluster
    else {
        return Err(Error::Config("gateway configuration required".into()));
    };
    let upstream = std::env::var(upstream_token_env)
        .map_err(|_| Error::Config(format!("missing {upstream_token_env}")))?;
    if upstream.is_empty() || upstream.bytes().any(|c| !c.is_ascii_graphic()) {
        return Err(Error::Config(
            "upstream token must be printable ASCII without spaces".into(),
        ));
    }
    let mut state = Gateway {
        shards: shards.clone(),
        identities: BTreeMap::new(),
        owners: RwLock::new(BTreeMap::new()),
        client: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?,
        token: upstream,
        access: Access::load(token, &config.auth)?,
        requests: Semaphore::new(64),
    };
    for shard in shards {
        let (_, identity) = state.discover(shard).await?;
        state.identities.insert(shard.id.clone(), identity);
    }
    // Pin placement durably. Changing the shard set or accidentally pointing at
    // another database is rejected instead of silently stranding namespaces.
    let store = config.storage.s3_builder()?.build()?;
    let path = Path::from(format!(
        "{}/coordination/topology.json",
        config.database.prefix
    ));
    let expected = serde_json::to_vec(&state.identities)?;
    match store
        .put_opts(
            &path,
            expected.clone().into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => {}
        Err(
            slatedb::object_store::Error::AlreadyExists { .. }
            | slatedb::object_store::Error::Precondition { .. },
        ) => {
            if store.get(&path).await?.bytes().await?.as_ref() != expected {
                return Err(Error::Config("gateway topology differs from stored placement; migrate data before changing shards".into()));
            }
        }
        Err(error) => return Err(error.into()),
    }
    let state = Arc::new(state);
    Ok(Router::new()
        .route(
            "/healthz",
            get(|| async { Json(json!({"status":"ok", "role":"gateway"})) }),
        )
        .route("/readyz", get(gateway_ready))
        .fallback(gateway_forward)
        .with_state(state))
}

/// Stable rendezvous placement; shard IDs, not worker addresses, determine it.
pub fn shard_for<'a>(namespace: &str, shards: &'a [ShardConfig]) -> Option<&'a ShardConfig> {
    shards
        .iter()
        .max_by_key(|shard| crate::index::digest(format!("{namespace}/{}", shard.id).as_bytes()))
}
impl Gateway {
    async fn discover(&self, shard: &ShardConfig) -> Result<(String, String)> {
        for worker in &shard.workers {
            let response = self
                .client
                .get(format!("{}/readyz", worker.trim_end_matches('/')))
                .timeout(Duration::from_secs(2))
                .send()
                .await;
            if let Ok(response) = response
                && response.status().is_success()
            {
                let value: Value = response.json().await?;
                if let Some(identity) = value["database_identity"].as_str() {
                    if self
                        .identities
                        .get(&shard.id)
                        .is_some_and(|expected| expected != identity)
                    {
                        return Err(Error::Config(
                            "worker database identity differs from shard topology".into(),
                        ));
                    }
                    return Ok((worker.trim_end_matches('/').to_owned(), identity.to_owned()));
                }
            }
        }
        Err(Error::Unavailable)
    }
    async fn owner(&self, shard: &ShardConfig) -> Result<String> {
        if let Some((owner, seen)) = self.owners.read().await.get(&shard.id)
            && seen.elapsed() < Duration::from_secs(1)
        {
            return Ok(owner.clone());
        }
        let (owner, _) = self.discover(shard).await?;
        self.owners
            .write()
            .await
            .insert(shard.id.clone(), (owner.clone(), Instant::now()));
        Ok(owner)
    }
}
async fn gateway_ready(State(state): State<Arc<Gateway>>) -> Result<Json<Value>> {
    for shard in &state.shards {
        state.discover(shard).await?;
    }
    Ok(Json(json!({"status":"ready"})))
}
async fn gateway_forward(State(state): State<Arc<Gateway>>, request: Request) -> Result<Response> {
    state.access.authorize(&request)?;
    let _permit = state.requests.try_acquire().map_err(|_| Error::Busy)?;
    let path = request.uri().path();
    if path == "/v1/namespaces" && request.method() == axum::http::Method::GET {
        let page = axum::extract::Query::<PageRequest>::try_from_uri(request.uri())
            .map_err(|e| Error::Invalid(e.to_string()))?
            .0;
        page.validate()?;
        let limit = page.limit;
        let after = page
            .after
            .as_ref()
            .map_or(String::new(), |id| format!("&after={id}"));
        let pages: Vec<Page<Namespace>> = stream::iter(state.shards.clone())
            .map(|shard| {
                let state = state.clone();
                let after = after.clone();
                async move {
                    let owner = state.owner(&shard).await?;
                    Ok::<_, Error>(
                        state
                            .client
                            .get(format!("{owner}/v1/namespaces?limit={limit}{after}"))
                            .bearer_auth(&state.token)
                            .send()
                            .await?
                            .error_for_status()?
                            .json()
                            .await?,
                    )
                }
            })
            .buffer_unordered(8)
            .try_collect()
            .await?;
        let more = pages.iter().any(|p| p.next_cursor.is_some());
        let mut items: Vec<_> = pages.into_iter().flat_map(|p| p.items).collect();
        items.sort_by(|a, b| a.name.cmp(&b.name));
        let more = more || items.len() > page.limit;
        items.truncate(page.limit);
        let next_cursor = if more {
            items.last().map(|n| n.name.clone())
        } else {
            None
        };
        return Ok(Json(Page { items, next_cursor }).into_response());
    }
    let mut parts = path.split('/').filter(|s| !s.is_empty());
    if parts.next() != Some("v1") || parts.next() != Some("namespaces") {
        return Err(Error::NotFound(
            "gateway route not found; metrics and backups are per worker".into(),
        ));
    }
    let name = parts
        .next()
        .ok_or_else(|| Error::Invalid("namespace required".into()))?;
    validate_id(name)?;
    let shard = shard_for(name, &state.shards).ok_or(Error::Unavailable)?;
    let owner = state.owner(shard).await?;
    let uri = request
        .uri()
        .path_and_query()
        .map_or(path, |v| v.as_str())
        .to_owned();
    let method = request.method().clone();
    let body = to_bytes(request.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|_| {
            Error::Request(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request exceeds 8 MiB".into(),
            )
        })?;
    // Submit once. A transport error may follow a successful commit; never retry
    // writes automatically against another owner.
    let response = state
        .client
        .request(method, format!("{owner}{uri}"))
        .bearer_auth(&state.token)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;
    let response = match response {
        Ok(r) => r,
        Err(error) => {
            state.owners.write().await.remove(&shard.id);
            return Err(error.into());
        }
    };
    let status = response.status();
    if status.is_server_error() {
        state.owners.write().await.remove(&shard.id);
    }
    let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
    let mut result = (status, Body::from_stream(response.bytes_stream())).into_response();
    if let Some(value) = content_type {
        result.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    Ok(result)
}

pub fn s3_store(config: &Config) -> Result<slatedb::object_store::aws::AmazonS3> {
    Ok(config.storage.s3_builder()?.build()?)
}
