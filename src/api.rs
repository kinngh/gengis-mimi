use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::{Engine, Error, Result, model::*};

#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
    access: Arc<Access>,
    requests: Arc<Semaphore>,
}

/// HTTP API. An optional bearer token protects /v1; health probes stay public.
pub fn router(engine: Arc<Engine>, api_token: Option<String>) -> Router {
    router_with_access(engine, Arc::new(Access::admin(api_token)))
}

pub fn configured_router(engine: Arc<Engine>, api_token: Option<String>) -> Result<Router> {
    let access = Arc::new(Access::load(api_token, &engine.config.auth)?);
    Ok(router_with_access(engine, access))
}

fn router_with_access(engine: Arc<Engine>, access: Arc<Access>) -> Router {
    let state = AppState {
        engine,
        access,
        requests: Arc::new(Semaphore::new(64)),
    };
    let api = Router::new()
        .route("/namespaces", get(list_namespaces))
        .route(
            "/namespaces/{namespace}",
            get(get_namespace)
                .put(create_namespace)
                .delete(delete_namespace),
        )
        .route("/namespaces/{namespace}/write", post(write))
        .route("/namespaces/{namespace}/documents", get(list_documents))
        .route(
            "/namespaces/{namespace}/documents/{id}",
            get(get_document).delete(delete_document),
        )
        .route("/namespaces/{namespace}/query", post(query))
        .route("/namespaces/{namespace}/search", post(search_text))
        .route("/namespaces/{namespace}/stats", get(stats))
        .route(
            "/namespaces/{namespace}/index",
            get(index_status).post(rebuild_index),
        )
        .route("/metrics", get(metrics))
        .route("/export", get(export))
        .route_layer(middleware::from_fn_with_state(state.clone(), admit));
    Router::new()
        .nest("/v1", api)
        .route("/healthz", get(|| async { Json(json!({"status": "ok", "name": "Gengis Mimi", "version": env!("CARGO_PKG_VERSION")})) }))
        .route("/readyz", get(ready))
        .fallback(|| async { api_error(StatusCode::NOT_FOUND, "not_found", "Route not found.") })
        .method_not_allowed_fallback(|| async { api_error(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "Method not allowed.") })
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn admit(State(state): State<AppState>, request: Request, next: Next) -> Response {
    if let Err(error) = state.access.authorize(&request) {
        return error.into_response();
    }
    let Ok(_permit) = state.requests.try_acquire() else {
        return Error::Busy.into_response();
    };
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(state.engine.config.server.request_timeout_ms),
        next.run(request),
    )
    .await
    .unwrap_or_else(|_| Error::Timeout.into_response());
    state
        .engine
        .record_request(response.status().as_u16(), started.elapsed().as_secs_f64());
    tracing::info!(%method, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis() as u64, "request");
    response
}

async fn delete_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>> {
    state.engine.delete_namespace(&name).await?;
    Ok(Json(json!({"deleted": name})))
}
async fn stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<NamespaceStats>> {
    Ok(Json(state.engine.stats(&name).await?))
}
async fn index_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Option<crate::index::IndexStatus>>> {
    Ok(Json(state.engine.index_status(&name).await?))
}
async fn rebuild_index(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<crate::index::IndexStatus>> {
    Ok(Json(state.engine.rebuild_index(&name).await?))
}
async fn search_text(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: std::result::Result<Json<TextQuery>, JsonRejection>,
) -> Result<Json<QueryResult>> {
    Ok(Json(
        state.engine.search_text(&name, json_body(body)?).await?,
    ))
}
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.engine.prometheus(),
    )
}
async fn export(State(state): State<AppState>) -> Result<Response> {
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        axum::body::Body::from_stream(state.engine.export().await?),
    )
        .into_response())
}

/// Shared by the gateway and the embedded API; tokens come from environment.
pub(crate) struct Access {
    admin: Option<String>,
    grants: Vec<(String, String, bool)>,
}
impl Access {
    fn admin(token: Option<String>) -> Self {
        Self {
            admin: token.map(|t| format!("Bearer {t}")),
            grants: vec![],
        }
    }
    pub(crate) fn load(token: Option<String>, config: &crate::config::AuthConfig) -> Result<Self> {
        let mut access = Self::admin(token);
        for grant in &config.grants {
            let token = std::env::var(&grant.token_env).map_err(|_| {
                Error::Config(format!(
                    "missing token environment variable {}",
                    grant.token_env
                ))
            })?;
            if token.is_empty() || token.bytes().any(|c| !c.is_ascii_graphic()) {
                return Err(Error::Config(
                    "tokens must be nonempty printable ASCII without spaces".into(),
                ));
            }
            access.grants.push((
                format!("Bearer {token}"),
                grant.namespace.clone(),
                grant.write,
            ));
        }
        if !access.grants.is_empty() && access.admin.is_none() {
            return Err(Error::Config(
                "namespace grants require an admin API token".into(),
            ));
        }
        Ok(access)
    }
    pub(crate) fn authorize(&self, request: &Request) -> Result<()> {
        let Some(admin) = &self.admin else {
            return Ok(());
        };
        let supplied = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if equal_token(admin, supplied) {
            return Ok(());
        }
        if !self
            .grants
            .iter()
            .any(|(token, _, _)| equal_token(token, supplied))
        {
            return Err(Error::Request(
                StatusCode::UNAUTHORIZED,
                "A valid bearer token is required.".into(),
            ));
        }
        // Nested Axum routers see /namespaces; the gateway sees /v1/namespaces.
        let path = request
            .uri()
            .path()
            .strip_prefix("/v1")
            .unwrap_or(request.uri().path());
        let parts: Vec<_> = path.split('/').filter(|s| !s.is_empty()).collect();
        let namespace = if parts.first() == Some(&"namespaces") {
            parts.get(1).copied()
        } else {
            None
        };
        let read = request.method() == axum::http::Method::GET
            || (request.method() == axum::http::Method::POST
                && parts.len() == 3
                && matches!(parts[2], "query" | "search"));
        if self.grants.iter().any(|(token, name, write)| {
            equal_token(token, supplied) && namespace == Some(name.as_str()) && (read || *write)
        }) {
            return Ok(());
        }
        Err(Error::Request(
            StatusCode::FORBIDDEN,
            "Token does not permit this operation.".into(),
        ))
    }
}
fn equal_token(a: &str, b: &str) -> bool {
    let mut difference = a.len() ^ b.len();
    for (i, byte) in a.bytes().enumerate() {
        difference |= usize::from(byte ^ b.as_bytes().get(i).copied().unwrap_or(0));
    }
    difference == 0
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>> {
    state.engine.check_ready().await?;
    Ok(Json(json!({"status": "ready"})))
}

async fn create_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: std::result::Result<Json<NamespaceConfig>, JsonRejection>,
) -> Result<Json<Namespace>> {
    Ok(Json(
        state
            .engine
            .create_namespace(name, json_body(body)?)
            .await?,
    ))
}

async fn get_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Namespace>> {
    Ok(Json(state.engine.namespace(&name).await?))
}

async fn list_namespaces(
    State(state): State<AppState>,
    page: std::result::Result<Query<PageRequest>, QueryRejection>,
) -> Result<Json<Page<Namespace>>> {
    Ok(Json(state.engine.namespaces(&page_request(page)?).await?))
}

async fn write(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: std::result::Result<Json<WriteRequest>, JsonRejection>,
) -> Result<Json<WriteResult>> {
    Ok(Json(state.engine.write(&name, json_body(body)?).await?))
}

async fn get_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<Document>> {
    Ok(Json(state.engine.get(&name, &id).await?))
}

async fn delete_document(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<Json<WriteResult>> {
    Ok(Json(
        state
            .engine
            .write(
                &name,
                WriteRequest {
                    upsert: vec![],
                    delete: vec![id],
                },
            )
            .await?,
    ))
}

async fn list_documents(
    State(state): State<AppState>,
    Path(name): Path<String>,
    page: std::result::Result<Query<PageRequest>, QueryRejection>,
) -> Result<Json<Page<Document>>> {
    Ok(Json(
        state.engine.documents(&name, &page_request(page)?).await?,
    ))
}

async fn query(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: std::result::Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Json<QueryResult>> {
    Ok(Json(state.engine.query(&name, json_body(body)?).await?))
}

fn json_body<T>(body: std::result::Result<Json<T>, JsonRejection>) -> Result<T> {
    body.map(|Json(value)| value)
        .map_err(|error| Error::Request(error.status(), error.body_text()))
}

fn page_request(
    page: std::result::Result<Query<PageRequest>, QueryRejection>,
) -> Result<PageRequest> {
    page.map(|Query(value)| value)
        .map_err(|error| Error::Invalid(error.body_text()))
}

fn api_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}
