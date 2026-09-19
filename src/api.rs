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
    authorization: Option<String>,
    requests: Arc<Semaphore>,
}

/// HTTP API. An optional bearer token protects /v1; health probes stay public.
pub fn router(engine: Arc<Engine>, api_token: Option<String>) -> Router {
    let state = AppState {
        engine,
        authorization: api_token.map(|token| format!("Bearer {token}")),
        requests: Arc::new(Semaphore::new(64)),
    };
    let api = Router::new()
        .route("/namespaces", get(list_namespaces))
        .route(
            "/namespaces/{namespace}",
            get(get_namespace).put(create_namespace),
        )
        .route("/namespaces/{namespace}/write", post(write))
        .route("/namespaces/{namespace}/documents", get(list_documents))
        .route(
            "/namespaces/{namespace}/documents/{id}",
            get(get_document).delete(delete_document),
        )
        .route("/namespaces/{namespace}/query", post(query))
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
    if let Some(expected) = &state.authorization
        && request
            .headers()
            .get(header::AUTHORIZATION)
            .is_none_or(|value| value.as_bytes() != expected.as_bytes())
    {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid bearer token is required.",
        );
    }
    let Ok(_permit) = state.requests.try_acquire() else {
        return Error::Busy.into_response();
    };
    let started = std::time::Instant::now();
    let method = request.method().clone();
    let response = next.run(request).await;
    tracing::info!(%method, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis() as u64, "request");
    response
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
