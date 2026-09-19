mod common;

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use gengis_mimi::{Engine, api::router};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn send(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", token);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn http_contract_covers_auth_crud_search_and_invalid_requests() {
    let dir = common::directory();
    let engine = Arc::new(Engine::open(&common::config(&dir)).await.unwrap());
    let app = router(engine.clone(), Some("test-token".into()));
    let auth_header = "Bearer test-token";
    let auth = Some(auth_header);
    assert_eq!(
        send(&app, "GET", "/healthz", Value::Null, None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&app, "GET", "/v1/namespaces", Value::Null, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            "/v1/namespaces/demo",
            json!({"dimensions": 2}),
            auth
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, body) = send(
        &app,
        "POST",
        "/v1/namespaces/demo/write",
        json!({"upsert":[{"id":"a","vector":[1,0],"attributes":{"kind":"doc"}}]}),
        auth,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["sequence"].as_u64().unwrap() > 0);
    let (status, body) = send(
        &app,
        "POST",
        "/v1/namespaces/demo/query",
        json!({"vector":[1,0],"top_k":1}),
        auth,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matches"][0]["id"], "a");
    assert_eq!(body["matches"][0]["score"], 1.);
    assert!(body["matches"][0].get("vector").is_none());
    assert_eq!(
        send(
            &app,
            "POST",
            "/v1/namespaces/demo/query",
            json!({"vector":[1],"top_k":1}),
            auth
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &app,
            "POST",
            "/v1/namespaces/demo/query",
            json!({"vector":[1,0],"typo":1}),
            auth
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        send(
            &app,
            "PUT",
            "/v1/namespaces/demo",
            json!({"dimensions": 3}),
            auth
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        send(
            &app,
            "GET",
            "/v1/namespaces/demo/documents?limit=0",
            Value::Null,
            auth
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(
            &app,
            "DELETE",
            "/v1/namespaces/demo/documents/a",
            Value::Null,
            auth
        )
        .await
        .0,
        StatusCode::OK
    );
    let (status, body) = send(
        &app,
        "GET",
        "/v1/namespaces/demo/documents/a",
        Value::Null,
        auth,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
    let too_large = "x".repeat(gengis_mimi::model::MAX_BODY_BYTES + 1);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/namespaces/demo/write")
                .header("content-type", "application/json")
                .header("authorization", auth_header)
                .body(Body::from(too_large))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    engine.close().await.unwrap();
}
