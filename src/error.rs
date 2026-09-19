use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("{1}")]
    Request(StatusCode, String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("too many concurrent queries; retry later")]
    Busy,
    #[error("{0}")]
    Config(String),
    #[error("storage failure: {0}")]
    Storage(#[from] slatedb::Error),
    #[error("object store failure: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("stored data could not be decoded: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Self::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request", self.to_string()),
            Self::Request(status, _) => (*status, "invalid_request", self.to_string()),
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found", self.to_string()),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict", self.to_string()),
            Self::Busy => (StatusCode::SERVICE_UNAVAILABLE, "busy", self.to_string()),
            Self::Storage(_) | Self::ObjectStore(_) => {
                tracing::error!(error = %self, "storage request failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "storage_unavailable",
                    "Storage unavailable; a failed write may still have committed.".into(),
                )
            }
            _ => {
                tracing::error!(error = %self, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "Internal server error.".into(),
                )
            }
        };
        (
            status,
            Json(json!({"error": {"code": code, "message": message}})),
        )
            .into_response()
    }
}
