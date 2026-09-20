//! Gengis Mimi: durable documents, vector and full-text search on object storage.

pub mod api;
pub mod backup;
mod binary;
pub mod cluster;
pub mod config;
pub mod engine;
pub mod error;
pub mod index;
pub mod metrics;
pub mod model;
mod search;

pub use engine::Engine;
pub use error::{Error, Result};
