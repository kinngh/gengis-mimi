//! Gengis Mimi: durable documents and exact vector search on object storage.

pub mod api;
pub mod config;
pub mod engine;
pub mod error;
pub mod model;
mod search;

pub use engine::Engine;
pub use error::{Error, Result};
