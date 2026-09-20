#![allow(dead_code)]

use std::collections::BTreeMap;

use gengis_mimi::{
    Engine,
    config::{Config, DatabaseConfig, StorageConfig},
    model::{Document, Filter, Metric, NamespaceConfig, QueryRequest, WriteRequest},
};
use serde_json::{Value, json};
use tempfile::TempDir;

pub fn directory() -> TempDir {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

pub fn config(dir: &TempDir) -> Config {
    Config {
        storage: StorageConfig::Local {
            path: dir.path().join("objects"),
        },
        database: DatabaseConfig {
            wal_flush_ms: 5,
            ..DatabaseConfig::default()
        },
        ..Config::default()
    }
}

pub async fn engine(dir: &TempDir, metric: Metric) -> Engine {
    let engine = Engine::open(&config(dir)).await.unwrap();
    engine
        .create_namespace(
            "demo".into(),
            NamespaceConfig {
                dimensions: Some(2),
                metric,
                text_fields: vec![],
            },
        )
        .await
        .unwrap();
    engine
}

pub fn document(id: &str, vector: [f32; 2], group: &str, rank: u32) -> Document {
    Document {
        id: id.into(),
        vector: Some(vector.to_vec()),
        attributes: BTreeMap::from([
            ("group".into(), Value::from(group)),
            ("rank".into(), json!(rank)),
        ]),
    }
}

pub fn write(documents: Vec<Document>) -> WriteRequest {
    WriteRequest {
        upsert: documents,
        delete: vec![],
    }
}

pub fn query(vector: [f32; 2], top_k: usize) -> QueryRequest {
    QueryRequest {
        vector: vector.to_vec(),
        top_k,
        filter: Filter::default(),
        mode: gengis_mimi::model::SearchMode::Auto,
        probes: 4,
    }
}
