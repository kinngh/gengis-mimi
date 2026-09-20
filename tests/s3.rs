mod common;

use gengis_mimi::{
    Engine,
    config::{Config, DatabaseConfig, StorageConfig},
    model::{Metric, NamespaceConfig},
};

/// Opt-in: the user supplies an existing bucket and credentials. Objects are
/// retained under a unique prefix for inspection; never touches other prefixes.
#[tokio::test]
#[ignore = "requires an S3/MinIO bucket; see docs/minio.md"]
async fn s3_durability_and_reopen() {
    let bucket = std::env::var("GENGIS_MIMI_TEST_BUCKET").expect("set GENGIS_MIMI_TEST_BUCKET");
    let endpoint = std::env::var("GENGIS_MIMI_TEST_ENDPOINT").ok();
    let prefix = format!(
        "gengis-mimi-tests/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    eprintln!("Test objects will be retained under {prefix}");
    let mut config = Config {
        storage: StorageConfig::S3 {
            bucket,
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            allow_http: endpoint
                .as_ref()
                .is_some_and(|url| url.starts_with("http://")),
            endpoint,
        },
        database: DatabaseConfig {
            prefix,
            ..DatabaseConfig::default()
        },
        ..Config::default()
    };
    config.database.l0_sst_bytes = 64 * 1024;
    config.database.wal_flush_ms = 5;
    let engine = Engine::open(&config).await.unwrap();
    engine
        .create_namespace(
            "demo".into(),
            NamespaceConfig {
                dimensions: Some(2),
                metric: Metric::Cosine,
                text_fields: vec![],
            },
        )
        .await
        .unwrap();
    engine
        .write(
            "demo",
            common::write(vec![common::document("hello", [1., 0.], "s3", 1)]),
        )
        .await
        .unwrap();
    engine.rebuild_index("demo").await.unwrap();
    engine.close().await.unwrap();
    drop(engine);
    let reopened = Engine::open(&config).await.unwrap();
    assert_eq!(
        reopened.get("demo", "hello").await.unwrap().attributes["group"],
        "s3"
    );
    assert_eq!(
        reopened
            .query("demo", common::query([1., 0.], 1))
            .await
            .unwrap()
            .matches[0]
            .id,
        "hello"
    );
    // A replacement writer fences the old handle; it cannot acknowledge writes.
    let replacement = Engine::open(&config).await.unwrap();
    assert!(
        reopened
            .write(
                "demo",
                common::write(vec![common::document("stale", [1., 0.], "old", 1)])
            )
            .await
            .is_err()
    );
    let _ = reopened.close().await;
    assert!(replacement.get("demo", "stale").await.is_err());
    for revision in 0..12 {
        let docs = (0..256)
            .map(|i| {
                let mut doc =
                    common::document(&format!("d{i:04}"), [1., i as f32 / 256.], "s3", revision);
                doc.attributes
                    .insert("body".into(), serde_json::json!("storage ".repeat(128)));
                doc
            })
            .collect();
        replacement
            .write("demo", common::write(docs))
            .await
            .unwrap();
    }
    replacement.rebuild_index("demo").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if replacement.prometheus().lines().any(|line| {
                line.starts_with("slatedb_compactor_bytes_compacted")
                    && line
                        .split_whitespace()
                        .last()
                        .and_then(|n| n.parse::<u64>().ok())
                        .is_some_and(|n| n > 0)
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("compaction must make progress under repeated updates");
    eprintln!(
        "{}",
        replacement
            .prometheus()
            .lines()
            .filter(|line| line.contains("compaction")
                && (line.contains("bytes") || line.contains("running")))
            .collect::<Vec<_>>()
            .join("\n")
    );
    replacement.close().await.unwrap();
    let store = std::sync::Arc::new(gengis_mimi::cluster::s3_store(&config).unwrap());
    let admin = slatedb::admin::Admin::builder(config.database.prefix.clone(), store).build();
    let mut gc = slatedb::config::GarbageCollectorOptions::default();
    for options in [
        &mut gc.manifest_options,
        &mut gc.wal_options,
        &mut gc.compacted_options,
        &mut gc.compactions_options,
    ]
    .into_iter()
    .flatten()
    {
        options.min_age = std::time::Duration::ZERO;
    }
    admin.run_gc_once(gc).await.unwrap();
    let final_engine = Engine::open(&config).await.unwrap();
    assert_eq!(
        final_engine.get("demo", "d0000").await.unwrap().attributes["rank"],
        11
    );
    assert_eq!(
        final_engine
            .query("demo", common::query([1., 0.], 10))
            .await
            .unwrap()
            .matches
            .len(),
        10
    );
    final_engine.close().await.unwrap();
}
