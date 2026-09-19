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
    let config = Config {
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
    let engine = Engine::open(&config).await.unwrap();
    engine
        .create_namespace(
            "demo".into(),
            NamespaceConfig {
                dimensions: Some(2),
                metric: Metric::Cosine,
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
    reopened.close().await.unwrap();
}
