mod common;

use std::{collections::BTreeMap, sync::Arc};

use gengis_mimi::{Engine, Error, model::*};
use serde_json::json;

use common::*;

#[tokio::test]
async fn durable_documents_tombstones_and_namespaces_survive_reopen_without_cache() {
    let dir = directory();
    let mut config = config(&dir);
    let cache = dir.path().join("cache");
    config.database.cache_dir = Some(cache.clone());
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
            write(vec![
                document("keep", [1., 0.], "a", 1),
                document("gone", [0., 1.], "b", 2),
            ]),
        )
        .await
        .unwrap();
    engine
        .write(
            "demo",
            WriteRequest {
                upsert: vec![document("keep", [0., 1.], "new", 3)],
                delete: vec!["gone".into()],
            },
        )
        .await
        .unwrap();
    engine.close().await.unwrap();
    drop(engine);
    let mut other_database = config.clone();
    other_database.database.prefix = "other/v1".into();
    assert!(
        Engine::open(&other_database).await.is_err(),
        "a cache cannot be reused for another database"
    );
    std::fs::remove_dir_all(cache).unwrap();
    let reopened = Engine::open(&config).await.unwrap();
    assert_eq!(
        reopened.namespace("demo").await.unwrap().config.dimensions,
        Some(2)
    );
    assert_eq!(
        reopened.get("demo", "keep").await.unwrap().attributes["group"],
        "new"
    );
    assert!(matches!(
        reopened.get("demo", "gone").await,
        Err(Error::NotFound(_))
    ));
    let result = reopened.query("demo", query([0., 1.], 10)).await.unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].id, "keep");
    assert_eq!(result.matches[0].score, 1.);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn rejected_batch_changes_nothing_and_scans_do_not_cross_namespaces() {
    let dir = directory();
    let engine = engine(&dir, Metric::Cosine).await;
    engine
        .create_namespace("demo_extra".into(), NamespaceConfig::default())
        .await
        .unwrap();
    engine
        .write(
            "demo",
            write(vec![
                document("a", [1., 0.], "a", 1),
                document("aa", [0., 1.], "a", 1),
                document("b", [1., 1.], "a", 1),
            ]),
        )
        .await
        .unwrap();
    let mut invalid = document("bad", [1., 0.], "a", 1);
    invalid.vector = Some(vec![1.]);
    assert!(
        engine
            .write(
                "demo",
                WriteRequest {
                    upsert: vec![document("new", [1., 0.], "a", 1), invalid],
                    delete: vec!["a".into()]
                }
            )
            .await
            .is_err()
    );
    assert!(engine.get("demo", "a").await.is_ok());
    assert!(engine.get("demo", "new").await.is_err());
    assert!(
        engine
            .write(
                "demo",
                WriteRequest {
                    upsert: vec![document("a", [1., 0.], "b", 2)],
                    delete: vec!["a".into()]
                }
            )
            .await
            .is_err()
    );
    let first = engine
        .documents(
            "demo",
            &PageRequest {
                limit: 2,
                after: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|d| d.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "aa"]
    );
    assert_eq!(first.next_cursor.as_deref(), Some("aa"));
    let next = engine
        .documents(
            "demo",
            &PageRequest {
                limit: 2,
                after: first.next_cursor,
            },
        )
        .await
        .unwrap();
    assert_eq!(next.items.len(), 1);
    assert_eq!(next.items[0].id, "b");
    assert!(next.next_cursor.is_none());
    assert!(
        engine
            .documents("demo_extra", &PageRequest::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(engine.get("demo_extra", "a").await.is_err());
    assert!(engine.namespace("../demo").await.is_err());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn exact_search_filters_before_ranking_and_respects_updates_and_deletes() {
    let dir = directory();
    let engine = engine(&dir, Metric::Cosine).await;
    engine
        .write(
            "demo",
            write(vec![
                document("unmatched", [1., 0.], "private", 10),
                document("alpha", [1., 1.], "public", 2),
                document("beta", [1., 1.], "public", 3),
                document("low", [0., 1.], "public", 0),
            ]),
        )
        .await
        .unwrap();
    let mut request = query([1., 0.], 2);
    request.filter.eq.insert("group".into(), json!("public"));
    request.filter.range.insert(
        "rank".into(),
        NumericRange {
            gte: Some(1.),
            lte: Some(3.),
        },
    );
    let result = engine.query("demo", request.clone()).await.unwrap();
    assert_eq!(
        result
            .matches
            .iter()
            .map(|d| d.id.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert!((result.matches[0].score - 1. / 2_f64.sqrt()).abs() < 1e-10);
    engine
        .write(
            "demo",
            WriteRequest {
                upsert: vec![document("beta", [-1., 0.], "public", 3)],
                delete: vec!["alpha".into()],
            },
        )
        .await
        .unwrap();
    let result = engine.query("demo", request).await.unwrap();
    assert_eq!(result.scanned_documents, 3);
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].id, "beta");
    assert_eq!(result.matches[0].score, -1.);
    assert!(engine.query("demo", query([0., 0.], 1)).await.is_err());
    assert!(
        engine
            .write(
                "demo",
                write(vec![document("nan", [f32::NAN, 1.], "public", 1)])
            )
            .await
            .is_err()
    );
    engine.close().await.unwrap();
}

#[tokio::test]
async fn metric_scores_have_consistent_order_and_document_only_namespaces_work() {
    let dir = directory();
    let engine = Engine::open(&config(&dir)).await.unwrap();
    for (name, metric) in [("dot", Metric::Dot), ("l2", Metric::SquaredEuclidean)] {
        engine
            .create_namespace(
                name.into(),
                NamespaceConfig {
                    dimensions: Some(2),
                    metric,
                },
            )
            .await
            .unwrap();
        engine
            .write(
                name,
                write(vec![
                    document("near", [1., 0.], "a", 1),
                    document("large", [2., 1.], "a", 1),
                    document("zero", [0., 0.], "a", 1),
                ]),
            )
            .await
            .unwrap();
    }
    let dot = engine.query("dot", query([1., 0.], 1)).await.unwrap();
    assert_eq!((&*dot.matches[0].id, dot.matches[0].score), ("large", 2.));
    let l2 = engine.query("l2", query([1., 0.], 3)).await.unwrap();
    assert_eq!(
        l2.matches.iter().map(|h| h.score).collect::<Vec<_>>(),
        [0., -1., -2.]
    );
    engine
        .create_namespace("docs".into(), NamespaceConfig::default())
        .await
        .unwrap();
    let doc = Document {
        id: "plain".into(),
        vector: None,
        attributes: BTreeMap::from([("body".into(), json!("hello"))]),
    };
    engine
        .write("docs", write(vec![doc.clone()]))
        .await
        .unwrap();
    assert_eq!(engine.get("docs", "plain").await.unwrap(), doc);
    assert!(engine.query("docs", query([1., 0.], 1)).await.is_err());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn namespace_creation_is_serialized_and_local_directory_has_one_owner() {
    let dir = directory();
    let engine = Arc::new(Engine::open(&config(&dir)).await.unwrap());
    assert!(Engine::open(&config(&dir)).await.is_err());
    let mut tasks = Vec::new();
    for dimensions in [2, 3] {
        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            engine
                .create_namespace(
                    "race".into(),
                    NamespaceConfig {
                        dimensions: Some(dimensions),
                        metric: Metric::Cosine,
                    },
                )
                .await
        }));
    }
    let first = tasks.remove(0).await.unwrap();
    let second = tasks.remove(0).await.unwrap();
    assert_ne!(first.is_ok(), second.is_ok());
    let stored = engine.namespace("race").await.unwrap();
    assert_eq!(stored.config, first.or(second).unwrap().config);
    engine.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queries_see_whole_batches_while_writes_run() {
    let dir = directory();
    let engine = Arc::new(engine(&dir, Metric::Dot).await);
    let docs = |revision| {
        (0..400)
            .map(|i| {
                document(
                    &format!("doc{i:04}"),
                    if i % 4 == 0 { [1., 0.] } else { [0., 1.] },
                    "a",
                    revision,
                )
            })
            .collect()
    };
    engine.write("demo", write(docs(0))).await.unwrap();
    let writer = engine.clone();
    let task = tokio::spawn(async move {
        for revision in 1..=5 {
            writer.write("demo", write(docs(revision))).await.unwrap();
        }
    });
    for _ in 0..10 {
        let result = engine.query("demo", query([1., 0.], 100)).await.unwrap();
        assert_eq!(result.scanned_documents, 400);
        let revision = &result.matches[0].attributes["rank"];
        assert!(
            result
                .matches
                .iter()
                .all(|hit| &hit.attributes["rank"] == revision)
        );
        assert_eq!(result.matches.first().unwrap().id, "doc0000");
        assert_eq!(result.matches.last().unwrap().id, "doc0396");
    }
    task.await.unwrap();
    engine.close().await.unwrap();
}
