mod common;
use common::*;
use gengis_mimi::{Engine, model::*};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

fn docs() -> Vec<Document> {
    (0..384)
        .map(|i| {
            let angle = i as f32 * 0.131;
            let mut doc = document(
                &format!("d{i:04}"),
                [angle.cos(), angle.sin()],
                if i % 3 == 0 { "a" } else { "b" },
                i,
            );
            doc.attributes.insert(
                "body".into(),
                json!(if i % 2 == 0 {
                    "Rust storage storage engine"
                } else {
                    "A search engine"
                }),
            );
            doc.attributes
                .insert("zero".into(), json!([if i % 2 == 0 { -0.0 } else { 0.0 }]));
            doc.attributes
                .insert("signed".into(), json!(i as i32 - 192));
            doc
        })
        .collect()
}
async fn setup() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = directory();
    let mut cfg = config(&dir);
    cfg.index.clusters = 12;
    cfg.index.interval_ms = 0;
    let engine = Arc::new(Engine::open(&cfg).await.unwrap());
    engine
        .create_namespace(
            "demo".into(),
            NamespaceConfig {
                dimensions: Some(2),
                metric: Metric::Cosine,
                text_fields: vec!["body".into()],
            },
        )
        .await
        .unwrap();
    engine.write("demo", write(docs())).await.unwrap();
    (dir, engine)
}
fn ids(result: &QueryResult) -> Vec<&str> {
    result.matches.iter().map(|h| h.id.as_str()).collect()
}
async fn exact(engine: &Engine, q: &QueryRequest) -> QueryResult {
    let mut q = q.clone();
    q.mode = SearchMode::Exact;
    engine.query("demo", q).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ann_and_filter_indexes_match_exact_with_pending_updates_deletes_and_rebuilds() {
    let (dir, engine) = setup().await;
    let first = engine.rebuild_index("demo").await.unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(engine.stats("demo").await.unwrap().pending_documents, 0);
    let mut q = query([1., 0.], 20);
    q.probes = 1024;
    let ann = engine.query("demo", q.clone()).await.unwrap();
    assert_eq!(ann.plan, "centroid_ann");
    assert_eq!(ids(&ann), ids(&exact(&engine, &q).await));
    q.filter.eq.insert("group".into(), json!("a"));
    q.filter.range.insert(
        "signed".into(),
        NumericRange {
            gte: Some(-100.),
            lte: Some(0.),
        },
    );
    let filtered = engine.query("demo", q.clone()).await.unwrap();
    assert_eq!(filtered.plan, "filtered_exact");
    assert!(filtered.scanned_documents < 100);
    assert_eq!(ids(&filtered), ids(&exact(&engine, &q).await));
    let mut zero_query = q.clone();
    zero_query.filter.eq.insert("zero".into(), json!([0.0]));
    assert_eq!(
        ids(&engine.query("demo", zero_query.clone()).await.unwrap()),
        ids(&exact(&engine, &zero_query).await)
    );
    let mut changed = document("d0000", [-1., 0.], "b", 0);
    changed.attributes.insert("body".into(), json!("storage"));
    let mut new = document("new", [1., 0.], "a", 0);
    new.attributes.insert("signed".into(), json!(-1));
    new.attributes
        .insert("body".into(), json!("storage storage"));
    engine
        .write(
            "demo",
            WriteRequest {
                upsert: vec![changed, new],
                delete: vec!["d0003".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        ids(&engine.query("demo", q.clone()).await.unwrap()),
        ids(&exact(&engine, &q).await)
    );
    let building = engine.clone();
    let task = tokio::spawn(async move { building.rebuild_index("demo").await.unwrap() });
    for revision in 2..8 {
        let mut doc = document("new", [1., 0.], "a", revision);
        doc.attributes.insert("signed".into(), json!(-1));
        engine.write("demo", write(vec![doc])).await.unwrap();
        assert_eq!(
            ids(&engine.query("demo", q.clone()).await.unwrap()),
            ids(&exact(&engine, &q).await)
        );
    }
    task.await.unwrap();
    engine.rebuild_index("demo").await.unwrap();
    assert_eq!(engine.stats("demo").await.unwrap().pending_documents, 0);
    engine.close().await.unwrap();
    drop(engine);
    let reopened = Engine::open(&config(&dir)).await.unwrap();
    assert_eq!(
        ids(&reopened.query("demo", q.clone()).await.unwrap()),
        ids(&exact(&reopened, &q).await)
    );
    assert!(reopened.get("demo", "d0003").await.is_err());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn bm25_corpus_statistics_and_postings_track_mutations_and_selective_filters() {
    let (_dir, engine) = setup().await;
    let mut q = TextQuery {
        field: "body".into(),
        text: "STORAGE engine".into(),
        top_k: 100,
        filter: Filter::default(),
    };
    q.filter.eq.insert("group".into(), json!("a"));
    let scanned = engine.search_text("demo", q.clone()).await.unwrap();
    assert_eq!(scanned.plan, "bm25_scan");
    engine.rebuild_index("demo").await.unwrap();
    let indexed = engine.search_text("demo", q.clone()).await.unwrap();
    assert_eq!(indexed.plan, "bm25_index");
    assert_eq!(ids(&scanned), ids(&indexed));
    for (a, b) in scanned.matches.iter().zip(&indexed.matches) {
        assert!((a.score - b.score).abs() < 1e-10);
    }
    let mut doc = document("d0000", [1., 0.], "a", 0);
    doc.attributes.insert("body".into(), json!("storage"));
    let mut new = doc.clone();
    new.id = "new".into();
    new.attributes
        .insert("body".into(), json!("a completely different corpus"));
    engine
        .write(
            "demo",
            WriteRequest {
                upsert: vec![doc, new],
                delete: vec!["d0006".into()],
            },
        )
        .await
        .unwrap();
    let pending = engine.search_text("demo", q.clone()).await.unwrap();
    engine.rebuild_index("demo").await.unwrap();
    let caught_up = engine.search_text("demo", q).await.unwrap();
    assert_eq!(ids(&pending), ids(&caught_up));
    for (a, b) in pending.matches.iter().zip(&caught_up.matches) {
        assert!((a.score - b.score).abs() < 1e-10);
    }
    engine.delete_namespace("demo").await.unwrap();
    assert!(engine.namespace("demo").await.is_err());
    engine
        .create_namespace("demo".into(), NamespaceConfig::default())
        .await
        .unwrap();
    assert_eq!(engine.stats("demo").await.unwrap().documents, 0);
    assert!(engine.index_status("demo").await.unwrap().is_none());
    engine.close().await.unwrap();
}

#[tokio::test]
async fn quotas_reject_whole_batches_and_release_after_indexing_or_deletion() {
    let dir = directory();
    let mut cfg = config(&dir);
    cfg.limits.max_documents = 2;
    cfg.limits.max_pending_documents = 2;
    let engine = Engine::open(&cfg).await.unwrap();
    engine
        .create_namespace("demo".into(), NamespaceConfig::default())
        .await
        .unwrap();
    let doc = |id: &str| Document {
        id: id.into(),
        vector: None,
        attributes: BTreeMap::new(),
    };
    engine
        .write("demo", write(vec![doc("a"), doc("b")]))
        .await
        .unwrap();
    assert!(
        engine
            .write(
                "demo",
                WriteRequest {
                    upsert: vec![doc("c")],
                    delete: vec!["a".into()]
                }
            )
            .await
            .is_err()
    );
    assert!(engine.get("demo", "a").await.is_ok());
    engine.rebuild_index("demo").await.unwrap();
    engine
        .write(
            "demo",
            WriteRequest {
                upsert: vec![doc("c")],
                delete: vec!["a".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(engine.stats("demo").await.unwrap().documents, 2);
    engine.close().await.unwrap();
}
