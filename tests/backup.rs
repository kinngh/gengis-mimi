mod common;
use common::*;
use futures_util::TryStreamExt;
use gengis_mimi::{Engine, model::*};
use slatedb::{Db, object_store::local::LocalFileSystem};
use std::{io::Write, sync::Arc};

#[tokio::test]
async fn snapshot_backup_survives_later_writes_and_corruption_is_rejected_before_restore() {
    let source = directory();
    let engine = engine(&source, Metric::Cosine).await;
    engine
        .write("demo", write(vec![document("a", [1., 0.], "old", 1)]))
        .await
        .unwrap();
    engine.rebuild_index("demo").await.unwrap();
    let mut backup = engine.export().await.unwrap();
    engine
        .write("demo", write(vec![document("a", [0., 1.], "new", 2)]))
        .await
        .unwrap();
    engine.rebuild_index("demo").await.unwrap();
    let file = source.path().join("snapshot.gmb");
    let mut output = std::fs::File::create(&file).unwrap();
    while let Some(bytes) = backup.try_next().await.unwrap() {
        output.write_all(&bytes).unwrap();
    }
    drop(output);
    let destination = directory();
    let restored = Engine::open(&config(&destination)).await.unwrap();
    let mut corrupt = std::fs::read(&file).unwrap();
    corrupt[20] ^= 1;
    let corrupt_file = source.path().join("corrupt.gmb");
    std::fs::write(&corrupt_file, corrupt).unwrap();
    assert!(restored.restore(&corrupt_file).await.is_err());
    assert!(
        restored
            .namespaces(&PageRequest::default())
            .await
            .unwrap()
            .items
            .is_empty()
    );
    restored.restore(&file).await.unwrap();
    assert_eq!(
        restored.get("demo", "a").await.unwrap().attributes["group"],
        "old"
    );
    assert_eq!(
        restored
            .query("demo", query([1., 0.], 1))
            .await
            .unwrap()
            .matches[0]
            .score,
        1.
    );
    assert!(restored.restore(&file).await.is_err());
    restored.close().await.unwrap();
    engine.close().await.unwrap();
}

#[tokio::test]
async fn v1_migration_can_resume_after_partial_document_conversion() {
    let dir = directory();
    let mut cfg = config(&dir);
    let path = dir.path().join("objects");
    std::fs::create_dir_all(&path).unwrap();
    let db = Db::open(
        cfg.database.prefix.clone(),
        Arc::new(
            LocalFileSystem::new_with_prefix(path)
                .unwrap()
                .with_fsync(true),
        ),
    )
    .await
    .unwrap();
    db.put("meta/format", b"gengis-mimi:1")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    let schema = serde_json::json!({"name":"demo","dimensions":2,"metric":"cosine"});
    db.put("n/demo", serde_json::to_vec(&schema).unwrap())
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    for id in ["a", "b"] {
        db.put(
            format!("d/demo/{id}"),
            serde_json::to_vec(&document(id, [1., 0.], "v1", 1)).unwrap(),
        )
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    }
    db.close().await.unwrap();
    assert!(Engine::open(&cfg).await.is_err());
    cfg.database.migrate = true;
    let engine = Engine::open(&cfg).await.unwrap();
    assert_eq!(engine.stats("demo").await.unwrap().documents, 2);
    assert_eq!(
        engine.get("demo", "b").await.unwrap().vector,
        Some(vec![1., 0.])
    );
    engine.close().await.unwrap();
    drop(engine);
    // Simulate a crash with one converted row and one legacy row remaining.
    let path = dir.path().join("objects");
    let db = Db::open(
        cfg.database.prefix.clone(),
        Arc::new(
            LocalFileSystem::new_with_prefix(path)
                .unwrap()
                .with_fsync(true),
        ),
    )
    .await
    .unwrap();
    db.put("meta/format", b"gengis-mimi:migrating:1:2")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.put(
        "d/demo/b",
        serde_json::to_vec(&document("b", [1., 0.], "v1", 1)).unwrap(),
    )
    .await
    .unwrap()
    .await_durable()
    .await
    .unwrap();
    db.delete("v/demo/b")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.close().await.unwrap();
    let engine = Engine::open(&cfg).await.unwrap();
    assert_eq!(engine.stats("demo").await.unwrap().documents, 2);
    assert_eq!(
        engine.get("demo", "a").await.unwrap().vector,
        Some(vec![1., 0.])
    );
    engine.rebuild_index("demo").await.unwrap();
    assert_eq!(
        engine
            .query("demo", query([1., 0.], 2))
            .await
            .unwrap()
            .matches
            .len(),
        2
    );
    engine.close().await.unwrap();
}
