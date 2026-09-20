mod common;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use gengis_mimi::{Engine, model::*};
use slatedb::{
    bytes::Bytes,
    object_store::{memory::InMemory, path::Path, *},
};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Debug)]
struct InterruptedStore {
    inner: InMemory,
    stalled: AtomicBool,
    reached: tokio::sync::Notify,
}
impl fmt::Display for InterruptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "interrupted test store")
    }
}
#[async_trait]
impl ObjectStore for InterruptedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if path.as_ref().contains("/wal/") {
            if self.stalled.load(Ordering::SeqCst) {
                self.reached.notify_one();
            }
            while self.stalled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    async fn get_ranges(&self, path: &Path, ranges: &[std::ops::Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(path, ranges).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_storage_never_acknowledges_and_cancelled_writers_keep_commit_serialization() {
    let dir = common::directory();
    let config = common::config(&dir);
    let store = Arc::new(InterruptedStore {
        inner: InMemory::new(),
        stalled: AtomicBool::new(false),
        reached: tokio::sync::Notify::new(),
    });
    let engine = Arc::new(
        Engine::open_with_store(&config, store.clone())
            .await
            .unwrap(),
    );
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
            common::write(vec![
                common::document("a", [1., 0.], "old", 1),
                common::document("b", [0., 1.], "old", 1),
            ]),
        )
        .await
        .unwrap();
    engine.rebuild_index("demo").await.unwrap();
    store.stalled.store(true, Ordering::SeqCst);
    let writer = engine.clone();
    let task = tokio::spawn(async move {
        writer
            .write(
                "demo",
                WriteRequest {
                    upsert: vec![common::document("a", [0., 1.], "new", 2)],
                    delete: vec!["b".into()],
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), store.reached.notified())
        .await
        .unwrap();
    assert!(!task.is_finished());
    task.abort(); // The commit task retains its lock and completes after recovery.
    let reader = engine.clone();
    let read = tokio::spawn(async move { reader.query("demo", common::query([0., 1.], 2)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !read.is_finished(),
        "a snapshot must not race an unacknowledged application commit"
    );
    store.stalled.store(false, Ordering::SeqCst);
    let result = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].id, "a");
    assert_eq!(result.matches[0].attributes["group"], "new");
    engine.close().await.unwrap();
    drop(engine);
    let reopened = Engine::open_with_store(&config, store).await.unwrap();
    assert!(reopened.get("demo", "b").await.is_err());
    assert_eq!(
        reopened.get("demo", "a").await.unwrap().attributes["group"],
        "new"
    );
    reopened.close().await.unwrap();
}
