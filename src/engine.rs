use std::{
    collections::{BinaryHeap, HashSet},
    fs::{File, OpenOptions},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use serde::de::DeserializeOwned;
use slatedb::{
    Db, DbIterator, DbSnapshot, Settings, WriteBatch,
    config::{DurabilityLevel, ReadOptions, ScanOptions},
    object_store::{
        ObjectStore,
        aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
        local::LocalFileSystem,
    },
};
use tokio::sync::{Mutex, Semaphore};

use crate::{
    Error, Result,
    config::{Config, StorageConfig},
    model::*,
    search::score_batch,
};

const FORMAT_KEY: &str = "meta/format";
const FORMAT: &[u8] = b"gengis-mimi:1";
const SEARCH_BATCH_BYTES: usize = 1024 * 1024;

/// One writer database containing isolated namespace key ranges.
/// Call `close()` after draining requests to flush and stop background work.
pub struct Engine {
    db: Db,
    schema_lock: Arc<Mutex<()>>,
    query_slots: Arc<Semaphore>,
    local_lock: Option<File>,
    cache_lock: Option<File>,
}

impl Engine {
    pub async fn open(config: &Config) -> Result<Self> {
        config.validate()?;
        let mut settings = Settings {
            flush_interval: Some(Duration::from_millis(config.database.wal_flush_ms)),
            l0_sst_size_bytes: 16 * 1024 * 1024,
            max_unflushed_bytes: 64 * 1024 * 1024,
            ..Settings::default()
        };
        let (store, local_lock, storage_identity): (
            Arc<dyn ObjectStore>,
            Option<File>,
            serde_json::Value,
        ) = match &config.storage {
            StorageConfig::Local { path } => {
                std::fs::create_dir_all(path)?;
                let lock = lock_directory(path)?;
                // LocalFileSystem has no conditional overwrite. Retain metadata rather
                // than weaken SlateDB's GC boundary protocol. Data-file GC stays enabled.
                if let Some(gc) = settings.garbage_collector_options.as_mut() {
                    gc.manifest_options = None;
                    gc.compactions_options = None;
                }
                (
                    Arc::new(LocalFileSystem::new_with_prefix(path)?.with_fsync(true)),
                    Some(lock),
                    serde_json::json!({"type":"local", "path":std::fs::canonicalize(path)?}),
                )
            }
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(*allow_http)
                    .with_virtual_hosted_style_request(false)
                    .with_conditional_put(S3ConditionalPut::ETagMatch);
                if let Some(endpoint) = endpoint {
                    // The service-specific environment endpoint otherwise overrides
                    // with_endpoint() in object_store's builder.
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
                }
                let effective_endpoint = builder
                    .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
                    .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint));
                let identity = serde_json::json!({"type":"s3", "bucket":bucket, "region":region, "endpoint":effective_endpoint});
                (Arc::new(builder.build()?), None, identity)
            }
        };
        let mut cache_lock = None;
        if let Some(cache_dir) = &config.database.cache_dir {
            std::fs::create_dir_all(cache_dir)?;
            let cache_path = std::fs::canonicalize(cache_dir)?;
            if let StorageConfig::Local { path } = &config.storage {
                let data_path = std::fs::canonicalize(path)?;
                if cache_path.starts_with(&data_path) || data_path.starts_with(&cache_path) {
                    return Err(Error::Config(
                        "cache and durable data directories must not overlap".into(),
                    ));
                }
            }
            // A cache cannot be reused for a different bucket/prefix. SlateDB's raw
            // object paths alone do not distinguish buckets.
            cache_lock = Some(lock_directory(&cache_path)?);
            let identity = serde_json::to_vec(&(storage_identity, &config.database.prefix))?;
            let identity_path = cache_path.join("gengis-mimi-cache.json");
            match std::fs::read(&identity_path) {
                Ok(existing) if existing != identity => return Err(Error::Config(
                    "cache belongs to a different storage configuration; choose a new cache_dir"
                        .into(),
                )),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::write(identity_path, identity)?
                }
                Err(e) => return Err(e.into()),
            }
            settings.object_store_cache_options.root_folder = Some(cache_path);
            settings.object_store_cache_options.max_cache_size_bytes =
                Some(config.database.cache_bytes);
        }
        let db = Db::builder(config.database.prefix.clone(), store)
            .with_settings(settings)
            .build()
            .await?;
        let engine = Self {
            db,
            schema_lock: Arc::new(Mutex::new(())),
            query_slots: Arc::new(Semaphore::new(config.server.max_concurrent_queries)),
            local_lock,
            cache_lock,
        };
        if let Err(error) = engine.initialize_format().await {
            let _ = engine.close().await;
            return Err(error);
        }
        Ok(engine)
    }

    async fn initialize_format(&self) -> Result<()> {
        match self
            .db
            .get_with_options(FORMAT_KEY, &read_options())
            .await?
        {
            Some(value) if value.as_ref() == FORMAT => Ok(()),
            Some(_) => Err(Error::Config(
                "unsupported Gengis Mimi storage format".into(),
            )),
            None => {
                let mut existing = self.db.scan_with_options(.., &scan_options()).await?;
                if existing.next().await?.is_some() {
                    return Err(Error::Config(
                        "database prefix contains an unrecognized keyspace".into(),
                    ));
                }
                self.db
                    .put(FORMAT_KEY, FORMAT)
                    .await?
                    .await_durable()
                    .await?;
                Ok(())
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        let _guard = self.schema_lock.lock().await;
        self.db.close().await?;
        if let Some(lock) = &self.local_lock {
            lock.unlock()?;
        }
        if let Some(lock) = &self.cache_lock {
            lock.unlock()?;
        }
        Ok(())
    }

    pub async fn check_ready(&self) -> Result<()> {
        self.db
            .get_with_options(FORMAT_KEY, &read_options())
            .await?;
        Ok(())
    }

    /// Repeating creation with identical configuration succeeds; changes conflict.
    pub async fn create_namespace(
        &self,
        name: String,
        config: NamespaceConfig,
    ) -> Result<Namespace> {
        validate_id(&name)?;
        config.validate()?;
        let db = self.db.clone();
        let guard = self.schema_lock.clone().lock_owned().await;
        // Keep the check-and-create serialized even if the HTTP request disconnects.
        tokio::spawn(async move {
            let _guard = guard;
            let key = namespace_key(&name);
            if let Some(value) = db.get_with_options(&key, &read_options()).await? {
                let existing: Namespace = serde_json::from_slice(&value)?;
                if existing.config != config {
                    return Err(Error::Conflict(
                        "namespace configuration is immutable".into(),
                    ));
                }
                return Ok(existing);
            }
            let namespace = Namespace { name, config };
            db.put(key, serde_json::to_vec(&namespace)?)
                .await?
                .await_durable()
                .await?;
            Ok(namespace)
        })
        .await?
    }

    pub async fn namespace(&self, name: &str) -> Result<Namespace> {
        validate_id(name)?;
        let value = self
            .db
            .get_with_options(namespace_key(name), &read_options())
            .await?
            .ok_or_else(|| Error::NotFound(format!("namespace '{name}' not found")))?;
        Ok(serde_json::from_slice(&value)?)
    }

    pub async fn namespaces(&self, page: &PageRequest) -> Result<Page<Namespace>> {
        page.validate()?;
        let snapshot = self.db.snapshot().await?;
        let iter = page_iterator(&snapshot, "n/", page).await?;
        collect_page(iter, page.limit, |namespace: &Namespace| &namespace.name).await
    }

    /// Validates the entire batch before submitting one atomic SlateDB write.
    pub async fn write(&self, namespace: &str, request: WriteRequest) -> Result<WriteResult> {
        let schema = self.namespace(namespace).await?.config;
        let count = request.upsert.len() + request.delete.len();
        if !(1..=MAX_BATCH_OPERATIONS).contains(&count) {
            return Err(Error::Invalid(format!(
                "a batch requires 1–{MAX_BATCH_OPERATIONS} operations"
            )));
        }
        let mut seen = HashSet::with_capacity(count);
        let mut batch = WriteBatch::new();
        let mut size = 0;
        for document in &request.upsert {
            validate_id(&document.id)?;
            if !seen.insert(document.id.as_str()) {
                return Err(Error::Invalid(
                    "a document ID may occur only once per batch".into(),
                ));
            }
            if let Some(vector) = &document.vector {
                schema.validate_vector(vector)?;
            }
            let bytes = serde_json::to_vec(document)?;
            if bytes.len() > MAX_DOCUMENT_BYTES {
                return Err(Error::Invalid("serialized document exceeds 256 KiB".into()));
            }
            size += bytes.len();
            batch.put(document_key(namespace, &document.id), bytes);
        }
        for id in &request.delete {
            validate_id(id)?;
            if !seen.insert(id.as_str()) {
                return Err(Error::Invalid(
                    "a document ID may occur only once per batch".into(),
                ));
            }
            size += id.len();
            batch.delete(document_key(namespace, id));
        }
        if size > MAX_BODY_BYTES {
            return Err(Error::Invalid("batch exceeds 8 MiB".into()));
        }
        let handle = self.db.write(batch).await?;
        handle.await_durable().await?;
        Ok(WriteResult {
            upserted: request.upsert.len(),
            deleted: request.delete.len(),
            sequence: handle.seqnum(),
        })
    }

    pub async fn get(&self, namespace: &str, id: &str) -> Result<Document> {
        validate_id(id)?;
        self.namespace(namespace).await?;
        let value = self
            .db
            .get_with_options(document_key(namespace, id), &read_options())
            .await?
            .ok_or_else(|| Error::NotFound(format!("document '{id}' not found")))?;
        Ok(serde_json::from_slice(&value)?)
    }

    pub async fn documents(&self, namespace: &str, page: &PageRequest) -> Result<Page<Document>> {
        page.validate()?;
        self.namespace(namespace).await?;
        let snapshot = self.db.snapshot().await?;
        let iter = page_iterator(&snapshot, &document_prefix(namespace), page).await?;
        collect_page(iter, page.limit, |document: &Document| &document.id).await
    }

    /// Exact search of one durable snapshot, including all acknowledged writes.
    pub async fn query(&self, namespace: &str, request: QueryRequest) -> Result<QueryResult> {
        let permit = self
            .query_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let schema = self.namespace(namespace).await?.config;
        schema.validate_vector(&request.vector)?;
        request.filter.validate()?;
        if !(1..=MAX_TOP_K).contains(&request.top_k) {
            return Err(Error::Invalid(format!(
                "top_k must be between 1 and {MAX_TOP_K}"
            )));
        }
        let snapshot = self.db.snapshot().await?;
        let mut iter = snapshot
            .scan_prefix_with_options(document_prefix(namespace), .., &scan_options())
            .await?;
        let request = Arc::new(request);
        // A cancelled request can leave one blocking batch running. Keep its permit
        // alive until that batch exits so cancellations cannot bypass the CPU limit.
        let permit = Arc::new(permit);
        let mut best = BinaryHeap::new();
        let mut scanned_documents = 0;
        loop {
            let mut rows = Vec::new();
            let mut bytes = 0;
            while rows.len() < 128 && bytes < SEARCH_BATCH_BYTES {
                let Some(row) = iter.next().await? else { break };
                bytes += row.value.len();
                rows.push(row.value);
            }
            if rows.is_empty() {
                break;
            }
            scanned_documents += rows.len();
            let query = request.clone();
            let permit = permit.clone();
            best = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                score_batch(rows, best, &query, schema.metric)
            })
            .await??;
        }
        let mut hits = best.into_vec();
        hits.sort();
        Ok(QueryResult {
            matches: hits.into_iter().map(|hit| hit.0).collect(),
            scanned_documents,
        })
    }
}

fn namespace_key(name: &str) -> String {
    format!("n/{name}")
}
fn document_prefix(namespace: &str) -> String {
    format!("d/{namespace}/")
}
fn document_key(namespace: &str, id: &str) -> String {
    format!("d/{namespace}/{id}")
}
fn read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..ReadOptions::default()
    }
}
fn scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: DurabilityLevel::Remote,
        ..ScanOptions::default()
    }
}

fn lock_directory(path: &std::path::Path) -> Result<File> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path.join(".gengis-mimi.lock"))?;
    lock.try_lock().map_err(|e| {
        Error::Config(format!(
            "cannot exclusively lock directory {}: {e}",
            path.display()
        ))
    })?;
    Ok(lock)
}

async fn page_iterator(
    snapshot: &DbSnapshot,
    prefix: &str,
    page: &PageRequest,
) -> Result<DbIterator> {
    let start = page
        .after
        .as_ref()
        .map_or(Bound::Unbounded, |after| Bound::Excluded(after.as_bytes()));
    Ok(snapshot
        .scan_prefix_with_options(prefix, (start, Bound::<&[u8]>::Unbounded), &scan_options())
        .await?)
}

async fn collect_page<T: DeserializeOwned>(
    mut iter: DbIterator,
    limit: usize,
    id: impl Fn(&T) -> &str,
) -> Result<Page<T>> {
    let mut items = Vec::with_capacity(limit);
    while let Some(row) = iter.next().await? {
        if items.len() == limit {
            let next_cursor = items.last().map(|item| id(item).to_owned());
            return Ok(Page { items, next_cursor });
        }
        items.push(serde_json::from_slice(&row.value)?);
    }
    Ok(Page {
        items,
        next_cursor: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn reads_and_search_hide_unflushed_updates_and_tombstones() {
        let db = Db::builder("test", Arc::new(InMemory::new()))
            .with_settings(Settings {
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        let namespace = Namespace {
            name: "demo".into(),
            config: NamespaceConfig {
                dimensions: Some(2),
                metric: Metric::Dot,
            },
        };
        let make_doc = |id: &str, vector| Document {
            id: id.into(),
            vector: Some(vector),
            attributes: Default::default(),
        };
        let old = make_doc("a", vec![1., 0.]);
        let mut initial = WriteBatch::new();
        initial.put(
            namespace_key("demo"),
            serde_json::to_vec(&namespace).unwrap(),
        );
        initial.put(document_key("demo", "a"), serde_json::to_vec(&old).unwrap());
        initial.put(
            document_key("demo", "b"),
            serde_json::to_vec(&make_doc("b", vec![0., 1.])).unwrap(),
        );
        db.write(initial).await.unwrap();
        db.flush().await.unwrap();
        let engine = Engine {
            db,
            schema_lock: Arc::new(Mutex::new(())),
            query_slots: Arc::new(Semaphore::new(1)),
            local_lock: None,
            cache_lock: None,
        };
        let mut pending = WriteBatch::new();
        pending.put(
            document_key("demo", "a"),
            serde_json::to_vec(&make_doc("a", vec![-1., 0.])).unwrap(),
        );
        pending.delete(document_key("demo", "b"));
        let handle = engine.db.write(pending).await.unwrap();
        assert_eq!(engine.get("demo", "a").await.unwrap(), old);
        assert!(engine.get("demo", "b").await.is_ok());
        let request = QueryRequest {
            vector: vec![1., 0.],
            top_k: 2,
            filter: Filter::default(),
        };
        let before = engine.query("demo", request.clone()).await.unwrap();
        assert_eq!(before.matches.len(), 2);
        assert_eq!(before.matches[0].score, 1.);
        engine.db.flush().await.unwrap();
        handle.await_durable().await.unwrap();
        let after = engine.query("demo", request).await.unwrap();
        assert_eq!(after.matches.len(), 1);
        assert_eq!(after.matches[0].score, -1.);
        assert!(matches!(
            engine.get("demo", "b").await,
            Err(Error::NotFound(_))
        ));
        engine.close().await.unwrap();
    }
}
