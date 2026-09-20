use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    ops::Bound,
    sync::Arc,
    time::Duration,
};

use serde::de::DeserializeOwned;
use slatedb::{
    Db, DbIterator, DbSnapshot, Settings, WriteBatch,
    config::{DurabilityLevel, ReadOptions, ScanOptions},
    object_store::{ObjectStore, aws::AmazonS3ConfigKey, local::LocalFileSystem},
};
use tokio::sync::{Mutex, Semaphore};

use crate::{
    Error, Result,
    config::{Config, StorageConfig},
    model::*,
};

pub(crate) const FORMAT_KEY: &str = "meta/format";
pub(crate) const FORMAT: &[u8] = b"gengis-mimi:2";

/// One writer database containing isolated namespace key ranges.
/// Call `close()` after draining requests to flush and stop background work.
#[derive(Clone)]
pub struct Engine {
    pub(crate) db: Db,
    pub(crate) database_identity: String,
    pub(crate) config: Arc<Config>,
    pub(crate) metrics: Arc<slatedb_common::metrics::DefaultMetricsRecorder>,
    index_lock: Arc<Mutex<()>>,
    pub(crate) http_metrics: Arc<crate::metrics::HttpMetrics>,
    pub(crate) schema_lock: Arc<Mutex<()>>,
    pub(crate) query_slots: Arc<Semaphore>,
    local_lock: Option<Arc<File>>,
    cache_lock: Option<Arc<File>>,
}

impl Engine {
    pub async fn open(config: &Config) -> Result<Self> {
        Self::open_inner(config, None).await
    }

    /// Inject a store for deterministic failure testing or embedded deployments.
    pub async fn open_with_store(config: &Config, store: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::open_inner(config, Some(store)).await
    }

    async fn open_inner(config: &Config, injected: Option<Arc<dyn ObjectStore>>) -> Result<Self> {
        config.validate()?;
        let mut settings = Settings {
            flush_interval: Some(Duration::from_millis(config.database.wal_flush_ms)),
            l0_sst_size_bytes: config.database.l0_sst_bytes,
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
            StorageConfig::S3 { bucket, region, .. } => {
                let builder = config.storage.s3_builder()?;
                let effective_endpoint = builder
                    .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
                    .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint));
                let identity = serde_json::json!({"type":"s3", "bucket":bucket, "region":region, "endpoint":effective_endpoint});
                (Arc::new(builder.build()?), None, identity)
            }
        };
        let database_identity = crate::index::digest(&serde_json::to_vec(&(
            &storage_identity,
            &config.database.prefix,
        ))?);
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
        let metrics = Arc::new(slatedb_common::metrics::DefaultMetricsRecorder::new());
        let store = Arc::new(crate::metrics::MeasuredStore::new(
            injected.unwrap_or(store),
            metrics.as_ref(),
        ));
        let db = Db::builder(config.database.prefix.clone(), store)
            .with_metrics_recorder(metrics.clone())
            .with_settings(settings)
            .build()
            .await?;
        let engine = Self {
            db,
            database_identity,
            http_metrics: Arc::new(crate::metrics::HttpMetrics::new(metrics.as_ref())),
            metrics,
            config: Arc::new(config.clone()),
            index_lock: Arc::new(Mutex::new(())),
            schema_lock: Arc::new(Mutex::new(())),
            query_slots: Arc::new(Semaphore::new(config.server.max_concurrent_queries)),
            local_lock: local_lock.map(Arc::new),
            cache_lock: cache_lock.map(Arc::new),
        };
        if let Err(error) = engine.initialize_format().await {
            let _ = engine.close().await;
            return Err(error);
        }
        let mut deletes = engine
            .db
            .scan_prefix_with_options("z/", .., &scan_options())
            .await?;
        while let Some(row) = deletes.next().await? {
            let name = std::str::from_utf8(&row.key[2..])
                .map_err(|_| Error::Corrupt("invalid deletion marker".into()))?;
            engine.erase_namespace(name).await?;
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
            Some(value)
                if (value.as_ref() == b"gengis-mimi:1"
                    || value.as_ref() == b"gengis-mimi:migrating:1:2")
                    && self.config.database.migrate =>
            {
                self.migrate_v1().await
            }
            Some(_) => Err(Error::Config(
                "unsupported storage format; use the migrate command for a v1 database".into(),
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
        let _index = self.index_lock.lock().await;
        let _guard = self.schema_lock.lock().await;
        let result = self.db.close().await;
        if let Some(lock) = &self.local_lock {
            lock.unlock()?;
        }
        if let Some(lock) = &self.cache_lock {
            lock.unlock()?;
        }
        result.map_err(Into::into)
    }

    pub async fn check_ready(&self) -> Result<()> {
        if self.db.status().close_reason.is_some() {
            return Err(Error::Unavailable);
        }
        match self
            .db
            .get_with_options(FORMAT_KEY, &read_options())
            .await?
        {
            Some(value) if value.as_ref() == FORMAT => Ok(()),
            _ => Err(Error::Unavailable),
        }
    }

    pub(crate) async fn snapshot(&self) -> Result<Arc<DbSnapshot>> {
        // All application commits retain this guard through durability, including
        // when the caller disconnects. Thus multi-get queries pin one durable view.
        let _guard = self.schema_lock.lock().await;
        self.check_ready().await?;
        Ok(self.db.snapshot().await?)
    }

    pub async fn create_namespace(
        &self,
        name: String,
        config: NamespaceConfig,
    ) -> Result<Namespace> {
        validate_id(&name)?;
        config.validate()?;
        let engine = self.clone();
        let guard = self.schema_lock.clone().lock_owned().await;
        tokio::spawn(async move {
            let _guard = guard;
            engine.check_ready().await?;
            let key = namespace_key(&name);
            if let Some(value) = engine.db.get_with_options(&key, &read_options()).await? {
                let existing: Namespace = serde_json::from_slice(&value)?;
                if existing.config != config {
                    return Err(Error::Conflict(
                        "namespace configuration is immutable".into(),
                    ));
                }
                return Ok(existing);
            }
            let namespace = Namespace { name, config };
            engine
                .db
                .put(key, serde_json::to_vec(&namespace)?)
                .await?
                .await_durable()
                .await?;
            Ok(namespace)
        })
        .await?
    }

    pub async fn namespace(&self, name: &str) -> Result<Namespace> {
        namespace_at(self.snapshot().await?.as_ref(), name).await
    }

    pub async fn namespaces(&self, page: &PageRequest) -> Result<Page<Namespace>> {
        page.validate()?;
        let snapshot = self.snapshot().await?;
        let iter = page_iterator(&snapshot, "n/", page).await?;
        collect_page(iter, page.limit, |namespace: &Namespace| &namespace.name).await
    }

    pub async fn stats(&self, name: &str) -> Result<NamespaceStats> {
        let snapshot = self.snapshot().await?;
        namespace_at(&snapshot, name).await?;
        stats_at(&snapshot, name).await
    }

    /// One durable batch contains attributes, binary vectors, quotas, and changes.
    pub async fn write(&self, name: &str, request: WriteRequest) -> Result<WriteResult> {
        validate_id(name)?;
        let count = request.upsert.len() + request.delete.len();
        if !(1..=MAX_BATCH_OPERATIONS).contains(&count) {
            return Err(Error::Invalid("batch requires 1–1000 operations".into()));
        }
        let mut seen = HashSet::new();
        for id in request.upsert.iter().map(|d| &d.id).chain(&request.delete) {
            validate_id(id)?;
            if !seen.insert(id) {
                return Err(Error::Invalid(
                    "a document ID may occur only once per batch".into(),
                ));
            }
        }
        let name = name.to_owned();
        let engine = self.clone();
        let guard = self.schema_lock.clone().lock_owned().await;
        tokio::spawn(async move {
            let _guard = guard;
            engine.check_ready().await?;
            let snapshot = engine.db.snapshot().await?;
            let schema = namespace_at(&snapshot, &name).await?.config;
            let mut stats = stats_at(&snapshot, &name).await?;
            stats.revision = stats
                .revision
                .checked_add(1)
                .ok_or_else(|| Error::Limit("revision exhausted".into()))?;
            let mut batch = WriteBatch::new();
            let mut input_bytes = 0;
            let upserted = request.upsert.len();
            let deleted = request.delete.len();
            for (id, new) in request
                .upsert
                .into_iter()
                .map(|d| (d.id.clone(), Some(d)))
                .chain(request.delete.into_iter().map(|id| (id, None)))
            {
                let key = document_key(&name, &id);
                let vector_key = vector_key(&name, &id);
                let old = snapshot.get_with_options(&key, &read_options()).await?;
                let old_vector = snapshot
                    .get_with_options(&vector_key, &read_options())
                    .await?;
                if let Some(old) = old {
                    stats.documents -= 1;
                    stats.bytes -= (old.len() + old_vector.as_ref().map_or(0, |v| v.len())) as u64;
                }
                if let Some(mut document) = new {
                    let serialized_size = serde_json::to_vec(&document)?.len();
                    if serialized_size > MAX_DOCUMENT_BYTES {
                        return Err(Error::Invalid("serialized document exceeds 256 KiB".into()));
                    }
                    if document.attributes.len() > 64 {
                        return Err(Error::Invalid("at most 64 attributes per document".into()));
                    }
                    input_bytes += serialized_size;
                    let vector = document.vector.take();
                    if let Some(vector) = vector {
                        schema.validate_vector(&vector)?;
                        let encoded = crate::binary::encode_vector(&vector);
                        stats.bytes += encoded.len() as u64;
                        batch.put(&vector_key, encoded);
                    } else {
                        batch.delete(&vector_key);
                    }
                    let encoded = serde_json::to_vec(&document)?;
                    stats.bytes += encoded.len() as u64;
                    stats.documents += 1;
                    batch.put(&key, encoded);
                } else {
                    batch.delete(&key);
                    batch.delete(&vector_key);
                    input_bytes += id.len();
                }
                let change = format!("c/{name}/{id}");
                if snapshot
                    .get_with_options(&change, &read_options())
                    .await?
                    .is_none()
                {
                    stats.pending_documents += 1;
                }
                batch.put(change, stats.revision.to_le_bytes());
            }
            if input_bytes > MAX_BODY_BYTES {
                return Err(Error::Invalid("batch exceeds 8 MiB".into()));
            }
            if stats.documents > engine.config.limits.max_documents
                || stats.bytes > engine.config.limits.max_namespace_bytes
                || stats.pending_documents > engine.config.limits.max_pending_documents
            {
                return Err(Error::Limit(
                    "namespace document, byte, or pending-index quota exceeded".into(),
                ));
            }
            batch.put(format!("s/{name}"), serde_json::to_vec(&stats)?);
            let handle = engine.db.write(batch).await?;
            handle.await_durable().await?;
            Ok(WriteResult {
                upserted,
                deleted,
                sequence: handle.seqnum(),
                revision: stats.revision,
            })
        })
        .await?
    }

    pub async fn get(&self, name: &str, id: &str) -> Result<Document> {
        validate_id(id)?;
        let snapshot = self.snapshot().await?;
        namespace_at(&snapshot, name).await?;
        document_at(&snapshot, name, id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("document '{id}' not found")))
    }

    pub async fn documents(&self, name: &str, page: &PageRequest) -> Result<Page<Document>> {
        page.validate()?;
        let snapshot = self.snapshot().await?;
        namespace_at(&snapshot, name).await?;
        let iter = page_iterator(&snapshot, &document_prefix(name), page).await?;
        let mut page = collect_page(iter, page.limit, |d: &Document| &d.id).await?;
        for document in &mut page.items {
            document.vector = vector_at(&snapshot, name, &document.id).await?;
        }
        Ok(page)
    }

    pub async fn query(&self, name: &str, request: QueryRequest) -> Result<QueryResult> {
        let permit = Arc::new(
            self.query_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Busy)?,
        );
        let snapshot = self.snapshot().await?;
        let schema = namespace_at(&snapshot, name).await?.config;
        schema.validate_vector(&request.vector)?;
        request.filter.validate()?;
        if !(1..=MAX_TOP_K).contains(&request.top_k) || !(1..=1024).contains(&request.probes) {
            return Err(Error::Invalid(
                "top_k must be 1..100 and probes 1..1024".into(),
            ));
        }
        crate::search::vector_query(
            &snapshot,
            name,
            &schema,
            request,
            &self.config.index,
            permit,
        )
        .await
    }

    pub async fn search_text(&self, name: &str, request: TextQuery) -> Result<QueryResult> {
        let _permit = self
            .query_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let snapshot = self.snapshot().await?;
        let schema = namespace_at(&snapshot, name).await?.config;
        request.filter.validate()?;
        if !schema.text_fields.contains(&request.field) {
            return Err(Error::Invalid(
                "field is not in namespace text_fields".into(),
            ));
        }
        if !(1..=MAX_TOP_K).contains(&request.top_k) || request.text.len() > 4096 {
            return Err(Error::Invalid(
                "top_k must be 1..100 and text at most 4096 bytes".into(),
            ));
        }
        crate::search::text_query(&snapshot, name, &schema, request).await
    }

    pub async fn index_status(&self, name: &str) -> Result<Option<crate::index::IndexStatus>> {
        let snapshot = self.snapshot().await?;
        namespace_at(&snapshot, name).await?;
        index_at(&snapshot, name).await
    }

    pub async fn rebuild_index(&self, name: &str) -> Result<crate::index::IndexStatus> {
        validate_id(name)?;
        let guard = self.index_lock.clone().lock_owned().await;
        let engine = self.clone();
        let name = name.to_owned();
        tokio::spawn(async move {
            let _guard = guard;
            engine.build_index_inner(&name).await
        })
        .await?
    }

    async fn build_index_inner(&self, name: &str) -> Result<crate::index::IndexStatus> {
        let snapshot = self.snapshot().await?;
        let schema = namespace_at(&snapshot, name).await?.config;
        let stats = stats_at(&snapshot, name).await?;
        let mut iter = snapshot
            .scan_prefix_with_options(document_prefix(name), .., &scan_options())
            .await?;
        let mut documents = Vec::new();
        let mut bytes = 0;
        while let Some(row) = iter.next().await? {
            let mut document: Document = serde_json::from_slice(&row.value)?;
            document.vector = vector_at(&snapshot, name, &document.id).await?;
            // Reserve headroom for training copies, postings maps and output.
            bytes += row.value.len() + document.vector.as_ref().map_or(0, |v| v.len() * 4) + 256;
            if bytes > self.config.index.max_build_bytes / 8 {
                return Err(Error::Limit("index input exceeds build memory budget; raise index.max_build_bytes or partition data".into()));
            }
            documents.push(document);
        }
        let build_name = name.to_owned();
        let config = self.config.index.clone();
        let (status, entries) = tokio::task::spawn_blocking(move || {
            crate::index::build(&build_name, &schema, stats.revision, documents, &config)
        })
        .await??;
        self.db
            .put(format!("j/{name}"), b"cleanup")
            .await?
            .await_durable()
            .await?;
        let mut batch = WriteBatch::new();
        let mut bytes = 0;
        for (key, value) in entries {
            bytes += key.len() + value.len();
            batch.put(key, value);
            if bytes >= 1024 * 1024 {
                self.db.write(batch).await?.await_durable().await?;
                batch = WriteBatch::new();
                bytes = 0;
            }
        }
        if bytes > 0 {
            self.db.write(batch).await?.await_durable().await?;
        }
        {
            let _guard = self.schema_lock.lock().await;
            // New writes remain in c/ and are overlaid by every query.
            self.db
                .put(format!("i/{name}"), serde_json::to_vec(&status)?)
                .await?
                .await_durable()
                .await?;
        }
        drop(snapshot);
        self.cleanup_inner(name, &status).await?;
        self.db
            .delete(format!("j/{name}"))
            .await?
            .await_durable()
            .await?;
        Ok(status)
    }

    async fn cleanup_inner(&self, name: &str, active: &crate::index::IndexStatus) -> Result<()> {
        // Retire keys with SlateDB tombstones: concurrent snapshots retain their
        // old values. Never directly delete SST/object files from the application.
        let snapshot = self.snapshot().await?;
        let mut iter = snapshot
            .scan_prefix_with_options(format!("g/{name}/"), .., &scan_options())
            .await?;
        let retained = crate::index::root(name, &active.generation);
        let mut batch = WriteBatch::new();
        let mut count = 0;
        while let Some(row) = iter.next().await? {
            if !row.key.starts_with(retained.as_bytes()) {
                batch.delete(row.key);
                count += 1;
            }
            if count == 1000 {
                self.db.write(batch).await?.await_durable().await?;
                batch = WriteBatch::new();
                count = 0;
            }
        }
        if count > 0 {
            self.db.write(batch).await?.await_durable().await?;
        }
        let mut iter = snapshot
            .scan_prefix_with_options(format!("c/{name}/"), .., &scan_options())
            .await?;
        let mut keys = Vec::new();
        while let Some(row) = iter.next().await? {
            if revision(&row.value)? <= active.revision {
                keys.push(row.key);
            }
        }
        for keys in keys.chunks(1000) {
            let _guard = self.schema_lock.lock().await;
            let current = self.db.snapshot().await?;
            let mut stats = stats_at(&current, name).await?;
            let mut batch = WriteBatch::new();
            for key in keys {
                if let Some(value) = current.get_with_options(key, &read_options()).await?
                    && revision(&value)? <= active.revision
                {
                    batch.delete(key);
                    stats.pending_documents -= 1;
                }
            }
            batch.put(format!("s/{name}"), serde_json::to_vec(&stats)?);
            self.db.write(batch).await?.await_durable().await?;
        }
        Ok(())
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        validate_id(name)?;
        let engine = self.clone();
        let name = name.to_owned();
        let index_guard = self.index_lock.clone().lock_owned().await;
        let guard = self.schema_lock.clone().lock_owned().await;
        tokio::spawn(async move {
            let (_index_guard, _guard) = (index_guard, guard);
            engine.check_ready().await?;
            // Durable deletion intent hides the namespace immediately and is
            // resumed on reopen before accepting requests.
            let mut batch = WriteBatch::new();
            batch.delete(namespace_key(&name));
            batch.put(format!("z/{name}"), b"delete");
            engine.db.write(batch).await?.await_durable().await?;
            engine.erase_namespace(&name).await
        })
        .await?
    }

    async fn erase_namespace(&self, name: &str) -> Result<()> {
        for prefix in [
            format!("d/{name}/"),
            format!("v/{name}/"),
            format!("c/{name}/"),
            format!("g/{name}/"),
        ] {
            let mut iter = self
                .db
                .scan_prefix_with_options(prefix, .., &scan_options())
                .await?;
            let mut batch = WriteBatch::new();
            let mut count = 0;
            while let Some(row) = iter.next().await? {
                batch.delete(row.key);
                count += 1;
                if count == 1000 {
                    self.db.write(batch).await?.await_durable().await?;
                    batch = WriteBatch::new();
                    count = 0;
                }
            }
            if count > 0 {
                self.db.write(batch).await?.await_durable().await?;
            }
        }
        let mut batch = WriteBatch::new();
        for key in [
            format!("i/{name}"),
            format!("s/{name}"),
            format!("z/{name}"),
            format!("j/{name}"),
        ] {
            batch.delete(key);
        }
        self.db.write(batch).await?.await_durable().await?;
        Ok(())
    }

    pub async fn run_indexer(&self, mut stop: tokio::sync::watch::Receiver<bool>) {
        if self.config.index.interval_ms == 0 {
            return;
        }
        let mut timer = tokio::time::interval(Duration::from_millis(self.config.index.interval_ms));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stop.changed() => return, _ = timer.tick() => {} }
            let mut page = PageRequest::default();
            loop {
                let namespaces = match self.namespaces(&page).await {
                    Ok(v) => v,
                    Err(error) => {
                        tracing::error!(%error, "indexer could not list namespaces");
                        break;
                    }
                };
                for namespace in namespaces.items {
                    if *stop.borrow() {
                        return;
                    }
                    if self
                        .stats(&namespace.name)
                        .await
                        .is_ok_and(|s| s.pending_documents > 0)
                    {
                        if let Err(error) = self.rebuild_index(&namespace.name).await {
                            tracing::error!(namespace = namespace.name, %error, "index build failed");
                        }
                    } else if let Err(error) = self.resume_index_cleanup(&namespace.name).await {
                        tracing::error!(namespace = namespace.name, %error, "index cleanup failed");
                    }
                }
                match namespaces.next_cursor {
                    Some(cursor) => page.after = Some(cursor),
                    None => break,
                }
            }
        }
    }

    async fn resume_index_cleanup(&self, name: &str) -> Result<()> {
        let _guard = self.index_lock.lock().await;
        if self
            .db
            .get_with_options(format!("j/{name}"), &read_options())
            .await?
            .is_some()
        {
            let snapshot = self.snapshot().await?;
            let active = index_at(&snapshot, name).await?.unwrap_or_default();
            self.cleanup_inner(name, &active).await?;
            self.db
                .delete(format!("j/{name}"))
                .await?
                .await_durable()
                .await?;
        }
        Ok(())
    }
}

pub(crate) fn namespace_key(name: &str) -> String {
    format!("n/{name}")
}
pub(crate) fn document_prefix(name: &str) -> String {
    format!("d/{name}/")
}
pub(crate) fn document_key(name: &str, id: &str) -> String {
    format!("d/{name}/{id}")
}
pub(crate) fn vector_key(name: &str, id: &str) -> String {
    format!("v/{name}/{id}")
}
pub(crate) fn read_options() -> ReadOptions {
    ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
}
pub(crate) fn scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    }
}
pub(crate) fn revision(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| Error::Corrupt("invalid revision".into()))?,
    ))
}
pub(crate) async fn namespace_at(snapshot: &DbSnapshot, name: &str) -> Result<Namespace> {
    validate_id(name)?;
    let value = snapshot
        .get_with_options(namespace_key(name), &read_options())
        .await?
        .ok_or_else(|| Error::NotFound(format!("namespace '{name}' not found")))?;
    Ok(serde_json::from_slice(&value)?)
}
pub(crate) async fn stats_at(snapshot: &DbSnapshot, name: &str) -> Result<NamespaceStats> {
    snapshot
        .get_with_options(format!("s/{name}"), &read_options())
        .await?
        .map(|v| serde_json::from_slice(&v))
        .transpose()
        .map(|v| v.unwrap_or_default())
        .map_err(Into::into)
}
pub(crate) async fn index_at(
    snapshot: &DbSnapshot,
    name: &str,
) -> Result<Option<crate::index::IndexStatus>> {
    snapshot
        .get_with_options(format!("i/{name}"), &read_options())
        .await?
        .map(|v| serde_json::from_slice(&v))
        .transpose()
        .map_err(Into::into)
}
pub(crate) async fn vector_at(
    snapshot: &DbSnapshot,
    name: &str,
    id: &str,
) -> Result<Option<Vec<f32>>> {
    snapshot
        .get_with_options(vector_key(name, id), &read_options())
        .await?
        .map(|v| crate::binary::decode_vector(&v))
        .transpose()
}
pub(crate) async fn attributes_at(
    snapshot: &DbSnapshot,
    name: &str,
    id: &str,
) -> Result<Option<Document>> {
    snapshot
        .get_with_options(document_key(name, id), &read_options())
        .await?
        .map(|v| serde_json::from_slice(&v))
        .transpose()
        .map_err(Into::into)
}
pub(crate) async fn document_at(
    snapshot: &DbSnapshot,
    name: &str,
    id: &str,
) -> Result<Option<Document>> {
    let mut doc = attributes_at(snapshot, name, id).await?;
    if let Some(doc) = &mut doc {
        doc.vector = vector_at(snapshot, name, id).await?;
    }
    Ok(doc)
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
