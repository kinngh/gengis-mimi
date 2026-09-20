//! Snapshot exports with a SHA-256 footer. Restore validates the entire file
//! before writing to an empty database. Incomplete restores stay unavailable.
use crate::{Engine, Error, Result, engine::*, model::*};
use futures_util::{StreamExt, stream, stream::BoxStream};
use sha2::{Digest, Sha256};
use slatedb::{WriteBatch, bytes::Bytes};
use std::{
    fs::File,
    io::{Read, Seek, Write},
    path::Path,
};
const HEADER: &[u8; 8] = b"GMBAK\x02\r\n";
const MAX_RECORD: usize = 16 * 1024 * 1024;

impl Engine {
    pub async fn export(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        let permit = self
            .query_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        let snapshot = self.snapshot().await?;
        let iter = snapshot.scan_with_options(.., &scan_options()).await?;
        let mut hash = Sha256::new();
        hash.update(HEADER);
        let records = stream::try_unfold(
            (snapshot, iter, hash, 0u64, false, permit),
            |(snapshot, mut iter, mut hash, count, done, permit)| async move {
                if done {
                    return Ok(None);
                }
                if let Some(row) = iter.next().await? {
                    let mut bytes = Vec::with_capacity(8 + row.key.len() + row.value.len());
                    bytes.extend_from_slice(&(row.key.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(&(row.value.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(&row.key);
                    bytes.extend_from_slice(&row.value);
                    hash.update(&bytes);
                    Ok(Some((
                        Bytes::from(bytes),
                        (snapshot, iter, hash, count + 1, false, permit),
                    )))
                } else {
                    let mut footer = u32::MAX.to_le_bytes().to_vec();
                    footer.extend_from_slice(&count.to_le_bytes());
                    footer.extend_from_slice(&hash.clone().finalize());
                    Ok(Some((
                        Bytes::from(footer),
                        (snapshot, iter, hash, count, true, permit),
                    )))
                }
            },
        );
        Ok(stream::once(async { Ok(Bytes::from_static(HEADER)) })
            .chain(records)
            .boxed())
    }

    pub async fn restore(&self, path: &Path) -> Result<u64> {
        let mut file = File::open(path)?;
        file.lock_shared()?;
        verify(&mut file)?;
        file.rewind()?;
        let _guard = self.schema_lock.lock().await;
        let mut existing = self.db.scan_with_options(.., &scan_options()).await?;
        while let Some(row) = existing.next().await? {
            if row.key.as_ref() != FORMAT_KEY.as_bytes() {
                return Err(Error::Conflict(
                    "restore requires an empty database prefix".into(),
                ));
            }
        }
        self.db
            .put(FORMAT_KEY, b"gengis-mimi:restoring:2")
            .await?
            .await_durable()
            .await?;
        let mut header = [0; 8];
        file.read_exact(&mut header)?;
        let mut batch = WriteBatch::new();
        let mut size = 0;
        let mut count = 0;
        while let Some((key, value)) = read_record(&mut file)? {
            if key.as_slice() != FORMAT_KEY.as_bytes() {
                size += key.len() + value.len();
                batch.put(key, value);
                count += 1;
                if size >= 1024 * 1024 {
                    self.db.write(batch).await?.await_durable().await?;
                    batch = WriteBatch::new();
                    size = 0;
                }
            }
        }
        batch.put(FORMAT_KEY, FORMAT);
        self.db.write(batch).await?.await_durable().await?;
        Ok(count)
    }

    pub(crate) async fn migrate_v1(&self) -> Result<()> {
        self.db
            .put(FORMAT_KEY, b"gengis-mimi:migrating:1:2")
            .await?
            .await_durable()
            .await?;
        let mut namespaces = self
            .db
            .scan_prefix_with_options("n/", .., &scan_options())
            .await?;
        while let Some(row) = namespaces.next().await? {
            let namespace: Namespace = serde_json::from_slice(&row.value)?;
            let name = namespace.name;
            let mut docs = self
                .db
                .scan_prefix_with_options(document_prefix(&name), .., &scan_options())
                .await?;
            let mut stats = NamespaceStats {
                revision: 1,
                ..Default::default()
            };
            let mut batch = WriteBatch::new();
            let mut count = 0;
            while let Some(row) = docs.next().await? {
                let mut doc: Document = serde_json::from_slice(&row.value)?;
                let key = vector_key(&name, &doc.id);
                if let Some(vector) = doc.vector.take() {
                    namespace.config.validate_vector(&vector)?;
                    let encoded = crate::binary::encode_vector(&vector);
                    stats.bytes += encoded.len() as u64;
                    batch.put(key, encoded);
                } else if let Some(encoded) = self.db.get_with_options(key, &read_options()).await?
                {
                    stats.bytes += encoded.len() as u64;
                }
                let encoded = serde_json::to_vec(&doc)?;
                stats.bytes += encoded.len() as u64;
                stats.documents += 1;
                stats.pending_documents += 1;
                batch.put(document_key(&name, &doc.id), encoded);
                batch.put(format!("c/{name}/{}", doc.id), 1u64.to_le_bytes());
                count += 1;
                if count == 128 {
                    self.db.write(batch).await?.await_durable().await?;
                    batch = WriteBatch::new();
                    count = 0;
                }
            }
            batch.put(format!("s/{name}"), serde_json::to_vec(&stats)?);
            self.db.write(batch).await?.await_durable().await?;
        }
        self.db
            .put(FORMAT_KEY, FORMAT)
            .await?
            .await_durable()
            .await?;
        Ok(())
    }
}

fn read_record(file: &mut File) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut size = [0; 4];
    file.read_exact(&mut size)?;
    let key_len = u32::from_le_bytes(size);
    if key_len == u32::MAX {
        return Ok(None);
    }
    file.read_exact(&mut size)?;
    let value_len = u32::from_le_bytes(size) as usize;
    if key_len == 0 || key_len > 2048 || value_len > MAX_RECORD {
        return Err(Error::Corrupt("invalid backup record length".into()));
    }
    let mut key = vec![0; key_len as usize];
    let mut value = vec![0; value_len];
    file.read_exact(&mut key)?;
    file.read_exact(&mut value)?;
    Ok(Some((key, value)))
}
fn verify(file: &mut File) -> Result<()> {
    let mut header = [0; 8];
    file.read_exact(&mut header)?;
    if &header != HEADER {
        return Err(Error::Corrupt("unsupported backup format".into()));
    }
    let mut hash = Sha256::new();
    hash.update(header);
    let mut count = 0u64;
    let mut format = false;
    let mut previous = Vec::new();
    while let Some((key, value)) = read_record(file)? {
        if key <= previous {
            return Err(Error::Corrupt(
                "backup keys must be strictly ordered".into(),
            ));
        }
        if key == FORMAT_KEY.as_bytes() {
            if value != FORMAT {
                return Err(Error::Corrupt(
                    "backup is not a complete v2 database".into(),
                ));
            }
            format = true;
        }
        hash.update((key.len() as u32).to_le_bytes());
        hash.update((value.len() as u32).to_le_bytes());
        hash.update(&key);
        hash.update(value);
        count += 1;
        previous = key;
    }
    let mut footer = [0; 40];
    file.read_exact(&mut footer)?;
    let mut trailing = [0];
    if !format
        || count
            != u64::from_le_bytes(
                footer[..8]
                    .try_into()
                    .map_err(|_| Error::Corrupt("invalid footer".into()))?,
            )
        || hash.finalize().as_slice() != &footer[8..]
        || file.read(&mut trailing)? != 0
    {
        return Err(Error::Corrupt(
            "backup count, checksum, or footer is invalid".into(),
        ));
    }
    Ok(())
}

/// Download a consistent online backup, verify it, and only then expose the
/// requested output name. Existing outputs are never overwritten.
pub async fn download(url: &str, output: &Path, token: Option<&str>) -> Result<()> {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(".gm-backup-{}.partial", uuid::Uuid::new_v4()));
    let result = async {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut request = client.get(format!("{}/v1/export", url.trim_end_matches('/')));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let mut response = request.send().await?.error_for_status()?;
        let mut file = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        while let Some(chunk) = response.chunk().await? {
            file.write_all(&chunk)?;
        }
        file.sync_all()?;
        file.rewind()?;
        verify(&mut file)?;
        std::fs::hard_link(&temporary, output)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
    .await;
    let _ = std::fs::remove_file(temporary);
    result
}
