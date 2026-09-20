//! Prometheus exposition of SlateDB I/O/cache/compaction metrics and API latency.
use crate::Engine;
use slatedb_common::metrics::{LATENCY_BOUNDARIES, MetricValue, MetricsRecorder};
use std::fmt::Write;
impl Engine {
    pub fn record_request(&self, status: u16, seconds: f64) {
        self.http_metrics.classes[(usize::from(status) / 100).clamp(1, 5) - 1].increment(1);
        self.http_metrics.duration.record(seconds);
    }
    pub fn prometheus(&self) -> String {
        let mut out = String::new();
        for metric in self.metrics.snapshot().all() {
            let name = metric.name.replace(['.', '-'], "_");
            let labels: Vec<_> = metric
                .labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
                .collect();
            let suffix = if labels.is_empty() {
                String::new()
            } else {
                format!("{{{}}}", labels.join(","))
            };
            match &metric.value {
                MetricValue::Counter(v) => {
                    let _ = writeln!(out, "{name}{suffix} {v}");
                }
                MetricValue::Gauge(v) => {
                    let _ = writeln!(out, "{name}{suffix} {v}");
                }
                MetricValue::UpDownCounter(v) => {
                    let _ = writeln!(out, "{name}{suffix} {v}");
                }
                MetricValue::Histogram {
                    count,
                    sum,
                    boundaries,
                    bucket_counts,
                    ..
                } => {
                    let _ = writeln!(
                        out,
                        "{name}_count{suffix} {count}\n{name}_sum{suffix} {sum}"
                    );
                    let mut total = 0;
                    for (i, n) in bucket_counts.iter().enumerate() {
                        total += n;
                        let bound = boundaries
                            .get(i)
                            .map_or_else(|| "+Inf".into(), ToString::to_string);
                        let mut labels = labels.clone();
                        labels.push(format!("le=\"{bound}\""));
                        let _ = writeln!(out, "{name}_bucket{{{}}} {total}", labels.join(","));
                    }
                }
            }
        }
        let _ = writeln!(out, "gm_durable_sequence {}", self.db.status().durable_seq);
        out
    }
}
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub(crate) struct HttpMetrics {
    classes: Vec<std::sync::Arc<dyn slatedb_common::metrics::CounterFn>>,
    duration: std::sync::Arc<dyn slatedb_common::metrics::HistogramFn>,
}
impl HttpMetrics {
    pub fn new(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            classes: (1..=5)
                .map(|i| {
                    recorder.register_counter(
                        "gm.http.requests",
                        "HTTP requests",
                        &[("status_class", &format!("{i}xx"))],
                    )
                })
                .collect(),
            duration: recorder.register_histogram(
                "gm.http.duration_seconds",
                "HTTP duration",
                &[],
                LATENCY_BOUNDARIES,
            ),
        }
    }
}

use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, stream::BoxStream};
use slatedb::{
    bytes::Bytes,
    object_store::{self, path::Path, *},
};
use slatedb_common::metrics::CounterFn;
use std::{fmt, ops::Range, sync::Arc};

/// Counts payload bytes below SlateDB caches. HTTP headers and retries internal
/// to a provider are outside this boundary; these are not network billing bytes.
pub(crate) struct MeasuredStore {
    inner: Arc<dyn ObjectStore>,
    read: Arc<dyn CounterFn>,
    written: Arc<dyn CounterFn>,
    calls: Arc<dyn CounterFn>,
}
impl MeasuredStore {
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: &dyn MetricsRecorder) -> Self {
        Self {
            inner,
            calls: metrics.register_counter(
                "gm.object_store.calls",
                "Raw object-store API calls below caches",
                &[],
            ),
            read: metrics.register_counter(
                "gm.object_store.read_bytes",
                "Payload bytes delivered by the object store",
                &[],
            ),
            written: metrics.register_counter(
                "gm.object_store.written_bytes",
                "Payload bytes accepted by the object store",
                &[],
            ),
        }
    }
}
impl fmt::Debug for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MeasuredStore")
    }
}
impl fmt::Display for MeasuredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeasuredStore({})", self.inner)
    }
}
#[async_trait]
impl ObjectStore for MeasuredStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.calls.increment(1);
        let size = payload.content_length() as u64;
        let result = self.inner.put_opts(path, payload, options).await?;
        self.written.increment(size);
        Ok(result)
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.calls.increment(1);
        Ok(Box::new(MeasuredUpload {
            inner: self.inner.put_multipart_opts(path, options).await?,
            written: self.written.clone(),
            calls: self.calls.clone(),
        }))
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.calls.increment(1);
        let result = self.inner.get_opts(path, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let count = self.read.clone();
        let payload = GetResultPayload::Stream(
            result
                .into_stream()
                .map(move |chunk| {
                    if let Ok(bytes) = &chunk {
                        count.increment(bytes.len() as u64);
                    }
                    chunk
                })
                .boxed(),
        );
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions,
        })
    }
    async fn get_ranges(
        &self,
        path: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.calls.increment(1);
        let result = self.inner.get_ranges(path, ranges).await?;
        self.read
            .increment(result.iter().map(|b| b.len() as u64).sum());
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.calls.increment(1);
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.calls.increment(1);
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.calls.increment(1);
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.calls.increment(1);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.calls.increment(1);
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.calls.increment(1);
        self.inner.rename_opts(from, to, options).await
    }
}
struct MeasuredUpload {
    inner: Box<dyn MultipartUpload>,
    written: Arc<dyn CounterFn>,
    calls: Arc<dyn CounterFn>,
}
impl fmt::Debug for MeasuredUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MeasuredUpload")
    }
}
#[async_trait]
impl MultipartUpload for MeasuredUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.calls.increment(1);
        let size = data.content_length() as u64;
        let count = self.written.clone();
        let upload = self.inner.put_part(data);
        async move {
            upload.await?;
            count.increment(size);
            Ok(())
        }
        .boxed()
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.calls.increment(1);
        self.inner.complete().await
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.calls.increment(1);
        self.inner.abort().await
    }
}
