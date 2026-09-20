//! Immutable index generations. The engine publishes their pointer only after
//! every block is durable; SlateDB snapshots protect retired generations.
use crate::{
    Error, Result, binary, config::IndexConfig, engine::scan_options, model::*,
    search::distance_score,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::DbSnapshot;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type Entries = Vec<(String, Vec<u8>)>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexStatus {
    pub generation: String,
    pub revision: u64,
    pub documents: usize,
    pub vectors: usize,
    pub centroids: Vec<Vec<f32>>,
    pub cluster_blocks: Vec<usize>,
    pub text: BTreeMap<String, Corpus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Corpus {
    pub documents: u64,
    pub tokens: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Terms {
    pub length: u32,
    pub frequencies: BTreeMap<String, u32>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Posting {
    pub id: String,
    pub frequency: u32,
    pub length: u32,
}

pub(crate) fn digest(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}
fn equality_key(value: &serde_json::Value) -> Result<String> {
    fn canonicalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Number(number)
                if number.is_f64() && number.as_f64() == Some(0.0) =>
            {
                *value = serde_json::json!(0.0);
            }
            serde_json::Value::Array(values) => values.iter_mut().for_each(canonicalize),
            serde_json::Value::Object(values) => values.values_mut().for_each(canonicalize),
            _ => {}
        }
    }
    let mut value = value.clone();
    canonicalize(&mut value);
    Ok(digest(&serde_json::to_vec(&value)?))
}
pub(crate) fn root(namespace: &str, generation: &str) -> String {
    format!("g/{namespace}/{generation}/")
}
pub(crate) fn number_key(value: f64) -> String {
    let bits = if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    };
    format!(
        "{:016x}",
        if bits >> 63 == 1 {
            !bits
        } else {
            bits ^ (1 << 63)
        }
    )
}

/// Unicode alphanumeric tokenization, lowercase, no stemming/stopword removal.
pub(crate) fn tokenize(text: &str) -> Terms {
    let mut result = Terms::default();
    for term in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
    {
        *result.frequencies.entry(term.to_lowercase()).or_default() += 1;
        result.length += 1;
    }
    result
}

pub(crate) fn document_terms(document: &Document, fields: &[String]) -> BTreeMap<String, Terms> {
    fields
        .iter()
        .filter_map(|field| {
            document
                .attributes
                .get(field)?
                .as_str()
                .map(|text| (field.clone(), tokenize(text)))
        })
        .collect()
}

pub(crate) fn build(
    namespace: &str,
    schema: &NamespaceConfig,
    revision: u64,
    documents: Vec<Document>,
    config: &IndexConfig,
) -> Result<(IndexStatus, Entries)> {
    let mut status = IndexStatus {
        generation: uuid::Uuid::new_v4().simple().to_string(),
        revision,
        documents: documents.len(),
        ..Default::default()
    };
    let prefix = root(namespace, &status.generation);
    let vectors: Vec<_> = documents
        .iter()
        .filter_map(|doc| doc.vector.as_ref().map(|v| (doc.id.clone(), v.clone())))
        .collect();
    status.vectors = vectors.len();
    let training: Vec<Vec<f32>> = vectors
        .iter()
        .map(|(_, v)| {
            let mut v = v.clone();
            if schema.metric == Metric::Cosine {
                normalize(&mut v);
            }
            v
        })
        .collect();
    let count = config.clusters.min(vectors.len());
    if count > 0 {
        // Deterministic spread across ID order; Lloyd refinement follows.
        status.centroids = (0..count)
            .map(|i| training[i * training.len() / count].clone())
            .collect();
        for _ in 0..config.iterations {
            let mut sums = vec![vec![0.0f64; training[0].len()]; count];
            let mut sizes = vec![0usize; count];
            for vector in &training {
                let cluster = nearest(vector, &status.centroids);
                sizes[cluster] += 1;
                for (sum, value) in sums[cluster].iter_mut().zip(vector) {
                    *sum += f64::from(*value);
                }
            }
            for i in 0..count {
                if sizes[i] > 0 {
                    status.centroids[i] = sums[i]
                        .iter()
                        .map(|v| (v / sizes[i] as f64) as f32)
                        .collect();
                    if schema.metric == Metric::Cosine {
                        normalize(&mut status.centroids[i]);
                    }
                }
            }
        }
    }
    let mut clusters = vec![Vec::new(); count];
    for (row, vector) in vectors.into_iter().zip(&training) {
        clusters[nearest(vector, &status.centroids)].push(row);
    }
    let mut entries = Vec::new();
    for (cluster, rows) in clusters.iter().enumerate() {
        let per_block = (256 * 1024 / (schema.dimensions.unwrap_or(1) * 4 + 130)).clamp(1, 128);
        status.cluster_blocks.push(rows.len().div_ceil(per_block));
        for (block, rows) in rows.chunks(per_block).enumerate() {
            entries.push((
                format!("{prefix}v/{cluster:04}/{block:08}"),
                binary::encode(rows),
            ));
        }
    }
    let mut attributes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut text: BTreeMap<String, Vec<Posting>> = BTreeMap::new();
    for document in documents {
        for (field, value) in &document.attributes {
            let field = digest(field.as_bytes());
            let key = format!("{prefix}e/{field}/{}", equality_key(value)?);
            attributes.entry(key).or_default().push(document.id.clone());
            if let Some(value) = value.as_f64().filter(|v| v.is_finite()) {
                attributes
                    .entry(format!("{prefix}r/{field}/{}", number_key(value)))
                    .or_default()
                    .push(document.id.clone());
            }
        }
        let terms = document_terms(&document, &schema.text_fields);
        for (field, terms) in &terms {
            let corpus = status.text.entry(field.clone()).or_default();
            corpus.documents += 1;
            corpus.tokens += u64::from(terms.length);
            for (term, frequency) in &terms.frequencies {
                text.entry(format!(
                    "{prefix}t/{}/{}",
                    digest(field.as_bytes()),
                    digest(term.as_bytes())
                ))
                .or_default()
                .push(Posting {
                    id: document.id.clone(),
                    frequency: *frequency,
                    length: terms.length,
                });
            }
        }
        if !terms.is_empty() {
            entries.push((
                format!("{prefix}l/{}", document.id),
                serde_json::to_vec(&terms)?,
            ));
        }
    }
    for (key, ids) in attributes {
        append_blocks(&mut entries, &key, &ids)?;
    }
    for (key, postings) in text {
        append_blocks(&mut entries, &key, &postings)?;
    }
    if entries
        .iter()
        .map(|(k, v)| k.len() + v.len())
        .sum::<usize>()
        > config.max_build_bytes
    {
        return Err(Error::Limit(
            "index output exceeds index.max_build_bytes".into(),
        ));
    }
    Ok((status, entries))
}

fn append_blocks<T: Serialize>(entries: &mut Entries, key: &str, rows: &[T]) -> Result<()> {
    for (block, rows) in rows.chunks(256).enumerate() {
        entries.push((format!("{key}/{block:08}"), serde_json::to_vec(rows)?));
    }
    Ok(())
}
fn normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|v| f64::from(*v).powi(2))
        .sum::<f64>()
        .sqrt();
    if norm > 0.0 {
        for v in vector {
            *v = (f64::from(*v) / norm) as f32;
        }
    }
}
fn nearest(vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    centroids
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            distance_score(vector, a, Metric::SquaredEuclidean).total_cmp(&distance_score(
                vector,
                b,
                Metric::SquaredEuclidean,
            ))
        })
        .map_or(0, |(i, _)| i)
}

pub(crate) async fn postings<T: serde::de::DeserializeOwned>(
    snapshot: &DbSnapshot,
    prefix: &str,
) -> Result<Vec<T>> {
    let mut iter = snapshot
        .scan_prefix_with_options(prefix, .., &scan_options())
        .await?;
    let mut items = Vec::new();
    while let Some(row) = iter.next().await? {
        items.extend(serde_json::from_slice::<Vec<T>>(&row.value)?);
    }
    Ok(items)
}

pub(crate) async fn filter_ids(
    snapshot: &DbSnapshot,
    prefix: &str,
    filter: &Filter,
) -> Result<Option<BTreeSet<String>>> {
    let mut result: Option<BTreeSet<String>> = None;
    for (field, value) in &filter.eq {
        let ids: BTreeSet<String> = postings(
            snapshot,
            &format!(
                "{prefix}e/{}/{}/",
                digest(field.as_bytes()),
                equality_key(value)?
            ),
        )
        .await?
        .into_iter()
        .collect();
        intersect(&mut result, ids);
    }
    for (field, bounds) in &filter.range {
        let key = format!("{prefix}r/{}/", digest(field.as_bytes()));
        let low = bounds.gte.map(number_key).unwrap_or_default();
        let high = bounds
            .lte
            .map(|n| format!("{}~", number_key(n)))
            .unwrap_or_else(|| "~".into());
        let mut iter = snapshot
            .scan_prefix_with_options(&key, low.as_bytes()..high.as_bytes(), &scan_options())
            .await?;
        let mut ids = BTreeSet::new();
        while let Some(row) = iter.next().await? {
            ids.extend(serde_json::from_slice::<Vec<String>>(&row.value)?);
        }
        intersect(&mut result, ids);
    }
    Ok(result)
}
fn intersect(result: &mut Option<BTreeSet<String>>, ids: BTreeSet<String>) {
    match result {
        Some(existing) => existing.retain(|id| ids.contains(id)),
        None => *result = Some(ids),
    }
}
