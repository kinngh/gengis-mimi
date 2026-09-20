use crate::{
    Error, Result, binary,
    config::IndexConfig,
    engine::*,
    index::{self, Posting, Terms},
    model::*,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use slatedb::DbSnapshot;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    sync::Arc,
};
use tokio::sync::OwnedSemaphorePermit;

struct Ranked(SearchHit);
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then_with(|| self.0.id.cmp(&other.0.id))
    }
}
fn retain(best: &mut BinaryHeap<Ranked>, id: String, score: f64, k: usize) {
    let hit = Ranked(SearchHit {
        id,
        score,
        attributes: Default::default(),
    });
    if best.len() < k {
        best.push(hit);
    } else if best.peek().is_some_and(|worst| hit < *worst) {
        best.pop();
        best.push(hit);
    }
}
pub(crate) fn distance_score(a: &[f32], b: &[f32], metric: Metric) -> f64 {
    let dot = || {
        a.iter()
            .zip(b)
            .map(|(a, b)| f64::from(*a) * f64::from(*b))
            .sum::<f64>()
    };
    match metric {
        Metric::Dot => dot(),
        Metric::SquaredEuclidean => -a
            .iter()
            .zip(b)
            .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
            .sum::<f64>(),
        Metric::Cosine => {
            let norm = |v: &[f32]| v.iter().map(|v| f64::from(*v).powi(2)).sum::<f64>().sqrt();
            let denominator = norm(a) * norm(b);
            if denominator == 0.0 {
                -1.0
            } else {
                (dot() / denominator).clamp(-1.0, 1.0)
            }
        }
    }
}
async fn score_rows(
    rows: Vec<(String, Vec<f32>)>,
    mut best: BinaryHeap<Ranked>,
    query: &QueryRequest,
    metric: Metric,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<BinaryHeap<Ranked>> {
    let query = query.clone();
    Ok(tokio::task::spawn_blocking(move || {
        let _permit = permit;
        for (id, vector) in rows {
            retain(
                &mut best,
                id,
                distance_score(&query.vector, &vector, metric),
                query.top_k,
            );
        }
        best
    })
    .await?)
}
async fn hydrate(
    snapshot: &DbSnapshot,
    name: &str,
    best: BinaryHeap<Ranked>,
) -> Result<Vec<SearchHit>> {
    let mut hits = best.into_vec();
    hits.sort();
    let mut result = Vec::with_capacity(hits.len());
    for Ranked(mut hit) in hits {
        hit.attributes = attributes_at(snapshot, name, &hit.id)
            .await?
            .ok_or_else(|| Error::Corrupt("index references a missing document".into()))?
            .attributes;
        result.push(hit);
    }
    Ok(result)
}
async fn pending(snapshot: &DbSnapshot, name: &str, watermark: u64) -> Result<BTreeSet<String>> {
    let prefix = format!("c/{name}/");
    let mut iter = snapshot
        .scan_prefix_with_options(&prefix, .., &scan_options())
        .await?;
    let mut ids = BTreeSet::new();
    while let Some(row) = iter.next().await? {
        if revision(&row.value)? > watermark {
            ids.insert(
                std::str::from_utf8(&row.key[prefix.len()..])
                    .map_err(|_| Error::Corrupt("invalid change ID".into()))?
                    .to_owned(),
            );
        }
    }
    Ok(ids)
}

pub(crate) async fn vector_query(
    snapshot: &DbSnapshot,
    name: &str,
    schema: &NamespaceConfig,
    query: QueryRequest,
    config: &IndexConfig,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<QueryResult> {
    let stats = stats_at(snapshot, name).await?;
    let index = index_at(snapshot, name).await?;
    let mut best = BinaryHeap::new();
    let mut scanned = 0;
    let mut rows = Vec::new();
    let plan;
    let watermark = index.as_ref().map_or(0, |i| i.revision);
    if query.mode == SearchMode::Exact || index.is_none() {
        plan = "exact_scan";
        let mut iter = snapshot
            .scan_prefix_with_options(document_prefix(name), .., &scan_options())
            .await?;
        while let Some(row) = iter.next().await? {
            scanned += 1;
            let doc: Document = serde_json::from_slice(&row.value)?;
            if query.filter.matches(&doc.attributes)
                && let Some(vector) = vector_at(snapshot, name, &doc.id).await?
            {
                rows.push((doc.id, vector));
            }
            if rows.len() == 128 {
                best = score_rows(
                    std::mem::take(&mut rows),
                    best,
                    &query,
                    schema.metric,
                    permit.clone(),
                )
                .await?;
            }
        }
    } else if let Some(index) = &index {
        let prefix = index::root(name, &index.generation);
        let dirty = pending(snapshot, name, index.revision).await?;
        let selected = index::filter_ids(snapshot, &prefix, &query.filter).await?;
        if let Some(ids) = &selected
            && ids.len() <= config.filter_scan_threshold
        {
            plan = "filtered_exact";
            for id in ids.difference(&dirty) {
                scanned += 1;
                if let Some(vector) = vector_at(snapshot, name, id).await? {
                    rows.push((id.clone(), vector));
                }
                if rows.len() == 128 {
                    best = score_rows(
                        std::mem::take(&mut rows),
                        best,
                        &query,
                        schema.metric,
                        permit.clone(),
                    )
                    .await?;
                }
            }
        } else {
            plan = "centroid_ann";
            let mut clusters: Vec<_> = index
                .centroids
                .iter()
                .enumerate()
                .map(|(i, c)| (i, distance_score(&query.vector, c, schema.metric)))
                .collect();
            clusters.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let keys: Vec<_> = clusters
                .into_iter()
                .take(query.probes)
                .flat_map(|(cluster, _)| {
                    (0..index.cluster_blocks[cluster]).map({
                        let prefix = prefix.clone();
                        move |block| format!("{prefix}v/{cluster:04}/{block:08}")
                    })
                })
                .collect();
            let mut blocks = stream::iter(keys)
                .map(|key| async move {
                    snapshot
                        .get_with_options(key, &read_options())
                        .await?
                        .ok_or_else(|| Error::Corrupt("missing vector cluster block".into()))
                })
                .buffered(8);
            while let Some(block) = blocks.try_next().await? {
                for (id, vector) in binary::decode(&block)? {
                    scanned += 1;
                    if !dirty.contains(&id) && selected.as_ref().is_none_or(|ids| ids.contains(&id))
                    {
                        rows.push((id, vector));
                    }
                }
                best = score_rows(
                    std::mem::take(&mut rows),
                    best,
                    &query,
                    schema.metric,
                    permit.clone(),
                )
                .await?;
            }
        }
        for id in dirty {
            scanned += 1;
            if let Some(doc) = document_at(snapshot, name, &id).await?
                && query.filter.matches(&doc.attributes)
                && let Some(vector) = doc.vector
            {
                rows.push((id, vector));
            }
            if rows.len() == 128 {
                best = score_rows(
                    std::mem::take(&mut rows),
                    best,
                    &query,
                    schema.metric,
                    permit.clone(),
                )
                .await?;
            }
        }
    } else {
        unreachable!()
    }
    if !rows.is_empty() {
        best = score_rows(rows, best, &query, schema.metric, permit).await?;
    }
    Ok(QueryResult {
        matches: hydrate(snapshot, name, best).await?,
        scanned_documents: scanned,
        plan: plan.into(),
        revision: stats.revision,
        indexed_revision: watermark,
    })
}

pub(crate) async fn text_query(
    snapshot: &DbSnapshot,
    name: &str,
    schema: &NamespaceConfig,
    query: TextQuery,
) -> Result<QueryResult> {
    let terms = index::tokenize(&query.text);
    if terms.frequencies.is_empty() || terms.frequencies.len() > 32 {
        return Err(Error::Invalid(
            "text query requires 1..32 distinct terms".into(),
        ));
    }
    let stats = stats_at(snapshot, name).await?;
    let active = index_at(snapshot, name).await?;
    let mut corpus = index::Corpus::default();
    let mut postings: BTreeMap<String, BTreeMap<String, Posting>> = terms
        .frequencies
        .keys()
        .map(|t| (t.clone(), BTreeMap::new()))
        .collect();
    let mut eligible: Option<BTreeSet<String>> = None;
    let mut scanned = 0;
    let plan;
    if let Some(active) = &active {
        plan = "bm25_index";
        corpus = active.text.get(&query.field).cloned().unwrap_or_default();
        let prefix = index::root(name, &active.generation);
        let dirty = pending(snapshot, name, active.revision).await?;
        eligible = index::filter_ids(snapshot, &prefix, &query.filter).await?;
        if let Some(eligible) = &mut eligible {
            eligible.retain(|id| !dirty.contains(id));
        }
        for (term, rows) in &mut postings {
            let items: Vec<Posting> = index::postings(
                snapshot,
                &format!(
                    "{prefix}t/{}/{}/",
                    index::digest(query.field.as_bytes()),
                    index::digest(term.as_bytes())
                ),
            )
            .await?;
            for p in items {
                if !dirty.contains(&p.id) {
                    rows.insert(p.id.clone(), p);
                }
            }
        }
        for id in dirty {
            if let Some(old) = snapshot
                .get_with_options(format!("{prefix}l/{id}"), &read_options())
                .await?
            {
                let old: BTreeMap<String, Terms> = serde_json::from_slice(&old)?;
                if let Some(old) = old.get(&query.field) {
                    corpus.documents -= 1;
                    corpus.tokens -= u64::from(old.length);
                }
            }
            if let Some(document) = attributes_at(snapshot, name, &id).await? {
                add_text_document(&document, &query, &mut corpus, &mut postings, &mut eligible);
            }
        }
    } else {
        plan = "bm25_scan";
        if !query.filter.eq.is_empty() || !query.filter.range.is_empty() {
            eligible = Some(BTreeSet::new());
        }
        let mut iter = snapshot
            .scan_prefix_with_options(document_prefix(name), .., &scan_options())
            .await?;
        while let Some(row) = iter.next().await? {
            scanned += 1;
            let document: Document = serde_json::from_slice(&row.value)?;
            add_text_document(&document, &query, &mut corpus, &mut postings, &mut eligible);
        }
    }
    let mut scores: BTreeMap<String, f64> = BTreeMap::new();
    let average = if corpus.documents > 0 {
        corpus.tokens as f64 / corpus.documents as f64
    } else {
        0.0
    };
    for rows in postings.values() {
        let df = rows.len() as f64;
        let idf = (1.0 + (corpus.documents as f64 - df + 0.5) / (df + 0.5)).ln();
        for (id, posting) in rows {
            if eligible.as_ref().is_some_and(|ids| !ids.contains(id)) {
                continue;
            }
            if average > 0.0 {
                let tf = f64::from(posting.frequency);
                *scores.entry(id.clone()).or_default() += idf * (tf * 2.2)
                    / (tf + 1.2 * (0.25 + 0.75 * f64::from(posting.length) / average));
            }
        }
    }
    if active.is_some() {
        scanned = scores.len();
    }
    let mut best = BinaryHeap::new();
    for (id, score) in scores {
        retain(&mut best, id, score, query.top_k);
    }
    let _ = schema;
    Ok(QueryResult {
        matches: hydrate(snapshot, name, best).await?,
        scanned_documents: scanned,
        plan: plan.into(),
        revision: stats.revision,
        indexed_revision: active.map_or(0, |i| i.revision),
    })
}
fn add_text_document(
    document: &Document,
    query: &TextQuery,
    corpus: &mut index::Corpus,
    postings: &mut BTreeMap<String, BTreeMap<String, Posting>>,
    eligible: &mut Option<BTreeSet<String>>,
) {
    if let Some(text) = document
        .attributes
        .get(&query.field)
        .and_then(serde_json::Value::as_str)
    {
        let terms = index::tokenize(text);
        corpus.documents += 1;
        corpus.tokens += u64::from(terms.length);
        for (term, rows) in postings {
            if let Some(frequency) = terms.frequencies.get(term) {
                rows.insert(
                    document.id.clone(),
                    Posting {
                        id: document.id.clone(),
                        frequency: *frequency,
                        length: terms.length,
                    },
                );
            }
        }
    }
    if let Some(eligible) = eligible
        && query.filter.matches(&document.attributes)
    {
        eligible.insert(document.id.clone());
    }
}
