use std::{cmp::Ordering, collections::BinaryHeap};

use slatedb::bytes::Bytes;

use crate::{
    Result,
    model::{Document, Metric, QueryRequest, SearchHit},
};

/// The worst retained hit is at the root, so memory stays proportional to k.
pub(crate) struct Ranked(pub SearchHit);

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

pub(crate) fn score_batch(
    rows: Vec<Bytes>,
    mut best: BinaryHeap<Ranked>,
    query: &QueryRequest,
    metric: Metric,
) -> Result<BinaryHeap<Ranked>> {
    let query_norm = norm(&query.vector);
    for row in rows {
        let document: Document = serde_json::from_slice(&row)?;
        let Some(vector) = &document.vector else {
            continue;
        };
        if !query.filter.matches(&document.attributes) {
            continue;
        }
        let score = match metric {
            Metric::Cosine => {
                (dot(&query.vector, vector) / (query_norm * norm(vector))).clamp(-1.0, 1.0)
            }
            Metric::Dot => dot(&query.vector, vector),
            Metric::SquaredEuclidean => -query
                .vector
                .iter()
                .zip(vector)
                .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                .sum::<f64>(),
        };
        let hit = Ranked(SearchHit {
            id: document.id,
            score,
            attributes: document.attributes,
        });
        if best.len() < query.top_k {
            best.push(hit);
        } else if best.peek().is_some_and(|worst| hit < *worst) {
            best.pop();
            best.push(hit);
        }
    }
    Ok(best)
}

fn dot(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

fn norm(vector: &[f32]) -> f64 {
    dot(vector, vector).sqrt()
}
