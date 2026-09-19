use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

pub const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_DOCUMENT_BYTES: usize = 256 * 1024;
pub const MAX_BATCH_OPERATIONS: usize = 1000;
pub const MAX_DIMENSIONS: usize = 4096;
pub const MAX_PAGE_SIZE: usize = 1000;
pub const MAX_TOP_K: usize = 100;

/// IDs are URL-safe and cannot escape their namespace's key prefix.
pub fn validate_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(Error::Invalid(
            "IDs must contain 1–128 ASCII letters, digits, underscores, or hyphens".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    #[default]
    Cosine,
    Dot,
    SquaredEuclidean,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    /// None creates a document-only namespace. Configuration is immutable.
    pub dimensions: Option<usize>,
    #[serde(default)]
    pub metric: Metric,
}

impl NamespaceConfig {
    pub fn validate(&self) -> Result<()> {
        if self
            .dimensions
            .is_some_and(|d| !(1..=MAX_DIMENSIONS).contains(&d))
        {
            return Err(Error::Invalid(format!(
                "dimensions must be between 1 and {MAX_DIMENSIONS}"
            )));
        }
        Ok(())
    }

    pub fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        let dimensions = self
            .dimensions
            .ok_or_else(|| Error::Invalid("namespace has no vector dimensions".into()))?;
        if vector.len() != dimensions || vector.iter().any(|v| !v.is_finite()) {
            return Err(Error::Invalid(format!(
                "vector must have {dimensions} finite f32 components"
            )));
        }
        if self.metric == Metric::Cosine && vector.iter().all(|v| *v == 0.0) {
            return Err(Error::Invalid(
                "cosine vectors must have nonzero length".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    #[serde(flatten)]
    pub config: NamespaceConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    #[serde(default)]
    pub upsert: Vec<Document>,
    #[serde(default)]
    pub delete: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteResult {
    pub upserted: usize,
    pub deleted: usize,
    pub sequence: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    /// Exact JSON equality on top-level attributes; all predicates are ANDed.
    #[serde(default)]
    pub eq: BTreeMap<String, Value>,
    #[serde(default)]
    pub range: BTreeMap<String, NumericRange>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericRange {
    pub gte: Option<f64>,
    pub lte: Option<f64>,
}

impl Filter {
    pub fn validate(&self) -> Result<()> {
        if self.eq.len() + self.range.len() > 64 {
            return Err(Error::Invalid(
                "at most 64 filter predicates are allowed".into(),
            ));
        }
        for range in self.range.values() {
            if (range.gte.is_none() && range.lte.is_none())
                || range.gte.is_some_and(|v| !v.is_finite())
                || range.lte.is_some_and(|v| !v.is_finite())
                || matches!((range.gte, range.lte), (Some(lo), Some(hi)) if lo > hi)
            {
                return Err(Error::Invalid(
                    "ranges require finite, ordered gte and/or lte bounds".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn matches(&self, attributes: &BTreeMap<String, Value>) -> bool {
        self.eq
            .iter()
            .all(|(field, expected)| attributes.get(field) == Some(expected))
            && self.range.iter().all(|(field, range)| {
                attributes
                    .get(field)
                    .and_then(Value::as_f64)
                    .is_some_and(|value| {
                        range.gte.is_none_or(|lo| value >= lo)
                            && range.lte.is_none_or(|hi| value <= hi)
                    })
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
    #[serde(default)]
    pub filter: Filter,
}

fn default_top_k() -> usize {
    10
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchHit {
    pub id: String,
    /// Larger is always better. Squared Euclidean returns negative distance.
    pub score: f64,
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub matches: Vec<SearchHit>,
    pub scanned_documents: usize,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageRequest {
    pub limit: usize,
    pub after: Option<String>,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            limit: 100,
            after: None,
        }
    }
}

impl PageRequest {
    pub fn validate(&self) -> Result<()> {
        if !(1..=MAX_PAGE_SIZE).contains(&self.limit) {
            return Err(Error::Invalid(format!(
                "limit must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        if let Some(after) = &self.after {
            validate_id(after)?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}
