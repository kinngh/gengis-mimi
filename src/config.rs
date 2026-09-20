use std::{
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{Error, Result, model::validate_id};

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub database: DatabaseConfig,
    pub index: IndexConfig,
    pub limits: LimitsConfig,
    pub auth: AuthConfig,
    pub cluster: Option<ClusterConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub max_concurrent_queries: usize,
    pub request_timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 7878)),
            max_concurrent_queries: 4,
            request_timeout_ms: 30_000,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageConfig {
    Local {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        #[serde(default = "default_region")]
        region: String,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        allow_http: bool,
    },
}

fn default_region() -> String {
    "us-east-1".into()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Local {
            path: "./data".into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    pub prefix: String,
    pub wal_flush_ms: u64,
    pub cache_dir: Option<PathBuf>,
    pub cache_bytes: usize,
    pub migrate: bool,
    pub l0_sst_bytes: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            prefix: "gengis-mimi/v1".into(),
            wal_flush_ms: 100,
            cache_dir: None,
            cache_bytes: 512 * 1024 * 1024,
            migrate: false,
            l0_sst_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Config {
    /// Relative paths are resolved against the working directory, not the config file.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let config = match path {
            Some(path) => toml::from_str(&std::fs::read_to_string(path)?)
                .map_err(|e| Error::Config(format!("invalid configuration: {e}")))?,
            None => Self::default(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if !(100..=3_600_000).contains(&self.server.request_timeout_ms) {
            return Err(Error::Config(
                "request_timeout_ms must be 100..3600000".into(),
            ));
        }
        if !(64 * 1024..=256 * 1024 * 1024).contains(&self.database.l0_sst_bytes) {
            return Err(Error::Config("l0_sst_bytes must be 64 KiB..256 MiB".into()));
        }
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
            if !matches!(self.storage, StorageConfig::S3 { .. }) {
                return Err(Error::Config(
                    "cluster deployment requires S3 storage".into(),
                ));
            }
        }
        if self.index.clusters == 0
            || self.index.clusters > 1024
            || self.index.iterations == 0
            || self.index.iterations > 50
            || self.index.max_build_bytes < 1024 * 1024
        {
            return Err(Error::Config("index requires 1..1024 clusters, 1..50 iterations, and at least 1 MiB max_build_bytes".into()));
        }
        if self.limits.max_documents == 0
            || self.limits.max_namespace_bytes == 0
            || self.limits.max_pending_documents == 0
        {
            return Err(Error::Config("namespace limits must be positive".into()));
        }
        for grant in &self.auth.grants {
            validate_id(&grant.namespace)?;
            if grant.token_env.is_empty() {
                return Err(Error::Config("token_env cannot be empty".into()));
            }
        }
        if self
            .database
            .prefix
            .split('/')
            .any(|part| validate_id(part).is_err())
        {
            return Err(Error::Config(
                "database.prefix must contain nonempty slash-separated ID components".into(),
            ));
        }
        if !(1..=60_000).contains(&self.database.wal_flush_ms) {
            return Err(Error::Config(
                "wal_flush_ms must be between 1 and 60000".into(),
            ));
        }
        if !(1..=64).contains(&self.server.max_concurrent_queries) {
            return Err(Error::Config(
                "max_concurrent_queries must be between 1 and 64".into(),
            ));
        }
        if self.database.cache_dir.is_some() && self.database.cache_bytes < 4 * 1024 * 1024 {
            return Err(Error::Config("cache_bytes must be at least 4 MiB".into()));
        }
        if let StorageConfig::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
        } = &self.storage
        {
            if bucket.is_empty() || region.is_empty() {
                return Err(Error::Config("S3 bucket and region cannot be empty".into()));
            }
            if let Some(endpoint) = endpoint
                && !(endpoint.starts_with("https://")
                    || (*allow_http && endpoint.starts_with("http://")))
            {
                return Err(Error::Config(
                    "S3 endpoint requires https://, or http:// with allow_http = true".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IndexConfig {
    pub interval_ms: u64,
    pub clusters: usize,
    pub iterations: usize,
    pub max_build_bytes: usize,
    pub filter_scan_threshold: usize,
}
impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5000,
            clusters: 32,
            iterations: 4,
            max_build_bytes: 512 * 1024 * 1024,
            filter_scan_threshold: 4096,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    pub max_documents: u64,
    pub max_namespace_bytes: u64,
    pub max_pending_documents: u64,
}
impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_documents: 1_000_000,
            max_namespace_bytes: 16 * 1024 * 1024 * 1024,
            max_pending_documents: 50_000,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub grants: Vec<GrantConfig>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantConfig {
    pub namespace: String,
    pub token_env: String,
    #[serde(default)]
    pub write: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterConfig {
    Worker {
        node_id: String,
        #[serde(default = "default_lease")]
        lease_ms: u64,
    },
    Gateway {
        shards: Vec<ShardConfig>,
        upstream_token_env: String,
    },
}
fn default_lease() -> u64 {
    9000
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardConfig {
    pub id: String,
    pub workers: Vec<String>,
}
impl ClusterConfig {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Worker { node_id, lease_ms } => {
                validate_id(node_id)?;
                if !(3000..=300_000).contains(lease_ms) {
                    return Err(Error::Config("lease_ms must be 3000..300000".into()));
                }
            }
            Self::Gateway {
                shards,
                upstream_token_env,
            } => {
                if shards.is_empty() || shards.len() > 128 || upstream_token_env.is_empty() {
                    return Err(Error::Config(
                        "gateway requires 1..128 shards and an upstream token environment variable"
                            .into(),
                    ));
                }
                let mut ids = std::collections::HashSet::new();
                for shard in shards {
                    validate_id(&shard.id)?;
                    if !ids.insert(&shard.id) || shard.workers.is_empty() || shard.workers.len() > 8
                    {
                        return Err(Error::Config(
                            "shard IDs must be unique, each with 1..8 workers".into(),
                        ));
                    }
                    for worker in &shard.workers {
                        let url = reqwest::Url::parse(worker)
                            .map_err(|e| Error::Config(e.to_string()))?;
                        if !matches!(url.scheme(), "http" | "https")
                            || url.path() != "/"
                            || url.query().is_some()
                            || url.fragment().is_some()
                            || !url.username().is_empty()
                            || url.password().is_some()
                        {
                            return Err(Error::Config(
                                "worker addresses must be HTTP(S) origins".into(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl StorageConfig {
    pub(crate) fn s3_builder(&self) -> Result<slatedb::object_store::aws::AmazonS3Builder> {
        use slatedb::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
        let Self::S3 {
            bucket,
            region,
            endpoint,
            allow_http,
        } = self
        else {
            return Err(Error::Config("S3 storage required".into()));
        };
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_allow_http(*allow_http)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(endpoint) = endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
        }
        Ok(builder)
    }
}
