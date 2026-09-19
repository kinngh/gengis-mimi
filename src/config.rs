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
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub max_concurrent_queries: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 7878)),
            max_concurrent_queries: 4,
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
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            prefix: "gengis-mimi/v1".into(),
            wal_flush_ms: 100,
            cache_dir: None,
            cache_bytes: 512 * 1024 * 1024,
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
