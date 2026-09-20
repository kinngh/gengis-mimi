//! Seeded ingestion, cold/warm vector search, recall, filtered search and BM25.
//! Creates a unique database prefix; retained objects can be inspected afterward.
use clap::Parser;
use gengis_mimi::{Engine, config::Config, model::*};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Instant,
};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = 5000)]
    documents: usize,
    #[arg(long, default_value_t = 32)]
    dimensions: usize,
    #[arg(long, default_value_t = 25)]
    queries: usize,
    #[arg(long, default_value_t = 4)]
    probes: usize,
    #[arg(long, default_value = "target/benchmark.metrics")]
    metrics: PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.documents == 0
        || args.documents > 100_000
        || args.dimensions == 0
        || args.dimensions > 4096
        || args.queries == 0
        || args.queries > 1000
    {
        return Err("documents 1..100000, dimensions 1..4096, queries 1..1000 required".into());
    }
    let mut config = Config::load(args.config.as_deref())?;
    if config.cluster.is_some() {
        return Err("benchmark uses a standalone storage configuration".into());
    }
    let run = uuid::Uuid::new_v4().simple().to_string();
    config.database.prefix = format!("{}/bench-{run}", config.database.prefix);
    config.database.cache_dir = config.database.cache_dir.map(|path| path.join(&run));
    config.index.interval_ms = 0;
    config.limits.max_documents = config.limits.max_documents.max(args.documents as u64);
    config.limits.max_pending_documents = config
        .limits
        .max_pending_documents
        .max(args.documents as u64);
    let mut rng = 42u64;
    let mut random = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((rng >> 32) as u32 as f64 / u32::MAX as f64 * 2.0 - 1.0) as f32
    };
    let documents: Vec<_> = (0..args.documents)
        .map(|i| Document {
            id: format!("d{i:08}"),
            vector: Some((0..args.dimensions).map(|_| random()).collect()),
            attributes: BTreeMap::from([
                ("group".into(), json!(i % 100)),
                ("rank".into(), json!(i)),
                (
                    "body".into(),
                    json!(if i % 2 == 0 {
                        "Rust storage engine and search"
                    } else {
                        "durable document database"
                    }),
                ),
            ]),
        })
        .collect();
    let queries: Vec<_> = (0..args.queries)
        .map(|_| QueryRequest {
            vector: (0..args.dimensions).map(|_| random()).collect(),
            top_k: 10,
            filter: Filter::default(),
            mode: SearchMode::Ann,
            probes: args.probes,
        })
        .collect();
    let engine = Engine::open(&config).await?;
    engine
        .create_namespace(
            "bench".into(),
            NamespaceConfig {
                dimensions: Some(args.dimensions),
                metric: Metric::Cosine,
                text_fields: vec!["body".into()],
            },
        )
        .await?;
    let start = Instant::now();
    for chunk in documents.chunks(1000) {
        engine
            .write(
                "bench",
                WriteRequest {
                    upsert: chunk.to_vec(),
                    delete: vec![],
                },
            )
            .await?;
    }
    let ingest = start.elapsed().as_secs_f64();
    drop(documents);
    let start = Instant::now();
    let index = engine.rebuild_index("bench").await?;
    let index_seconds = start.elapsed().as_secs_f64();
    let primary_bytes = engine.stats("bench").await?.bytes;
    engine.close().await?;
    let ingest_metrics = engine.prometheus();
    drop(engine);
    if let Some(cache) = &config.database.cache_dir {
        std::fs::remove_dir_all(cache)?;
    }
    let start = Instant::now();
    let engine = Engine::open(&config).await?;
    let reopen_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let cold = engine.query("bench", queries[0].clone()).await?;
    let cold_ms = start.elapsed().as_secs_f64() * 1000.;
    let mut latencies = Vec::new();
    let mut recalls = Vec::new();
    let mut exact_latencies = Vec::new();
    let mut scanned = 0;
    for query in &queries {
        // Warm this query's blocks once before timing; exact is measured later.
        engine.query("bench", query.clone()).await?;
        let start = Instant::now();
        let ann = engine.query("bench", query.clone()).await?;
        latencies.push(start.elapsed().as_secs_f64() * 1000.);
        scanned += ann.scanned_documents;
        let mut exact_query = query.clone();
        exact_query.mode = SearchMode::Exact;
        let exact_started = Instant::now();
        let exact = engine.query("bench", exact_query).await?;
        exact_latencies.push(exact_started.elapsed().as_secs_f64() * 1000.);
        let expected: BTreeSet<_> = exact.matches.iter().map(|h| h.id.as_str()).collect();
        recalls.push(
            ann.matches
                .iter()
                .filter(|h| expected.contains(h.id.as_str()))
                .count() as f64
                / expected.len() as f64,
        );
    }
    let mut filtered = queries[0].clone();
    filtered.filter.eq.insert("group".into(), json!(1));
    let start = Instant::now();
    let filtered = engine.query("bench", filtered).await?;
    let filtered_ms = start.elapsed().as_secs_f64() * 1000.;
    let start = Instant::now();
    let text = engine
        .search_text(
            "bench",
            TextQuery {
                field: "body".into(),
                text: "storage engine".into(),
                top_k: 10,
                filter: Filter::default(),
            },
        )
        .await?;
    let text_ms = start.elapsed().as_secs_f64() * 1000.;
    latencies.sort_by(f64::total_cmp);
    exact_latencies.sort_by(f64::total_cmp);
    let p = |q: f64| latencies[((latencies.len() - 1) as f64 * q).ceil() as usize];
    std::fs::write(
        &args.metrics,
        format!(
            "# Ingestion and indexing handle\n{ingest_metrics}\n# Reopened query handle (counters restart)\n{}",
            engine.prometheus()
        ),
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "version":env!("CARGO_PKG_VERSION"),"seed":42,"distribution":"uniform independent f32 components in [-1,1]","documents":args.documents,"dimensions":args.dimensions,"queries":args.queries,"clusters":index.centroids.len(),"probes":args.probes,"prefix":config.database.prefix,
            "ingest_seconds":ingest,"documents_per_second":args.documents as f64/ingest,"index_seconds":index_seconds,"reopen_seconds":reopen_seconds,
            "cold_query_ms":cold_ms,"cold_scanned_documents":cold.scanned_documents,"warm_p50_ms":p(0.5),"warm_p99_ms":p(0.99),"recall_at_10":recalls.iter().sum::<f64>()/recalls.len() as f64,"mean_candidates":scanned as f64/queries.len() as f64,
        "exact_p50_ms":exact_latencies[exact_latencies.len()/2], "primary_encoded_bytes":primary_bytes, "ingest_index_io":io_totals(&ingest_metrics), "query_io":io_totals(&engine.prometheus()),
            "filtered_query_ms":filtered_ms,"filtered_plan":filtered.plan,"filtered_candidates":filtered.scanned_documents,"bm25_ms":text_ms,"bm25_plan":text.plan,"metrics_file":args.metrics,
        }))?
    );
    engine.close().await?;
    Ok(())
}

fn io_totals(metrics: &str) -> serde_json::Value {
    let sum = |prefix: &str| {
        metrics
            .lines()
            .filter(|line| line.starts_with(prefix))
            .filter_map(|line| line.split_whitespace().last()?.parse::<u64>().ok())
            .sum::<u64>()
    };
    json!({"payload_read_bytes":sum("gm_object_store_read_bytes "),"payload_written_bytes":sum("gm_object_store_written_bytes "),"raw_store_calls":sum("gm_object_store_calls "),"slatedb_requests":sum("slatedb_object_store_request_count{")})
}
