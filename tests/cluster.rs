mod common;
use gengis_mimi::{cluster::shard_for, config::ShardConfig};
use serde_json::{Value, json};
use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn start(config: &Path, log: &Path) -> Server {
    let log = std::fs::File::create(log).unwrap();
    Server(
        Command::new(env!("CARGO_BIN_EXE_gengis-mimi"))
            .args(["--config", config.to_str().unwrap()])
            .env("GENGIS_MIMI_API_TOKEN", "cluster-test-admin")
            .env("GM_TEST_UPSTREAM", "cluster-test-admin")
            .env("GM_READ_TOKEN", "cluster-test-reader")
            .env("RUST_LOG", "gengis_mimi=info,slatedb=warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn wait(client: &reqwest::Client, url: &str) {
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if client
                .get(url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("not ready: {url}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires S3/MinIO; starts two shards, a standby, and a gateway"]
async fn gateway_places_namespaces_enforces_auth_and_survives_owner_process_kill() {
    let dir = common::directory();
    let bucket = std::env::var("GENGIS_MIMI_TEST_BUCKET").expect("test bucket");
    let endpoint = std::env::var("GENGIS_MIMI_TEST_ENDPOINT").ok();
    let endpoint_line = endpoint.as_ref().map_or(String::new(), |e| {
        format!(
            "endpoint = {e:?}\nallow_http = {}\n",
            e.starts_with("http://")
        )
    });
    let storage = format!(
        "[storage]\ntype = \"s3\"\nbucket = {bucket:?}\nregion = \"us-east-1\"\n{endpoint_line}"
    );
    let prefix = format!("gm-cluster-tests/{}", uuid::Uuid::new_v4().simple());
    let (a, b, c, g) = (port(), port(), port(), port());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .no_proxy()
        .build()
        .unwrap();
    let worker = |id: &str, shard: &str, port: u16| {
        let file = dir.path().join(format!("{id}.toml"));
        std::fs::write(&file,format!("[server]\nbind = \"127.0.0.1:{port}\"\n{storage}\n[database]\nprefix = \"{prefix}/{shard}\"\nwal_flush_ms = 5\n[index]\ninterval_ms = 0\n[cluster]\nrole = \"worker\"\nnode_id = {id:?}\nlease_ms = 3000\n")).unwrap();
        start(&file, &dir.path().join(format!("{id}.log")))
    };
    let owner = worker("a1", "a", a);
    wait(&client, &format!("http://127.0.0.1:{a}/readyz")).await;
    let standby = worker("a2", "a", b);
    let second = worker("b1", "b", c);
    wait(&client, &format!("http://127.0.0.1:{b}/healthz")).await;
    wait(&client, &format!("http://127.0.0.1:{c}/readyz")).await;
    assert_eq!(
        client
            .get(format!("http://127.0.0.1:{b}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    let shards = vec![
        ShardConfig {
            id: "a".into(),
            workers: vec![
                format!("http://127.0.0.1:{a}"),
                format!("http://127.0.0.1:{b}"),
            ],
        },
        ShardConfig {
            id: "b".into(),
            workers: vec![format!("http://127.0.0.1:{c}")],
        },
    ];
    let names: Vec<String> = ["a", "b"]
        .iter()
        .map(|wanted| {
            (0..100)
                .map(|i| format!("ns{i}"))
                .find(|name| shard_for(name, &shards).unwrap().id == *wanted)
                .unwrap()
        })
        .collect();
    let gateway_file = dir.path().join("gateway.toml");
    std::fs::write(&gateway_file,format!("[server]\nbind = \"127.0.0.1:{g}\"\n{storage}\n[database]\nprefix = \"{prefix}/gateway\"\n[[auth.grants]]\nnamespace = {:?}\ntoken_env = \"GM_READ_TOKEN\"\nwrite = false\n[cluster]\nrole = \"gateway\"\nupstream_token_env = \"GM_TEST_UPSTREAM\"\n[[cluster.shards]]\nid = \"a\"\nworkers = [\"http://127.0.0.1:{a}\",\"http://127.0.0.1:{b}\"]\n[[cluster.shards]]\nid = \"b\"\nworkers = [\"http://127.0.0.1:{c}\"]\n",names[0])).unwrap();
    let gateway = start(&gateway_file, &dir.path().join("gateway.log"));
    let url = format!("http://127.0.0.1:{g}");
    wait(&client, &format!("{url}/readyz")).await;
    for name in &names {
        client
            .put(format!("{url}/v1/namespaces/{name}"))
            .bearer_auth("cluster-test-admin")
            .json(&json!({"dimensions":2,"text_fields":["body"]}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        client.post(format!("{url}/v1/namespaces/{name}/write")).bearer_auth("cluster-test-admin").json(&json!({"upsert":[{"id":"a","vector":[1,0],"attributes":{"body":"durable storage"}}]})).send().await.unwrap().error_for_status().unwrap();
        client
            .post(format!("{url}/v1/namespaces/{name}/index"))
            .bearer_auth("cluster-test-admin")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let listing: Value = client
        .get(format!("{url}/v1/namespaces"))
        .bearer_auth("cluster-test-admin")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listing["items"].as_array().unwrap().len(), 2);
    let target = format!("{url}/v1/namespaces/{}", names[0]);
    assert_eq!(
        client
            .post(format!("{target}/query"))
            .bearer_auth("cluster-test-reader")
            .json(&json!({"vector":[1,0]}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{target}/write"))
            .bearer_auth("cluster-test-reader")
            .json(&json!({"delete":["a"]}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(format!("{url}/v1/namespaces/{}", names[1]))
            .bearer_auth("cluster-test-reader")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    drop(owner);
    wait(&client, &format!("http://127.0.0.1:{b}/readyz")).await;
    // Discovery cache expires in one second; no writes are automatically retried.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let query: Value = client
        .post(format!("{target}/query"))
        .bearer_auth("cluster-test-reader")
        .json(&json!({"vector":[1,0]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(query["matches"][0]["id"], "a");
    client.post(format!("{target}/write")).bearer_auth("cluster-test-admin").json(&json!({"upsert":[{"id":"b","vector":[0,1],"attributes":{"body":"fresh recovery"}}],"delete":["a"]})).send().await.unwrap().error_for_status().unwrap();
    let query: Value = client
        .post(format!("{target}/search"))
        .bearer_auth("cluster-test-reader")
        .json(&json!({"field":"body","text":"recovery"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(query["matches"][0]["id"], "b");
    drop((gateway, standby, second));
}
