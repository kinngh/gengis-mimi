mod common;

use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use serde_json::{Value, json};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start(config: &Path, address: &str, log: &Path) -> Server {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_gengis-mimi"))
        .args(["--config", config.to_str().unwrap()])
        .env_remove("GENGIS_MIMI_API_TOKEN")
        .env("RUST_LOG", "gengis_mimi=info,slatedb=warn")
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .spawn()
        .unwrap();
    let mut server = Server(child);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    for _ in 0..100 {
        assert!(
            server.0.try_wait().unwrap().is_none(),
            "server exited: {}",
            std::fs::read_to_string(log).unwrap()
        );
        if client
            .get(format!("{address}/readyz"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return server;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "server did not become ready: {}",
        std::fs::read_to_string(log).unwrap()
    );
}

#[tokio::test]
async fn acknowledged_batch_survives_process_kill_and_empty_cache_restart() {
    let dir = common::directory();
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    let bind = socket.local_addr().unwrap();
    drop(socket);
    let config_path = dir.path().join("server.toml");
    let cache = dir.path().join("cache");
    std::fs::write(&config_path, format!(
        "[server]\nbind = \"{bind}\"\n[storage]\ntype = \"local\"\npath = {:?}\n[database]\nwal_flush_ms = 5\ncache_dir = {cache:?}\n",
        dir.path().join("objects")
    )).unwrap();
    let address = format!("http://{bind}");
    let log = dir.path().join("server.log");
    let server = start(&config_path, &address, &log).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    client
        .put(format!("{address}/v1/namespaces/demo"))
        .json(&json!({"dimensions":2}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client
        .post(format!("{address}/v1/namespaces/demo/write"))
        .json(&json!({"upsert":[{"id":"keep","vector":[1,0]},{"id":"gone","vector":[0,1]}]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    client.post(format!("{address}/v1/namespaces/demo/write")).json(&json!({"upsert":[{"id":"keep","vector":[0,1],"attributes":{"revision":2}}],"delete":["gone"]})).send().await.unwrap().error_for_status().unwrap();
    // Drop uses kill(), not SIGTERM: no graceful flush or shutdown runs.
    drop(server);
    std::fs::remove_dir_all(&cache).unwrap();
    let _restarted = start(&config_path, &address, &log).await;
    let document: Value = client
        .get(format!("{address}/v1/namespaces/demo/documents/keep"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(document["attributes"]["revision"], 2);
    assert_eq!(
        client
            .get(format!("{address}/v1/namespaces/demo/documents/gone"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let query: Value = client
        .post(format!("{address}/v1/namespaces/demo/query"))
        .json(&json!({"vector":[0,1]}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(query["matches"].as_array().unwrap().len(), 1);
    assert_eq!(query["matches"][0]["id"], "keep");
    assert_eq!(query["matches"][0]["score"], 1.);
}
