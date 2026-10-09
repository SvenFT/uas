//! Starts the real `uas` binary and checks which ports answer: only the websocket port (the
//! HTTP API with database and mails was removed on 09.10.2026 – even `WS_ONLY=0`/`API_PORT`
//! must not open it). No Kafka is needed – the binary starts without it (it only logs errors).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server {
    child: Child,
    api_port: u16,
    ws_port: u16,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn connect(port: u16) -> Option<TcpStream> {
    TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().unwrap(),
        Duration::from_millis(500),
    )
    .ok()
}

fn wait_listening(port: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if connect(port).is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Spawn the binary with a fresh working directory: an empty `.env` (dotenvy walks up the
/// directory tree until it finds one, so this stops it from picking up a developer's file)
/// and its own `logs/`.
fn start(ws_only: Option<&str>) -> Server {
    let api_port = free_port();
    let ws_port = free_port();
    let dir = std::env::temp_dir().join(format!("uas-bin-test-{}-{ws_port}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(".env"), "").unwrap();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_uas"));
    cmd.current_dir(&dir)
        .env_remove("WS_ONLY")
        .env("API_PORT", api_port.to_string())
        .env("WS_PORT", ws_port.to_string())
        .env("KAFKA_BROKERS", "127.0.0.1:1")
        .env("KAFKA_PRICES_BROKERS", "127.0.0.1:1")
        .env("RUST_LOG", "warn")
        .env("JWT_SECRET", "test-secret")
        .env("LOG_FILES", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(v) = ws_only {
        cmd.env("WS_ONLY", v);
    }
    let child = cmd.spawn().expect("spawn uas binary");
    let server = Server {
        child,
        api_port,
        ws_port,
        dir,
    };
    // The websocket listener is bound last, so once it accepts the HTTP decision is final.
    assert!(
        wait_listening(ws_port, Duration::from_secs(30)),
        "websocket port did not open"
    );
    server
}

fn raw_http(port: u16, request: &str) -> Option<String> {
    let mut stream = connect(port)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request.as_bytes()).ok()?;
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    Some(String::from_utf8_lossy(&buf).into_owned())
}

fn ws_handshake_ok(port: u16) -> bool {
    let req = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\
Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
    let mut stream = match connect(port) {
        Some(s) => s,
        None => return false,
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 101")
}

#[test]
fn default_mode_is_websocket_only() {
    let s = start(None);
    assert!(
        ws_handshake_ok(s.ws_port),
        "websocket must accept connections"
    );
    assert!(
        connect(s.api_port).is_none(),
        "HTTP port must not be bound"
    );
}

#[test]
fn old_http_switch_opens_nothing() {
    // WS_ONLY=0 hat früher die HTTP-API eingeschaltet – die gibt es nicht mehr
    let s = start(Some("0"));
    assert!(ws_handshake_ok(s.ws_port));
    assert!(connect(s.api_port).is_none(), "no HTTP API any more");
}

#[test]
fn health_on_websocket_port_without_kafka_is_503() {
    // Kafka unter 127.0.0.1:1 ist nicht erreichbar: keine Zuteilung, also 503 – und der
    // WebSocket-Handshake auf demselben Port funktioniert weiter
    let s = start(None);
    let resp = raw_http(s.ws_port, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap_or_default();
    assert!(resp.starts_with("HTTP/1.1 503"), "unexpected: {resp}");
    assert!(ws_handshake_ok(s.ws_port), "websocket must still accept connections");
}

#[test]
fn missing_jwt_secret_stops_the_server() {
    let dir = std::env::temp_dir().join(format!("uas-bin-test-nojwt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(".env"), "").unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_uas"))
        .current_dir(&dir)
        .env_remove("JWT_SECRET")
        .env("LOG_FILES", "0")
        .env("WS_PORT", free_port().to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success());
}
