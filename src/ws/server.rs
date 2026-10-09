//! Port of `src/connection/ApiConnectionServer.js`: the raw websocket listener.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

use super::client::OutMsg;
use super::handler::{ApiConnectionHandler, HEARTBEAT_INTERVAL_MS};

/// Default interval for protocol-level PING frames sent by the server.
pub const DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(25);

/// Accept connections until the listener fails. Each connection runs in its own task.
///
/// `ping_interval`: the server sends a WebSocket PING control frame at this interval on every
/// connection. Browsers answer PINGs in their network stack (no JavaScript involved), so this
/// keeps sockets of hidden or throttled tabs alive; clients need no change for it.
pub async fn run(
    listener: TcpListener,
    handler: Arc<ApiConnectionHandler>,
    ping_interval: Duration,
) -> std::io::Result<()> {
    tracing::info!(
        "create ws on {} - systemId: {} - ping every {:?}",
        listener.local_addr()?,
        handler.system_id,
        ping_interval
    );
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                if is_connection_error(&e) {
                    // the peer went away between SYN and accept – nothing to do
                    continue;
                }
                // e.g. EMFILE/ENFILE: log, back off, keep serving (same policy as axum's listener)
                tracing::error!("ws accept error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, peer, handler, ping_interval).await {
                tracing::debug!("ws connection {peer} ended: {e}");
            }
        });
    }
}

/// Errors that only concern the single connection being accepted, not the listener.
fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
    )
}

/// Periodic `checkHeartbeatTimeouts` (65 s in Node).
pub fn spawn_heartbeat(handler: Arc<ApiConnectionHandler>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(HEARTBEAT_INTERVAL_MS));
        interval.tick().await; // first tick fires immediately; skip it like setInterval does
        loop {
            interval.tick().await;
            handler.check_heartbeat_timeouts();
        }
    })
}

/// `GET /health` ohne WebSocket-Upgrade (Healthcheck von Load Balancer und Docker): Die
/// WebSocket-Bibliothek beantwortet einen normalen GET nicht, deshalb wird der Anfang der
/// Verbindung vorab gelesen (peek, verbraucht nichts). 200 erst, wenn Kafka Partitionen zugeteilt
/// hat, vorher 503. Antwortet `true`, wenn die Verbindung damit erledigt ist.
async fn answer_health(stream: &mut TcpStream) -> bool {
    use tokio::io::AsyncWriteExt;
    let mut buf = [0u8; 1024];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut n = 0;
    // bis Kopfende oder Puffer voll; ein WebSocket-Client schickt seinen Request sofort
    while tokio::time::Instant::now() < deadline {
        n = match tokio::time::timeout_at(deadline, stream.peek(&mut buf)).await {
            Ok(Ok(n)) => n,
            _ => return false,
        };
        if n == 0 || n == buf.len() || buf[..n].windows(4).any(|w| w == b"\r\n\r\n") || !buf[..n].starts_with(&b"GET /health"[..n.min(11)]) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let head = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
    let is_health = (head.starts_with("get /health ") || head.starts_with("get /health?") || head.starts_with("head /health "))
        && !head.contains("upgrade: websocket");
    if !is_health {
        return false;
    }
    let ready = crate::kafka::KAFKA_READY.load(std::sync::atomic::Ordering::SeqCst);
    let (status, body) = if ready { ("200 OK", "ok") } else { ("503 Service Unavailable", "kafka not ready") };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        if head.starts_with("head ") { "" } else { body }
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.shutdown().await;
    true
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    handler: Arc<ApiConnectionHandler>,
    ping_interval: Duration,
) -> anyhow::Result<()> {
    if answer_health(&mut stream).await {
        return Ok(());
    }
    let mut forwarded_for: Option<String> = None;
    let mut user_agent = String::new();
    let ws = tokio_tungstenite::accept_hdr_async(
        stream,
        |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
            forwarded_for = req
                .headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            user_agent = req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            Ok(resp)
        },
    )
    .await?;

    let ip_address = forwarded_for.unwrap_or_else(|| peer.ip().to_string());
    let (client, mut rx) = handler.register_client(ip_address.clone(), user_agent.clone());
    tracing::info!(
        "new ws client {} {} ({})",
        ip_address,
        user_agent,
        client.uuid
    );

    let (mut sink, mut source) = ws.split();

    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await; // the first tick completes immediately
        loop {
            let res = tokio::select! {
                out = rx.recv() => match out {
                    Some(OutMsg::Text(t)) => sink.send(Message::Text(t.into())).await,
                    Some(OutMsg::Close) => {
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                    None => break,
                },
                _ = ping.tick() => sink.send(Message::Ping(Bytes::new())).await,
            };
            if res.is_err() {
                break;
            }
        }
    });

    while let Some(frame) = source.next().await {
        match frame {
            Ok(Message::Text(text)) => match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(json) => handler.handle_message(&client, json).await,
                Err(e) => {
                    tracing::warn!("error on msg {}: {e}", text.as_str());
                }
            },
            Ok(Message::Binary(bytes)) => match serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                Ok(json) => handler.handle_message(&client, json).await,
                Err(e) => tracing::warn!("error on binary msg: {e}"),
            },
            Ok(Message::Close(_)) => break,
            // answer to our PING (or an unsolicited pong): the socket is alive
            Ok(Message::Pong(_)) => client.record_pong(),
            // tungstenite answers incoming PINGs with PONGs on its own
            Ok(_) => {}
            Err(e) => {
                tracing::info!(
                    "error on connection {} {} {}",
                    e,
                    client.uuid,
                    client.ip_address
                );
                break;
            }
        }
    }

    tracing::info!("close on connection {} {}", client.uuid, client.ip_address);
    handler.delete_client(client.id);
    client.close();
    let _ = writer.await;
    Ok(())
}
