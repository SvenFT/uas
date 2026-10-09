//! Websocket server tests: real TCP socket, real tungstenite client, recording Kafka producer.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use uas::kafka::{KafkaBus, RecordingProducer, TOPIC_CONFIG_SAVE};
use uas::prices::PriceConnectionHandler;
use uas::ws::ApiConnectionHandler;
use uas::ws::server::run;

const SECRET: &str = "ws-test-secret";

struct Server {
    handler: Arc<ApiConnectionHandler>,
    producer: Arc<RecordingProducer>,
    url: String,
}

async fn start() -> Server {
    start_with_ping(Duration::from_secs(25)).await
}

async fn start_with_ping(ping_interval: Duration) -> Server {
    let producer = Arc::new(RecordingProducer::default());
    let bus = KafkaBus::new(producer.clone(), "TEST");
    let handler = Arc::new(ApiConnectionHandler::new(
        "WSS#TEST",
        SECRET,
        bus,
        Arc::new(PriceConnectionHandler::new()),
        None,
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(run(listener, handler.clone(), ping_interval));
    Server {
        handler,
        producer,
        url: format!("ws://{addr}"),
    }
}

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str, user_agent: &str) -> Ws {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert("user-agent", user_agent.parse().unwrap());
    req.headers_mut()
        .insert("x-forwarded-for", "203.0.113.7".parse().unwrap());
    let (ws, _) = connect_async(req).await.unwrap();
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

async fn recv(ws: &mut Ws) -> Value {
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for ws message")
        .expect("stream ended")
        .expect("ws error");
    match frame {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        other => panic!("unexpected frame {other:?}"),
    }
}

fn token(user_id: &str, role: &str) -> String {
    let data =
        json!({"USER_ID": user_id, "USER_NAME": "sven", "USER_ROLE": role, "TELEGRAM_USER_ID": 0});
    uas::auth::sign(SECRET, 60, data.as_object().unwrap().clone()).unwrap()
}

async fn wait_for_clients(server: &Server, n: usize) {
    for _ in 0..100 {
        if server.handler.client_count() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "expected {n} clients, got {}",
        server.handler.client_count()
    );
}

#[tokio::test]
async fn login_heartbeat_and_chart_config() {
    let server = start().await;
    let mut ws = connect(
        &server.url,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    )
    .await;
    wait_for_clients(&server, 1).await;
    let client = server.handler.clients().remove(0);
    assert_eq!(client.ip_address, "203.0.113.7");
    assert!(client.user_agent.contains("Macintosh"));
    assert_eq!(client.uuid, "WSS#TEST#1");

    // failed login
    send(
        &mut ws,
        json!({"action": "login", "data": {"user": "x", "password": "y", "token": "bad"}}),
    )
    .await;
    let m = recv(&mut ws).await;
    assert_eq!(m["action"], "login");
    assert_eq!(m["data"]["result"], "failed");

    // ignored before login
    send(&mut ws, json!({"action": "heartbeat"})).await;

    // successful login
    send(
        &mut ws,
        json!({"action": "login", "data": {"token": token("'12'", "USER")}}),
    )
    .await;
    let m = recv(&mut ws).await;
    assert_eq!(m["action"], "login");
    assert_eq!(m["requestId"], 0);
    assert_eq!(m["data"]["result"], "ok");
    assert_eq!(m["data"]["useMessanger"], false);
    assert_eq!(m["data"]["rights"].as_array().unwrap().len(), 6);
    assert_eq!(client.user_id(), 12);

    send(&mut ws, json!({"action": "heartbeat"})).await;
    let m = recv(&mut ws).await;
    assert_eq!(
        m,
        json!({"action": "resHeartbeat", "requestId": 0, "data": {}})
    );

    send(
        &mut ws,
        json!({"action": "chart_cfg", "requestId": 4, "data": {"symbol": "DAX"}}),
    )
    .await;
    for _ in 0..50 {
        if !server.producer.for_topic(TOPIC_CONFIG_SAVE).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let saved = server.producer.for_topic(TOPIC_CONFIG_SAVE);
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0]["userId"], 12);
    assert_eq!(saved[0]["user"], "sven");
    assert_eq!(saved[0]["uuid"], "WSS#TEST#1");
    assert_eq!(saved[0]["cfg"], json!({"symbol": "DAX", "requestId": 4}));

    // invalid JSON is ignored, connection stays open
    ws.send(Message::Text("not json".into())).await.unwrap();
    send(&mut ws, json!({"action": "heartbeat"})).await;
    assert_eq!(recv(&mut ws).await["action"], "resHeartbeat");

    ws.close(None).await.unwrap();
    wait_for_clients(&server, 0).await;
}

#[tokio::test]
async fn kafka_messages_are_fanned_out_to_clients() {
    let server = start().await;
    let mut a = connect(&server.url, "ua").await;
    let mut b = connect(&server.url, "ua").await;
    wait_for_clients(&server, 2).await;
    send(
        &mut a,
        json!({"action": "login", "data": {"token": token("1", "USER")}}),
    )
    .await;
    send(
        &mut b,
        json!({"action": "login", "data": {"token": token("2", "USER")}}),
    )
    .await;
    assert_eq!(recv(&mut a).await["data"]["result"], "ok");
    assert_eq!(recv(&mut b).await["data"]["result"], "ok");

    server.handler.on_kafka_message(
        "cs_config_live_update",
        r#"{"uuid":"","headline":"H","cfg":{"mode":"full","x":1}}"#,
    );
    let ma = recv(&mut a).await;
    let mb = recv(&mut b).await;
    assert_eq!(ma["action"], "updLiveChart");
    assert_eq!(ma["data"]["headline"], "H");
    assert_eq!(ma["data"]["x"], 1);
    assert_eq!(mb["action"], "updLiveChart");

    server
        .handler
        .on_kafka_message("cs_user_update", r#"{"userIds":[2],"data":{"abo":"plus"}}"#);
    let mb = recv(&mut b).await;
    assert_eq!(mb["action"], "user_update");
    assert_eq!(mb["data"]["abo"], "plus");

    // `a` got nothing from the targeted message: the next message it sees is its own heartbeat
    send(&mut a, json!({"action": "heartbeat"})).await;
    assert_eq!(recv(&mut a).await["action"], "resHeartbeat");
}

#[tokio::test]
async fn heartbeat_sweep_closes_stale_connections() {
    let server = start().await;
    let mut ws = connect(&server.url, "Mozilla/5.0 (Windows NT 10.0)").await;
    wait_for_clients(&server, 1).await;
    send(
        &mut ws,
        json!({"action": "login", "data": {"token": token("3", "ADMIN")}}),
    )
    .await;
    assert_eq!(recv(&mut ws).await["data"]["result"], "ok");

    let stats = server.handler.check_heartbeat_timeouts();
    assert_eq!(stats.connections, 1);
    assert_eq!(stats.distinct_admins, 1);
    assert_eq!(stats.systems.get("WINDOWS"), Some(&1));
    assert_eq!(recv(&mut ws).await["action"], "resHeartbeat");
    let prices = server.producer.for_topic("prices");
    assert_eq!(prices.len(), 3);
    assert_eq!(prices[2]["i"], "WS.SYSTEM.TEST.WINDOWS");

    // age the client beyond the timeout: the sweep removes and closes it
    server.handler.clients()[0].backdate_last_msg(6 * 60 * 1000);
    let stats = server.handler.check_heartbeat_timeouts();
    assert_eq!(stats.removed.len(), 1);
    assert_eq!(server.handler.client_count(), 0);
    let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap();
    assert!(matches!(
        frame,
        Some(Ok(Message::Close(_))) | None | Some(Err(_))
    ));
}

#[tokio::test]
async fn daxen_and_market_list_actions() {
    let server = start().await;
    let mut ws = connect(&server.url, "ua").await;
    wait_for_clients(&server, 1).await;
    send(
        &mut ws,
        json!({"action": "login", "data": {"token": token("4", "USER")}}),
    )
    .await;
    assert_eq!(recv(&mut ws).await["data"]["result"], "ok");

    send(&mut ws, json!({"action": "daxen", "data": {"type": "abo"}})).await;
    let info = recv(&mut ws).await;
    assert_eq!(info["action"], "daxen");
    assert_eq!(info["data"]["type"], "info");
    for _ in 0..3 {
        assert_eq!(recv(&mut ws).await["data"]["type"], "activeRun");
    }

    send(&mut ws, json!({"action": "getMarketLists", "requestId": 9})).await;
    for _ in 0..50 {
        if !server.producer.for_topic(TOPIC_CONFIG_SAVE).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let saved = server.producer.for_topic(TOPIC_CONFIG_SAVE);
    assert_eq!(
        saved[0]["cfg"],
        json!({"requestId": 9, "mode": "getMarketLists"})
    );

    // a reply from the backend with the market-list action is re-labelled for the client
    server.handler.on_kafka_message(
        "cs_config_live_update",
        r#"{"uuid":"WSS#TEST#1","cfg":{"mode":"resLastView","action":"getMarketLists","requestId":9,"lists":[1]}}"#,
    );
    let m = recv(&mut ws).await;
    assert_eq!(m["action"], "getMarketLists");
    assert_eq!(m["requestId"], 9);
    assert_eq!(m["data"]["lists"], json!([1]));
    assert!(m["data"].get("action").is_none());
}

#[tokio::test]
async fn server_sends_protocol_pings_and_counts_pongs() {
    let server = start_with_ping(Duration::from_millis(100)).await;
    let mut ws = connect(&server.url, "ua").await;
    wait_for_clients(&server, 1).await;

    // No login, no application traffic: the server must still ping on its own.
    // tokio-tungstenite answers each PING with a PONG automatically (like a browser does).
    let mut pings = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pings < 3 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Ping(_)))) => pings += 1,
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert_eq!(pings, 3, "expected three PING frames within the deadline");

    // The pongs reach the server; they are tracked but do not count as application messages.
    // Keep polling the client so tungstenite flushes its queued PONG replies.
    let client = server.handler.clients().remove(0);
    for _ in 0..50 {
        if client.pong_count() >= 3 {
            break;
        }
        let _ = tokio::time::timeout(Duration::from_millis(20), ws.next()).await;
    }
    assert!(client.pong_count() >= 3, "pongs: {}", client.pong_count());
    assert!(client.last_pong_ms() > 0);
    ws.close(None).await.unwrap();
    wait_for_clients(&server, 0).await;
}
