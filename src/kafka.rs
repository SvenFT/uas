//! Kafka integration (`src/connection/KafkaConnection.js`).
//!
//! Two producers (main cluster + "prices" cluster) and one consumer subscribed to
//! `cs_config_live_update` and `cs_user_update`. Producing is abstracted behind
//! [`KafkaProducer`] so the websocket handler can be tested without a broker.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::{Value, json};

use crate::config::KafkaConfig;
use crate::kafkasecurity::{consumer_session_timeout_ms, kafka_compression, kafka_security};

/// true, sobald der Consumer Partitionen zugeteilt bekommen hat – vorher gingen Antworten des
/// Configservers (cs_user_update, cs_config_live_update) an diese Instanz verloren. `GET /health`
/// auf dem WebSocket-Port antwortet erst danach mit 200.
pub static KAFKA_READY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Producer-Einstellungen: Broker, TLS (KAFKA_SECURITY_PROTOCOL …) und Kompression (KAFKA_COMPRESSION_TYPE).
fn producer_config(brokers: &[String], client_id: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", brokers.join(","))
        .set("client.id", client_id)
        .set("message.timeout.ms", "30000");
    kafka_security().apply_to(&mut c);
    if let Some(codec) = kafka_compression() {
        c.set("compression.type", codec);
    }
    c
}

pub const TOPIC_ALERT: &str = "cs_alert";
pub const TOPIC_ABO_INFO: &str = "cs_abo_info";
pub const TOPIC_VIDEO_INFO: &str = "cs_video_info";
pub const TOPIC_CONFIG_SAVE: &str = "cs_config_save";
pub const TOPIC_USER_SAVE: &str = "cs_user_save";
pub const TOPIC_PRICES: &str = "prices";
pub const TOPIC_CONFIG_LIVE_UPDATE: &str = "cs_config_live_update";
pub const TOPIC_USER_UPDATE: &str = "cs_user_update";
pub const TOPIC_ALERT_UPDATE: &str = "cs_alert_update";

/// Topics the consumer subscribes to.
pub const CONSUMED_TOPICS: &[&str] = &[TOPIC_CONFIG_LIVE_UPDATE, TOPIC_USER_UPDATE];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Cluster {
    Main,
    Prices,
}

pub trait KafkaProducer: Send + Sync {
    /// Fire-and-forget publish (the Node code never awaited producer results).
    fn send(&self, cluster: Cluster, topic: &str, payload: String);
}

/// Callback invoked for every consumed message: `(topic, payload)`.
pub type MessageCallback = Arc<dyn Fn(&str, &str) + Send + Sync>;

// ---------------------------------------------------------------------------------------------
// Message builders (pure, unit-tested)
// ---------------------------------------------------------------------------------------------

/// `new Date()` as JSON: ISO-8601 with milliseconds and `Z`.
pub fn js_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// `sendUserCountInfo` – produces the "prices" tick messages for users, connections and systems.
pub fn user_count_messages(
    cluster_node: &str,
    now_ms: i64,
    user_count: u64,
    connection_count: u64,
    systems: &BTreeMap<String, u64>,
) -> Vec<String> {
    let t = (now_ms / 60_000) * 60;
    let mk = |i: String, p: u64| {
        json!({
            "c": true,
            "i": i,
            "e": "ANFIN",
            "t": t,
            "p": [p, p, p, p, 1],
        })
        .to_string()
    };
    let mut out = vec![
        mk(format!("USERS.{cluster_node}"), user_count),
        mk(format!("WS.CONNECTIONS.{cluster_node}"), connection_count),
    ];
    for (system, count) in systems {
        out.push(mk(format!("WS.SYSTEM.{cluster_node}.{system}"), *count));
    }
    out
}

/// Typed facade over a producer – the methods of the Node `KafkaConnectionHandler`.
pub struct KafkaBus {
    producer: Arc<dyn KafkaProducer>,
    cluster_node: String,
}

impl KafkaBus {
    pub fn new(producer: Arc<dyn KafkaProducer>, cluster_node: impl Into<String>) -> Self {
        KafkaBus {
            producer,
            cluster_node: cluster_node.into(),
        }
    }

    pub fn producer(&self) -> Arc<dyn KafkaProducer> {
        self.producer.clone()
    }

    pub fn send_alert_message(&self, cfg: Value) {
        self.producer
            .send(Cluster::Main, TOPIC_ALERT, cfg.to_string());
    }

    pub fn send_abo_info_message(&self, cfg: Value) {
        self.producer
            .send(Cluster::Main, TOPIC_ABO_INFO, cfg.to_string());
    }

    pub fn send_video_info_message(&self, cfg: Value) {
        self.producer
            .send(Cluster::Main, TOPIC_VIDEO_INFO, cfg.to_string());
    }

    pub fn save_chart_config(&self, cfg: Value) {
        self.producer
            .send(Cluster::Main, TOPIC_CONFIG_SAVE, cfg.to_string());
    }

    pub fn save_user_config(&self, cfg: Value) {
        self.producer
            .send(Cluster::Main, TOPIC_USER_SAVE, cfg.to_string());
    }

    pub fn send_user_count_info(
        &self,
        user_count: u64,
        connection_count: u64,
        systems: &BTreeMap<String, u64>,
    ) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        for msg in user_count_messages(
            &self.cluster_node,
            now_ms,
            user_count,
            connection_count,
            systems,
        ) {
            self.producer.send(Cluster::Prices, TOPIC_PRICES, msg);
        }
        tracing::debug!("send systems ...");
    }
}

// ---------------------------------------------------------------------------------------------
// rdkafka implementation
// ---------------------------------------------------------------------------------------------

pub struct RdKafkaProducer {
    main: FutureProducer,
    /// None: zweiter Cluster aus (KAFKA_PRICES_BROKERS leer) – Nachrichten dorthin entfallen.
    prices: Option<FutureProducer>,
}

impl RdKafkaProducer {
    pub fn new(cfg: &KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
        let main = producer_config(&cfg.brokers, &cfg.client_id).create()?;
        let prices = if cfg.prices_brokers.is_empty() {
            tracing::info!("kafka: zweiter Cluster (prices) aus – KAFKA_PRICES_BROKERS leer, Nutzerzahlen werden nicht gesendet");
            None
        } else {
            // gleiche Sicherheit wie der Hauptcluster
            Some(producer_config(&cfg.prices_brokers, &cfg.prices_client_id).create()?)
        };
        Ok(RdKafkaProducer { main, prices })
    }
}

impl KafkaProducer for RdKafkaProducer {
    fn send(&self, cluster: Cluster, topic: &str, payload: String) {
        let producer = match cluster {
            Cluster::Main => self.main.clone(),
            Cluster::Prices => match &self.prices {
                Some(p) => p.clone(),
                None => return,
            },
        };
        let topic = topic.to_string();
        tokio::spawn(async move {
            let record = FutureRecord::<(), String>::to(&topic).payload(&payload);
            if let Err((e, _)) = producer.send(record, Duration::from_secs(5)).await {
                tracing::error!("kafka produce to {topic} failed: {e}");
            }
        });
    }
}

/// Start the consumer loop (`consumer.run({eachMessage})`) on the main cluster.
/// `fromBeginning: true` in kafkajs corresponds to `auto.offset.reset=earliest`.
pub fn spawn_consumer(
    cfg: &KafkaConfig,
    callback: MessageCallback,
) -> Result<tokio::task::JoinHandle<()>, rdkafka::error::KafkaError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", cfg.brokers.join(","))
        .set("client.id", &cfg.client_id)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", &cfg.auto_offset_reset)
        .set("enable.auto.commit", "true")
        .set("session.timeout.ms", consumer_session_timeout_ms());
    kafka_security().apply_to(&mut config);
    let consumer: Arc<StreamConsumer> = Arc::new(config.create()?);
    consumer.subscribe(CONSUMED_TOPICS)?;
    tracing::info!("subscribe {:?} (Gruppe {}, Start neuer Gruppe: {})", CONSUMED_TOPICS, cfg.group_id, cfg.auto_offset_reset);

    // Bereitschaft: Zuteilung abwarten (recv() treibt den Beitritt, die Prüfung läuft daneben)
    {
        let consumer = consumer.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            loop {
                let assigned = consumer.assignment().map(|a| a.count()).unwrap_or(0);
                if assigned > 0 {
                    KAFKA_READY.store(true, std::sync::atomic::Ordering::SeqCst);
                    tracing::info!("kafka: {} Partition(en) zugeteilt nach {:.1} s – bereit", assigned, started.elapsed().as_secs_f32());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    Ok(tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let topic = msg.topic().to_string();
                    let payload = match msg.payload() {
                        Some(p) => String::from_utf8_lossy(p).into_owned(),
                        None => String::new(),
                    };
                    callback(&topic, &payload);
                }
                Err(e) => {
                    tracing::error!("kafka consumer error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }))
}

/// Records every message; used by tests.
#[derive(Default)]
pub struct RecordingProducer {
    pub sent: std::sync::Mutex<Vec<(Cluster, String, String)>>,
}

impl RecordingProducer {
    pub fn messages(&self) -> Vec<(Cluster, String, String)> {
        self.sent.lock().unwrap().clone()
    }

    pub fn for_topic(&self, topic: &str) -> Vec<Value> {
        self.messages()
            .into_iter()
            .filter(|(_, t, _)| t == topic)
            .map(|(_, _, p)| serde_json::from_str(&p).unwrap_or(Value::String(p)))
            .collect()
    }
}

impl KafkaProducer for RecordingProducer {
    fn send(&self, cluster: Cluster, topic: &str, payload: String) {
        self.sent
            .lock()
            .unwrap()
            .push((cluster, topic.to_string(), payload));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_count_messages_match_node_layout() {
        let mut systems = BTreeMap::new();
        systems.insert("MACOS".to_string(), 2);
        systems.insert("WINDOWS".to_string(), 1);
        let msgs = user_count_messages("N1", 1_700_000_123_456, 3, 5, &systems);
        assert_eq!(msgs.len(), 4);
        let first: Value = serde_json::from_str(&msgs[0]).unwrap();
        assert_eq!(first["i"], "USERS.N1");
        assert_eq!(first["e"], "ANFIN");
        assert_eq!(first["c"], true);
        assert_eq!(first["p"], json!([3, 3, 3, 3, 1]));
        // minute aligned, in seconds
        assert_eq!(first["t"], json!((1_700_000_123_456i64 / 60_000) * 60));
        let second: Value = serde_json::from_str(&msgs[1]).unwrap();
        assert_eq!(second["i"], "WS.CONNECTIONS.N1");
        assert_eq!(second["p"], json!([5, 5, 5, 5, 1]));
        let third: Value = serde_json::from_str(&msgs[2]).unwrap();
        assert_eq!(third["i"], "WS.SYSTEM.N1.MACOS");
        assert_eq!(third["p"][0], 2);
    }

    #[test]
    fn bus_routes_to_topics_and_clusters() {
        let rec = Arc::new(RecordingProducer::default());
        let bus = KafkaBus::new(rec.clone(), "N1");
        bus.send_alert_message(json!({"a": 1}));
        bus.send_abo_info_message(json!({"b": 1}));
        bus.send_video_info_message(json!({"c": 1}));
        bus.save_chart_config(json!({"d": 1}));
        bus.save_user_config(json!({"e": 1}));
        bus.send_user_count_info(1, 2, &BTreeMap::new());
        let msgs = rec.messages();
        let topics: Vec<&str> = msgs.iter().map(|(_, t, _)| t.as_str()).collect();
        assert_eq!(
            topics,
            vec![
                TOPIC_ALERT,
                TOPIC_ABO_INFO,
                TOPIC_VIDEO_INFO,
                TOPIC_CONFIG_SAVE,
                TOPIC_USER_SAVE,
                TOPIC_PRICES,
                TOPIC_PRICES
            ]
        );
        assert!(msgs[..5].iter().all(|(c, _, _)| *c == Cluster::Main));
        assert!(msgs[5..].iter().all(|(c, _, _)| *c == Cluster::Prices));
        assert_eq!(rec.for_topic(TOPIC_ALERT)[0]["a"], 1);
    }

    #[test]
    fn js_now_is_iso_with_millis() {
        let s = js_now();
        assert!(s.ends_with('Z'));
        assert_eq!(s.len(), "2026-09-03T10:00:00.000Z".len());
    }
}
