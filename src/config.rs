//! Runtime configuration, read from environment variables (`.env` is loaded by `main`).
//!
//! Defaults mirror the Node implementation (`app.js`, `src/connection/KafkaConnection.js`).
//! Seit 09.10.2026 nur noch WebSocket: keine Datenbank-, SMTP- oder HTTP-Einstellungen mehr.

use std::env;


#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub client_id: String,
    /// Leer = zweiter Cluster aus (KAFKA_PRICES_BROKERS gesetzt, aber leer – z. B. IONOS ohne Topic prices).
    pub prices_brokers: Vec<String>,
    pub prices_client_id: String,
    /// kafkaproxy-Produce-Modus für das Topic prices (KAFKAPROXY_HOST, leer = aus): gesetzt gehen die
    /// Nutzerzahlen per TCP an den Proxy ins alte Kafka statt an KAFKA_PRICES_BROKERS.
    pub proxy_host: String,
    /// KAFKAPROXY_TOKEN – nur aus der Umgebung, nie loggen.
    pub proxy_token: String,
    pub proxy_pending_max: usize,
    pub proxy_window: usize,
    /// Präfix vor dem Symbol der Nutzerzahlen (PRICES_SYMBOL_PREFIX, z. B. `CLOUD` → `CLOUD.USERS.<node>`),
    /// damit im Chart die Umgebung erkennbar ist; leer = wie bisher.
    pub symbol_prefix: String,
    pub cluster_node: String,
    /// Consumer-Gruppe (KAFKA_GROUP, Vorgabe KAFKA_CLIENT_ID). Je uas-Instanz eindeutig: jede braucht
    /// alle Antworten (cs_user_update, cs_config_live_update) für ihre eigenen Verbindungen.
    pub group_id: String,
    /// auto.offset.reset für eine neue Gruppe (KAFKA_AUTO_OFFSET_RESET, Vorgabe earliest wie kafkajs
    /// fromBeginning); latest vermeidet, dass eine neue Gruppe 7 Tage alte Antworten nachliest.
    pub auto_offset_reset: String,
}


#[derive(Debug, Clone)]
pub struct Config {
    pub ws_port: u16,
    /// Interval of the server-side WebSocket PING frames (`WS_PING_INTERVAL_SECS`, default 25).
    pub ws_ping_interval_secs: u64,
    pub system_id: String,
    pub jwt_secret: String,
    pub kafka: KafkaConfig,
}

fn env_or(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

/// Boolean switch: `1/true/yes/on` → true, `0/false/no/off` → false, anything else → default.
pub fn parse_bool(value: Option<&str>, default: bool) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => true,
        Some(v) if matches!(v.as_str(), "0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    parse_bool(env::var(key).ok().as_deref(), default)
}

/// Öffentliche Variante von `env_bool` (z. B. LOG_FILES im Logging).
pub fn env_flag(key: &str, default: bool) -> bool {
    env_bool(key, default)
}

fn env_port(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Symbol-Präfix normieren: leer bleibt leer, sonst genau ein Punkt am Ende (`CLOUD` und `CLOUD.` → `CLOUD.`).
pub fn symbol_prefix(raw: &str) -> String {
    let p = raw.trim().trim_end_matches('.');
    if p.is_empty() { String::new() } else { format!("{p}.") }
}

/// Wie [`broker_list`], aber gesetzt und leer bedeutet "aus" (leere Liste).
fn optional_broker_list(list_key: &str) -> Vec<String> {
    match env::var(list_key) {
        Ok(v) if v.trim().is_empty() => Vec::new(),
        _ => broker_list(list_key),
    }
}

fn broker_list(list_key: &str) -> Vec<String> {
    match env::var(list_key) {
        Ok(v) if !v.is_empty() => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => vec![format!(
            "{}:{}",
            env_or("KAFKA_HOST", "localhost"),
            env_or("KAFKA_PORT", "9092")
        )],
    }
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            ws_port: env_port("WS_PORT", 7073),
            ws_ping_interval_secs: env_or("WS_PING_INTERVAL_SECS", "25")
                .parse()
                .ok()
                .filter(|s| *s > 0)
                .unwrap_or(25),
            system_id: env_or("SYSTEMID", "WSS#001"),
            // keine Vorgabe mehr im Quelltext: ohne JWT_SECRET beendet sich der Server (main)
            jwt_secret: env_or("JWT_SECRET", ""),
            kafka: KafkaConfig {
                brokers: broker_list("KAFKA_BROKERS"),
                client_id: env_or("KAFKA_CLIENT_ID", "websockerserver_v1"),
                prices_brokers: optional_broker_list("KAFKA_PRICES_BROKERS"),
                prices_client_id: env_or("KAFKA_PRICES_CLIENT_ID", "websockerserver_v1"),
                proxy_host: env_or("KAFKAPROXY_HOST", "").trim().to_string(),
                proxy_token: env_or("KAFKAPROXY_TOKEN", ""),
                proxy_pending_max: env_or("KAFKAPROXY_PENDING_MAX", "5000").trim().parse().unwrap_or(5000),
                proxy_window: env_or("KAFKAPROXY_WINDOW", "200").trim().parse().unwrap_or(200),
                symbol_prefix: symbol_prefix(&env_or("PRICES_SYMBOL_PREFIX", "")),
                cluster_node: env_or("CLUSTER_NODE", "undefined"),
                group_id: env_or("KAFKA_GROUP", &env_or("KAFKA_CLIENT_ID", "websockerserver_v1")),
                auto_offset_reset: env_or("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            },
        }
    }

    /// Configuration for tests: no external systems, well-known secret.
    pub fn for_tests() -> Self {
        Config {
            ws_port: 0,
            ws_ping_interval_secs: 25,
            system_id: "WSS#TEST".into(),
            jwt_secret: "test-secret".into(),
            kafka: KafkaConfig {
                brokers: vec!["localhost:9092".into()],
                client_id: "uas-test".into(),
                prices_brokers: vec!["localhost:9092".into()],
                prices_client_id: "uas-test".into(),
                proxy_host: String::new(),
                proxy_token: String::new(),
                proxy_pending_max: 100,
                proxy_window: 50,
                symbol_prefix: String::new(),
                cluster_node: "TEST".into(),
                group_id: "uas-test".into(),
                auto_offset_reset: "earliest".into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_list_splits_and_trims() {
        // SAFETY: tests in this module run single threaded with respect to this key.
        unsafe { env::set_var("UAS_TEST_BROKERS", "a:1, b:2,,c:3") };
        assert_eq!(broker_list("UAS_TEST_BROKERS"), vec!["a:1", "b:2", "c:3"]);
        unsafe { env::remove_var("UAS_TEST_BROKERS") };
    }

    #[test]
    fn broker_list_falls_back_to_host_port() {
        unsafe { env::remove_var("UAS_TEST_MISSING") };
        let list = broker_list("UAS_TEST_MISSING");
        assert_eq!(list.len(), 1);
        assert!(list[0].ends_with(":9092") || list[0].contains(':'));
    }

    #[test]
    fn bool_parsing() {
        for v in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(parse_bool(Some(v), false), "{v}");
        }
        for v in ["0", "false", "No", "off"] {
            assert!(!parse_bool(Some(v), true), "{v}");
        }
        assert!(parse_bool(None, true));
        assert!(!parse_bool(None, false));
        assert!(parse_bool(Some("garbage"), true));
        assert!(!parse_bool(Some(""), false));
    }

    #[test]
    fn env_bool_reads_flags() {
        unsafe { env::remove_var("UAS_TEST_FLAG") };
        assert!(env_bool("UAS_TEST_FLAG", true));
        unsafe { env::set_var("UAS_TEST_FLAG", "0") };
        assert!(!env_bool("UAS_TEST_FLAG", true));
        unsafe { env::remove_var("UAS_TEST_FLAG") };
    }

    #[test]
    fn symbol_prefix_is_normalized() {
        assert_eq!(symbol_prefix(""), "");
        assert_eq!(symbol_prefix("  "), "");
        assert_eq!(symbol_prefix("CLOUD"), "CLOUD.");
        assert_eq!(symbol_prefix("CLOUD."), "CLOUD.");
        assert_eq!(symbol_prefix(" cloud.. "), "cloud.");
    }

    #[test]
    fn test_config_has_expected_defaults() {
        let c = Config::for_tests();
        assert_eq!(c.jwt_secret, "test-secret");
    }
}
