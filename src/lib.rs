//! `uas` – User API Server.
//!
//! Rust port of the Node.js `UserApiServer` ("Chart Analyzing Service"):
//!
//! * websocket server (tokio-tungstenite) on `WS_PORT` – see [`ws`]
//! * Kafka producer/consumer – see [`kafka`]
//!
//! Die HTTP-API des Node-Servers und mit ihr alle Datenbankzugriffe und der Mailversand sind seit
//! 09.10.2026 entfernt (Svens Vorgabe): der uas ist nur noch WebSocket-Server, die Daten speichert
//! der Configserver (Kafka cs_config_save/cs_user_save).

pub mod auth;
pub mod config;
pub mod kafka;
pub mod kafkasecurity;
pub mod logging;
pub mod prices;
pub mod telegram;
pub mod useragent;
pub mod ws;
