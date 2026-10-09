//! Process entry point (`app.js`): wires configuration, logging, Kafka and the websocket
//! server together. Nur WebSocket – die HTTP-API mit Datenbank und Mailversand ist seit
//! 09.10.2026 entfernt.

use std::path::Path;
use std::sync::Arc;

use tokio::net::TcpListener;

use uas::config::Config;
use uas::kafka::{KafkaBus, RdKafkaProducer, spawn_consumer};
use uas::prices::PriceConnectionHandler;
use uas::ws::ApiConnectionHandler;
use uas::ws::server::{run as run_ws, spawn_heartbeat};

/// Resolves on SIGINT (Ctrl-C) or SIGTERM (what `docker stop` sends to PID 1).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("cannot listen for SIGTERM: {e}");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let config = Arc::new(Config::from_env());
    let _log_guard = uas::logging::init(Path::new("./logs"))?;
    // ohne Schlüssel ließe sich kein Token prüfen – früher stand eine Vorgabe im Quelltext
    if config.jwt_secret.is_empty() {
        tracing::error!("JWT_SECRET fehlt – derselbe Schlüssel, mit dem der Authserver die Tokens signiert");
        std::process::exit(1);
    }
    // Kafka-Einstellungen vor dem Start prüfen: falsche TLS-/Kompressionsangaben beenden den Prozess mit Meldung
    tracing::info!(
        "Kafka: {}, Kompression {}",
        uas::kafkasecurity::kafka_security(),
        uas::kafkasecurity::kafka_compression().unwrap_or_else(|| "keine".to_string())
    );


    // --- kafka + websocket handler --------------------------------------------------------
    let producer = Arc::new(RdKafkaProducer::new(&config.kafka)?);
    let bus = KafkaBus::new(producer, config.kafka.cluster_node.clone());
    let prices = Arc::new(PriceConnectionHandler::new());
    let handler = Arc::new(ApiConnectionHandler::new(
        config.system_id.clone(),
        config.jwt_secret.clone(),
        bus,
        prices.clone(),
        None, // Telegram integration is disabled, as in the Node server
    ));
    {
        let h = handler.clone();
        prices.set_message_callback(Box::new(move |msgtype, _symbol, _timeframe, price| {
            h.on_prices_message(msgtype, price);
        }));
    }
    {
        let h = handler.clone();
        spawn_consumer(
            &config.kafka,
            Arc::new(move |topic, payload| h.on_kafka_message(topic, payload)),
        )?;
    }
    spawn_heartbeat(handler.clone());

    // --- websocket server -----------------------------------------------------------------
    let ws_listener = TcpListener::bind(("0.0.0.0", config.ws_port)).await?;
    let ws_task = tokio::spawn(run_ws(
        ws_listener,
        handler.clone(),
        std::time::Duration::from_secs(config.ws_ping_interval_secs),
    ));

    tracing::info!("Server started (websocket only) at {}", config.ws_port);
    tokio::select! {
        r = ws_task => { r??; }
        _ = shutdown_signal() => { tracing::info!("shutdown requested"); }
    }
    Ok(())
}
