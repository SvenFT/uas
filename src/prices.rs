//! Port of `src/connection/PriceConnection.js`.
//!
//! In the Node server the STOMP connection to the price server is completely commented out;
//! `aboUpdates` and the update handlers are no-ops. This module keeps the same interface
//! (subscription bookkeeping and a message callback) so the websocket handler can call it,
//! without opening any network connection.

use std::collections::HashMap;
use std::sync::Mutex;

/// Callback signature `(msgtype, symbol, timeframe, price)`.
pub type PriceCallback = Box<dyn Fn(&str, &str, &str, &serde_json::Value) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionInfo {
    pub kind: String,
    pub symbol: String,
    pub timeframe: String,
}

/// `ChartWebsocketInterface` – subscription registry only (transport disabled upstream).
#[derive(Default)]
pub struct ChartWebsocketInterface {
    subscriptions: Mutex<HashMap<String, SubscriptionInfo>>,
    active: Mutex<bool>,
}

impl ChartWebsocketInterface {
    fn key(kind: &str, symbol: &str, timeframe: &str) -> String {
        format!("{kind}#{symbol}#{timeframe}")
    }

    pub fn subscribe(&self, kind: &str, symbol: &str, timeframe: &str) {
        let mut subs = self.subscriptions.lock().unwrap();
        subs.entry(Self::key(kind, symbol, timeframe))
            .or_insert_with(|| SubscriptionInfo {
                kind: kind.into(),
                symbol: symbol.into(),
                timeframe: timeframe.into(),
            });
    }

    pub fn unsubscribe(&self, kind: &str, symbol: &str, timeframe: &str) {
        self.subscriptions
            .lock()
            .unwrap()
            .remove(&Self::key(kind, symbol, timeframe));
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.lock().unwrap().len()
    }

    pub fn is_active(&self) -> bool {
        *self.active.lock().unwrap()
    }

    pub fn on_disconnect(&self) {
        *self.active.lock().unwrap() = false;
        tracing::info!("disconnect ws");
    }
}

/// `PriceConnectionHandler`
#[derive(Default)]
pub struct PriceConnectionHandler {
    pub interface: ChartWebsocketInterface,
    callback: Mutex<Option<PriceCallback>>,
}

impl PriceConnectionHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_message_callback(&self, cb: PriceCallback) {
        *self.callback.lock().unwrap() = Some(cb);
    }

    /// `aboUpdates(symbol, timeframe)` – a no-op in the Node server (call commented out).
    pub fn abo_updates(&self, symbol: &str, timeframe: &str, client_id: u64) {
        tracing::debug!(
            "priceAbo {symbol} {timeframe} from client {client_id} (price feed disabled)"
        );
    }

    pub fn handle_price_update(&self, symbol: &str, timeframe: &str, price: &serde_json::Value) {
        if let Some(cb) = self.callback.lock().unwrap().as_ref() {
            cb("updPrice", symbol, timeframe, price);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn subscriptions_are_deduplicated() {
        let i = ChartWebsocketInterface::default();
        i.subscribe("part", "EURUSD", "H1");
        i.subscribe("part", "EURUSD", "H1");
        i.subscribe("fibo", "EURUSD", "H1");
        assert_eq!(i.subscription_count(), 2);
        i.unsubscribe("part", "EURUSD", "H1");
        assert_eq!(i.subscription_count(), 1);
        assert!(!i.is_active());
    }

    #[test]
    fn callback_receives_price_updates() {
        let h = PriceConnectionHandler::new();
        let hits = Arc::new(AtomicUsize::new(0));
        let h2 = hits.clone();
        h.set_message_callback(Box::new(move |t, s, tf, _| {
            assert_eq!((t, s, tf), ("updPrice", "BTCUSDT.BINANCE", "M15"));
            h2.fetch_add(1, Ordering::SeqCst);
        }));
        h.handle_price_update("BTCUSDT.BINANCE", "M15", &serde_json::json!({"c": 1}));
        h.abo_updates("BTCUSDT.BINANCE", "M15", 1);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
