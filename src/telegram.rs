//! Port of `src/telegram/connection.js`.
//!
//! The Telegram bot is disabled in the Node server (`tgc = null`, constructor returns before
//! creating the bot). This keeps the interface – message callback and `send_message` – so the
//! instant-messenger plumbing in the websocket handler stays intact, but never talks to Telegram.

use std::sync::Mutex;

use serde_json::Value;

pub type InstantMessageCallback = Box<dyn Fn(&Value) + Send + Sync>;

pub struct TelegramConnection {
    pub group_id: i64,
    callback: Mutex<Option<InstantMessageCallback>>,
}

impl Default for TelegramConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl TelegramConnection {
    pub fn new() -> Self {
        TelegramConnection {
            group_id: std::env::var("TELEGRAM_GROUP_ID")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(-770005830),
            callback: Mutex::new(None),
        }
    }

    pub fn set_message_callback(&self, cb: InstantMessageCallback) {
        *self.callback.lock().unwrap() = Some(cb);
    }

    /// Would forward `"<user>: <text>"` to the group; bot is disabled, so it only logs.
    pub fn send_message(&self, user_name: &str, instant_messenger_id: i64, message: &Value) {
        if instant_messenger_id != 0
            && let Some(text) = message.get("text").and_then(Value::as_str)
        {
            tracing::info!("send im to {}: {} {}", self.group_id, text, user_name);
        }
    }

    /// Simulates an incoming Telegram message (used by tests).
    pub fn inject(&self, msg: &Value) {
        if let Some(cb) = self.callback.lock().unwrap().as_ref() {
            cb(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn callback_is_invoked() {
        let t = TelegramConnection::new();
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        t.set_message_callback(Box::new(move |_| {
            n2.fetch_add(1, Ordering::SeqCst);
        }));
        t.inject(&serde_json::json!({"text": "hi"}));
        t.send_message("sven", 1, &serde_json::json!({"text": "hi"}));
        t.send_message("sven", 0, &serde_json::json!({"text": "hi"}));
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
}
