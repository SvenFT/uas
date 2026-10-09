//! Port of `src/connection/ApiConnectionHandler.js` (+ `checkLogin` from
//! `ApiConnectionServer.js`): the registry of websocket clients, dispatch of incoming
//! client messages, fan-out of Kafka messages and the heartbeat/timeout sweep.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use super::client::{Client, LoginInfo, OutMsg, login_failed_payload, login_ok_payload, truthy};
use super::daxen::DaxenHandler;
use crate::auth;
use crate::kafka::{KafkaBus, TOPIC_ALERT_UPDATE, TOPIC_CONFIG_LIVE_UPDATE, js_now};
use crate::prices::PriceConnectionHandler;
use crate::telegram::TelegramConnection;
use crate::useragent::system_key;
use crate::ws::client::MARKET_LIST_ACTIONS;

/// Clients without an application message *and* without a PONG for longer than this are
/// dropped by the heartbeat sweep (see [`Client::last_activity_ms`]).
pub const CLIENT_TIMEOUT_MS: i64 = 1000 * 60 * 5;
/// Interval of the heartbeat sweep (`setInterval(..., 65000)`).
pub const HEARTBEAT_INTERVAL_MS: u64 = 65_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeartbeatStats {
    pub connections: usize,
    pub distinct_users: usize,
    pub distinct_admins: usize,
    pub removed: Vec<u64>,
    pub systems: BTreeMap<String, u64>,
}

pub struct ApiConnectionHandler {
    pub system_id: String,
    jwt_secret: String,
    clients: Mutex<HashMap<u64, Arc<Client>>>,
    next_id: AtomicU64,
    pub kafka: KafkaBus,
    pub prices: Arc<PriceConnectionHandler>,
    pub daxen: DaxenHandler,
    pub telegram: Option<Arc<TelegramConnection>>,
    messenger_messages: Mutex<Vec<String>>,
}

impl ApiConnectionHandler {
    pub fn new(
        system_id: impl Into<String>,
        jwt_secret: impl Into<String>,
        kafka: KafkaBus,
        prices: Arc<PriceConnectionHandler>,
        telegram: Option<Arc<TelegramConnection>>,
    ) -> Self {
        ApiConnectionHandler {
            system_id: system_id.into(),
            jwt_secret: jwt_secret.into(),
            clients: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            kafka,
            prices,
            daxen: DaxenHandler::new(),
            telegram,
            messenger_messages: Mutex::new(Vec::new()),
        }
    }

    // ---------------------------------------------------------------------------------------
    // client registry
    // ---------------------------------------------------------------------------------------

    pub fn register_client(
        &self,
        ip_address: String,
        user_agent: String,
    ) -> (Arc<Client>, mpsc::UnboundedReceiver<OutMsg>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();
        let client = Arc::new(Client::new(&self.system_id, id, ip_address, user_agent, tx));
        self.clients.lock().unwrap().insert(id, client.clone());
        (client, rx)
    }

    pub fn delete_client(&self, id: u64) {
        self.clients.lock().unwrap().remove(&id);
    }

    pub fn client_count(&self) -> usize {
        self.clients.lock().unwrap().len()
    }

    pub fn clients(&self) -> Vec<Arc<Client>> {
        let map = self.clients.lock().unwrap();
        let mut v: Vec<Arc<Client>> = map.values().cloned().collect();
        v.sort_by_key(|c| c.id);
        v
    }

    // ---------------------------------------------------------------------------------------
    // login
    // ---------------------------------------------------------------------------------------

    /// `ApiConnectionServer.checkLogin(user, password, token)` – only the token is evaluated.
    pub fn check_login(&self, token: Option<&str>) -> Option<LoginInfo> {
        let token = token?;
        if token.is_empty() {
            return None;
        }
        let claims = auth::verify(&self.jwt_secret, token).ok()?;
        let user_id = claims.user_id()?;
        // `parseInt(userId)` – NaN (non numeric) means "not logged in"
        let user_id: i64 = user_id.trim().parse().unwrap_or(0);
        Some(LoginInfo {
            user_id,
            user_name: claims.user_name().unwrap_or_default(),
            user_role: claims.user_role().unwrap_or_default(),
            instant_messenger_id: claims.telegram_user_id().unwrap_or(0),
            instant_messenger_active: claims.instant_messenger_active(),
        })
    }

    fn login_client(&self, client: &Arc<Client>, data: &Value) {
        let token = data.get("token").and_then(Value::as_str);
        let info = self.check_login(token);
        let ok = client.apply_login(info.clone());
        if ok {
            let info = info.unwrap_or_default();
            tracing::info!(
                "new ws client login - {} User: {} {}",
                info.user_id,
                info.user_name,
                client.ip_address
            );
            client.send_message(
                "login",
                login_ok_payload(client.is_instant_messenger_active()),
            );
            if client.is_instant_messenger_active() {
                self.send_instant_messenger_messages(client);
            }
        } else {
            tracing::info!(
                "new ws client login failed: {:?} {}",
                data.get("user"),
                client.ip_address
            );
            client.send_message("login", login_failed_payload());
        }
    }

    // ---------------------------------------------------------------------------------------
    // incoming client messages
    // ---------------------------------------------------------------------------------------

    pub async fn handle_message(&self, client: &Arc<Client>, msg: Value) {
        let action = msg
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let data = msg.get("data").cloned().unwrap_or(Value::Null);
        tracing::debug!(
            "{} {} {} {}",
            client.id,
            client.user_name(),
            client.user_id(),
            msg
        );

        if action == "login" {
            if !client.is_logged_in() {
                self.login_client(client, &data);
            }
            return;
        }
        if !client.is_logged_in() {
            return;
        }

        if action == "mu" {
            // Multi-user broadcast to the user's other sessions (the Node code crashed here on
            // undefined variables; this is the evident intent).
            let user_id = client.user_id();
            for cl in self.clients() {
                if cl.user_id() == user_id && cl.id != client.id {
                    cl.send_message("mu", data.clone());
                }
            }
            return;
        }

        client.touch();
        let login = client.login();
        let base = |extra: Map<String, Value>| {
            let mut m = Map::new();
            m.insert("user".into(), json!(login.user_name));
            m.insert("uuid".into(), json!(client.uuid));
            m.insert("userId".into(), json!(login.user_id));
            for (k, v) in extra {
                m.insert(k, v);
            }
            Value::Object(m)
        };

        match action.as_str() {
            "daxen" => self.daxen.handle_message(client, &data),
            a if a == "chart_cfg" || MARKET_LIST_ACTIONS.contains(&a) => {
                let mut cfg = match data {
                    Value::Object(m) => m,
                    _ => Map::new(),
                };
                if let Some(rid) = msg.get("requestId") {
                    cfg.insert("requestId".into(), rid.clone());
                }
                if a != "chart_cfg" {
                    cfg.insert("mode".into(), json!(a));
                }
                let mut extra = Map::new();
                extra.insert("userRole".into(), json!(login.user_role));
                extra.insert("cfg".into(), Value::Object(cfg));
                extra.insert("time".into(), json!(js_now()));
                self.kafka.save_chart_config(base(extra));
            }
            "priceAbo" => {
                let symbol = data.get("symbol").and_then(Value::as_str).unwrap_or("");
                let timeframe = data.get("timeframe").and_then(Value::as_str).unwrap_or("");
                self.prices.abo_updates(symbol, timeframe, client.id);
            }
            "abo_info" => {
                let mut extra = Map::new();
                extra.insert("cfg".into(), data);
                extra.insert("time".into(), json!(js_now()));
                self.kafka.send_abo_info_message(base(extra));
            }
            "video_info" => {
                let mut extra = Map::new();
                // `wsMessage.data ? wsMessage.data.mode : "req"`
                let action_value = if truthy(&data) {
                    data.get("mode").cloned()
                } else {
                    Some(json!("req"))
                };
                extra.insert("cfg".into(), data);
                if let Some(a) = action_value {
                    extra.insert("action".into(), a);
                }
                extra.insert("time".into(), json!(js_now()));
                self.kafka.send_video_info_message(base(extra));
            }
            "user" => {
                // `wsMessage.data.password`: without a data object Node threw and sent nothing
                let Some(d) = data.as_object() else {
                    return;
                };
                // bcrypt.hash(<non-string>) produced no hash in Node; the message was still sent,
                // just without `newPwd`.
                let new_hash = match d.get("password").and_then(Value::as_str) {
                    Some(p) => match auth::hash_password(p.to_string()).await {
                        Ok(h) => Some(h),
                        Err(e) => {
                            tracing::error!("password hash failed: {e}");
                            None
                        }
                    },
                    None => {
                        tracing::warn!("user action without password from client {}", client.id);
                        None
                    }
                };
                let mut cfg = Map::new();
                cfg.insert("mode".into(), json!("changePwd"));
                cfg.insert("userId".into(), json!(login.user_id));
                if let Some(old) = d.get("oldPassword") {
                    cfg.insert("oldPwd".into(), old.clone());
                }
                if let Some(h) = new_hash {
                    cfg.insert("newPwd".into(), json!(h));
                }
                cfg.insert("time".into(), json!(js_now()));
                let mut extra = Map::new();
                extra.insert("cfg".into(), Value::Object(cfg));
                let payload = base(extra);
                tracing::debug!("{}", payload);
                self.kafka.save_user_config(payload);
            }
            "chart_alert" => {
                let mut extra = Map::new();
                extra.insert("cfg".into(), data);
                extra.insert("time".into(), json!(js_now()));
                self.kafka.send_alert_message(base(extra));
            }
            "heartbeat" => {
                client.send_message("resHeartbeat", json!({}));
            }
            "sendInstantMessage" if client.is_instant_messenger_active() => {
                let text = data.get("text").cloned().unwrap_or(Value::Null);
                if let Some(tg) = &self.telegram {
                    tg.send_message(&login.user_name, login.instant_messenger_id, &data);
                }
                self.on_instant_message(json!({
                    "message_id": 0,
                    "text": text,
                    "from": {"first_name": login.user_name},
                    "time": chrono::Utc::now().timestamp_millis(),
                }));
            }
            _ => {}
        }
    }

    // ---------------------------------------------------------------------------------------
    // instant messenger
    // ---------------------------------------------------------------------------------------

    pub fn on_instant_message(&self, msg: Value) {
        let s = msg.to_string();
        tracing::info!("onInstantMessage {s}");
        self.messenger_messages.lock().unwrap().push(s.clone());
        for cl in self.clients() {
            if cl.is_instant_messenger_active() {
                cl.send_message("updInstantMessage", json!({"msg": s}));
            }
        }
    }

    pub fn send_instant_messenger_messages(&self, client: &Client) {
        let msgs = self.messenger_messages.lock().unwrap().clone();
        for m in msgs {
            client.send_message("updInstantMessage", json!({"msg": m}));
        }
    }

    // ---------------------------------------------------------------------------------------
    // price feed
    // ---------------------------------------------------------------------------------------

    pub fn on_prices_message(&self, msgtype: &str, price: &Value) {
        for cl in self.clients() {
            cl.send_message(msgtype, price.clone());
        }
    }

    // ---------------------------------------------------------------------------------------
    // kafka fan-out
    // ---------------------------------------------------------------------------------------

    pub fn on_kafka_message(&self, topic: &str, payload: &str) {
        let clients = self.clients();
        route_kafka_message(topic, payload, &clients);
    }

    // ---------------------------------------------------------------------------------------
    // heartbeat sweep
    // ---------------------------------------------------------------------------------------

    pub fn check_heartbeat_timeouts(&self) -> HeartbeatStats {
        let now = chrono::Utc::now().timestamp_millis();
        let mut users = HashSet::new();
        let mut admins = HashSet::new();
        let mut systems: BTreeMap<String, u64> = BTreeMap::new();
        let mut removed = vec![];
        let clients = self.clients();
        for c in &clients {
            users.insert(c.user_id());
            *systems.entry(system_key(&c.user_agent)).or_insert(0) += 1;
            if c.user_role() == "ADMIN" {
                admins.insert(c.user_id());
            }
            if c.last_activity_ms() + CLIENT_TIMEOUT_MS < now {
                removed.push(c.id);
            } else {
                c.send_message("resHeartbeat", json!({}));
            }
        }
        for id in &removed {
            tracing::info!("delete Client:{id}");
            if let Some(c) = self.clients.lock().unwrap().remove(id) {
                c.close();
            }
        }
        let stats = HeartbeatStats {
            connections: clients.len(),
            distinct_users: users.len(),
            distinct_admins: admins.len(),
            removed,
            systems,
        };
        tracing::info!(
            "checkHeartbeatTimeouts... {} ({}/{})",
            stats.connections,
            stats.distinct_users,
            stats.distinct_admins
        );
        self.kafka.send_user_count_info(
            stats.distinct_users as u64,
            stats.connections as u64,
            &stats.systems,
        );
        stats
    }
}

// ---------------------------------------------------------------------------------------------
// Kafka routing (pure functions, unit tested)
// ---------------------------------------------------------------------------------------------

/// `a || b` – first truthy value.
fn first_truthy(a: Option<&Value>, b: Option<&Value>) -> Option<Value> {
    match a {
        Some(v) if truthy(v) => Some(v.clone()),
        _ => match b {
            Some(v) if truthy(v) => Some(v.clone()),
            _ => None,
        },
    }
}

/// The field merging the Node code performs on `cs_config_live_update` messages.
pub fn merge_live_update(msg: &mut Map<String, Value>) {
    let mut cfg = match msg.get("cfg") {
        Some(Value::Object(m)) => m.clone(),
        _ => Map::new(),
    };
    // `cfg.key = a || b` in JS: when both are falsy the property becomes `undefined`, which
    // JSON.stringify drops – so the key is removed here.
    fn assign(cfg: &mut Map<String, Value>, key: &str, value: Option<Value>) {
        match value {
            Some(v) => {
                cfg.insert(key.into(), v);
            }
            None => {
                cfg.shift_remove(key);
            }
        }
    }
    let user = first_truthy(msg.get("userId"), msg.get("user"));
    assign(&mut cfg, "user", user);
    assign(&mut cfg, "time", msg.get("time").cloned());
    let views = first_truthy(msg.get("views"), cfg.get("views"));
    assign(&mut cfg, "views", views);
    for key in [
        "headline",
        "title",
        "abos",
        "description",
        "resourceInfo",
        "labelActions",
        "commentInternal",
    ] {
        let v = first_truthy(cfg.get(key), msg.get(key));
        assign(&mut cfg, key, v);
    }
    if !cfg.contains_key("inactive")
        && let Some(v) = msg.get("inactive")
    {
        cfg.insert("inactive".into(), v.clone());
    }
    msg.insert("cfg".into(), Value::Object(cfg));
}

/// JS `a == b` where `a` is a number.
fn loose_eq_num(a: i64, b: &Value) -> bool {
    match b {
        Value::Number(n) => n.as_f64() == Some(a as f64),
        Value::String(s) => s.trim().parse::<f64>().ok() == Some(a as f64),
        Value::Bool(bv) => (a == 1) == *bv && (a == 0 || a == 1),
        _ => false,
    }
}

/// `v === "0" || v === 0`
fn is_zero_user_id(v: Option<&Value>) -> bool {
    match v {
        Some(Value::String(s)) => s == "0",
        Some(Value::Number(n)) => n.as_f64() == Some(0.0),
        _ => false,
    }
}

/// `cl.uuid == jsonMsg.uuid` (`==` with a string left-hand side)
fn uuid_matches(client_uuid: &str, v: Option<&Value>) -> bool {
    match v {
        Some(Value::String(s)) => s == client_uuid,
        _ => false,
    }
}

/// `jsonMsg.uuid == ""`
fn uuid_is_empty_string(v: Option<&Value>) -> bool {
    matches!(v, Some(Value::String(s)) if s.is_empty())
        || matches!(v, Some(Value::Number(n)) if n.as_f64() == Some(0.0))
        || matches!(v, Some(Value::Bool(false)))
}

fn is_public(msg: &Map<String, Value>) -> bool {
    let data = msg.get("data");
    let user_tools = data
        .and_then(|d| d.get("data"))
        .and_then(|d| d.get("userTools"));
    match user_tools {
        Some(ut) if truthy(ut) => ut
            .get(0)
            .and_then(|t| t.get("userTool"))
            .and_then(|t| t.get("isPublic"))
            .map(truthy)
            .unwrap_or(false),
        _ => data
            .and_then(|d| d.get("isPublic"))
            .map(truthy)
            .unwrap_or(false),
    }
}

/// `onKafkaMessage(msgtype, msg)` applied to a snapshot of clients.
pub fn route_kafka_message(topic: &str, payload: &str, clients: &[Arc<Client>]) {
    let mut msg: Map<String, Value> = match serde_json::from_str::<Value>(payload) {
        Ok(Value::Object(m)) => m,
        Ok(_) => Map::new(),
        Err(e) => {
            tracing::error!("error onKafkaMessage {payload}: {e}");
            Map::new()
        }
    };

    if topic == TOPIC_CONFIG_LIVE_UPDATE {
        merge_live_update(&mut msg);
    }

    // `jsonMsg && jsonMsg.data ? jsonMsg.data : {}` (truthiness, not presence)
    let data_or_empty = || {
        msg.get("data")
            .filter(|d| truthy(d))
            .cloned()
            .unwrap_or_else(|| json!({}))
    };
    let cfg_or_empty = || {
        msg.get("cfg")
            .filter(|c| truthy(c))
            .cloned()
            .unwrap_or_else(|| json!({}))
    };
    let short_topic = topic.get(3..).unwrap_or("");

    // `if (jsonMsg.userIds && (msgtype.length > 3))` – otherwise the generic branches below run
    let has_user_ids = msg.get("userIds").map(truthy).unwrap_or(false);
    if has_user_ids && topic.len() > 3 {
        // `id === cl.userId`: strict equality, so only numeric ids match
        let ids: Vec<f64> = msg
            .get("userIds")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_f64).collect())
            .unwrap_or_default();
        for cl in clients {
            if ids.contains(&(cl.user_id() as f64)) {
                cl.send_message(short_topic, data_or_empty());
            }
        }
        return;
    }

    let uuid = msg.get("uuid");
    let cfg = msg.get("cfg");
    let cfg_mode = cfg.and_then(|c| c.get("mode")).and_then(Value::as_str);

    for cl in clients {
        if topic == TOPIC_ALERT_UPDATE {
            if uuid_is_empty_string(uuid) || uuid_matches(&cl.uuid, uuid) {
                cl.send_message("updAlert", json!({"msg": payload}));
            }
        } else if topic == TOPIC_CONFIG_LIVE_UPDATE && cfg_mode == Some("resLastView") {
            if uuid_matches(&cl.uuid, uuid) {
                cl.send_message("resLastView", cfg_or_empty());
            }
        } else if topic == TOPIC_CONFIG_LIVE_UPDATE {
            let is_part_without_org =
                cfg_mode == Some("part") && !msg.get("uuid_org").map(truthy).unwrap_or(false);
            let same_uuid = uuid_matches(&cl.uuid, uuid);
            if uuid_is_empty_string(uuid) || (cfg.is_some() && (is_part_without_org != same_uuid)) {
                cl.send_message("updLiveChart", cfg_or_empty());
            }
        } else {
            let uuid_falsy = uuid.map(|u| !truthy(u)).unwrap_or(true);
            let user_id = msg.get("userId");
            let user_matches = is_zero_user_id(user_id)
                || user_id
                    .map(|u| loose_eq_num(cl.user_id(), u))
                    .unwrap_or(false);
            if topic.len() > 3
                && ((uuid_falsy && user_matches) || is_public(&msg) || uuid_matches(&cl.uuid, uuid))
            {
                tracing::debug!(
                    "onKafkaMessage 4: {} {} {:?}",
                    cl.uuid,
                    cl.user_id(),
                    msg.get("data").and_then(|d| d.get("action"))
                );
                cl.send_message(short_topic, data_or_empty());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::RecordingProducer;

    fn handler() -> (ApiConnectionHandler, Arc<RecordingProducer>) {
        let rec = Arc::new(RecordingProducer::default());
        let bus = KafkaBus::new(rec.clone(), "N1");
        (
            ApiConnectionHandler::new(
                "WSS#T",
                "secret",
                bus,
                Arc::new(PriceConnectionHandler::new()),
                None,
            ),
            rec,
        )
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<OutMsg>) -> Vec<Value> {
        let mut out = vec![];
        while let Ok(OutMsg::Text(t)) = rx.try_recv() {
            out.push(serde_json::from_str(&t).unwrap());
        }
        out
    }

    fn logged_in(
        h: &ApiConnectionHandler,
        user_id: i64,
        role: &str,
    ) -> (Arc<Client>, mpsc::UnboundedReceiver<OutMsg>) {
        let (c, rx) = h.register_client(
            "ip".into(),
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)".into(),
        );
        c.apply_login(Some(LoginInfo {
            user_id,
            user_name: format!("user{user_id}"),
            user_role: role.into(),
            instant_messenger_id: 0,
            instant_messenger_active: false,
        }));
        (c, rx)
    }

    fn token(secret: &str, user_id: &str) -> String {
        let data = json!({"USER_ID": user_id, "USER_NAME": "sven", "USER_ROLE": "ADMIN", "TELEGRAM_USER_ID": 0});
        auth::sign(secret, 60, data.as_object().unwrap().clone()).unwrap()
    }

    #[test]
    fn check_login_parses_token() {
        let (h, _) = handler();
        let info = h.check_login(Some(&token("secret", "'42'"))).unwrap();
        assert_eq!(info.user_id, 42);
        assert_eq!(info.user_name, "sven");
        assert_eq!(info.user_role, "ADMIN");
        assert!(h.check_login(Some(&token("other", "1"))).is_none());
        assert!(h.check_login(None).is_none());
        assert!(h.check_login(Some("")).is_none());
        // TELEGRAM_USER_ID 0 -> messenger off; field missing (HTTP login tokens) -> on
        assert!(!info.instant_messenger_active);
        let data = json!({"USER_ID": "3", "USER_NAME": "n", "USER_ROLE": "USER"});
        let t = auth::sign("secret", 60, data.as_object().unwrap().clone()).unwrap();
        let info = h.check_login(Some(&t)).unwrap();
        assert!(info.instant_messenger_active);
        assert_eq!(info.instant_messenger_id, 0);
        assert_eq!(
            h.check_login(Some(&token("secret", "abc")))
                .unwrap()
                .user_id,
            0
        );
    }

    #[tokio::test]
    async fn login_flow_via_messages() {
        let (h, _) = handler();
        let (c, mut rx) = h.register_client("ip".into(), "ua".into());
        h.handle_message(&c, json!({"action": "login", "data": {"token": "bad"}}))
            .await;
        let m = drain(&mut rx);
        assert_eq!(m[0]["action"], "login");
        assert_eq!(m[0]["data"]["result"], "failed");
        assert!(!c.is_logged_in());

        // messages before login are ignored
        h.handle_message(&c, json!({"action": "heartbeat"})).await;
        assert!(drain(&mut rx).is_empty());

        h.handle_message(
            &c,
            json!({"action": "login", "data": {"token": token("secret", "7")}}),
        )
        .await;
        let m = drain(&mut rx);
        assert_eq!(m[0]["data"]["result"], "ok");
        assert_eq!(m[0]["data"]["rights"].as_array().unwrap().len(), 6);
        assert!(c.is_logged_in());
        assert_eq!(c.user_id(), 7);

        h.handle_message(&c, json!({"action": "heartbeat"})).await;
        let m = drain(&mut rx);
        assert_eq!(m[0]["action"], "resHeartbeat");
    }

    #[tokio::test]
    async fn chart_cfg_and_market_actions_go_to_kafka() {
        let (h, rec) = handler();
        let (c, _rx) = logged_in(&h, 7, "USER");
        h.handle_message(
            &c,
            json!({"action": "chart_cfg", "requestId": 5, "data": {"a": 1}}),
        )
        .await;
        h.handle_message(&c, json!({"action": "getMarketLists", "requestId": 6}))
            .await;
        let msgs = rec.for_topic("cs_config_save");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["user"], "user7");
        assert_eq!(msgs[0]["uuid"], "WSS#T#1");
        assert_eq!(msgs[0]["userId"], 7);
        assert_eq!(msgs[0]["userRole"], "USER");
        assert_eq!(msgs[0]["cfg"], json!({"a": 1, "requestId": 5}));
        assert!(msgs[0]["time"].as_str().unwrap().ends_with('Z'));
        assert_eq!(
            msgs[1]["cfg"],
            json!({"requestId": 6, "mode": "getMarketLists"})
        );
    }

    #[tokio::test]
    async fn other_actions_go_to_their_topics() {
        let (h, rec) = handler();
        let (c, _rx) = logged_in(&h, 7, "USER");
        h.handle_message(&c, json!({"action": "abo_info", "data": {"x": 1}}))
            .await;
        h.handle_message(
            &c,
            json!({"action": "video_info", "data": {"mode": "play"}}),
        )
        .await;
        h.handle_message(&c, json!({"action": "video_info"})).await;
        h.handle_message(
            &c,
            json!({"action": "video_info", "data": {"noMode": true}}),
        )
        .await;
        h.handle_message(&c, json!({"action": "chart_alert", "data": {"alert": 1}}))
            .await;
        h.handle_message(
            &c,
            json!({"action": "priceAbo", "data": {"symbol": "X", "timeframe": "H1"}}),
        )
        .await;
        h.handle_message(&c, json!({"action": "video_info", "data": 0}))
            .await;
        h.handle_message(&c, json!({"action": "unknown"})).await;
        assert_eq!(rec.for_topic("cs_abo_info")[0]["cfg"], json!({"x": 1}));
        let videos = rec.for_topic("cs_video_info");
        assert_eq!(videos[0]["action"], "play");
        assert_eq!(videos[1]["action"], "req");
        assert!(videos[2].get("action").is_none());
        assert_eq!(videos[3]["action"], "req");
        assert_eq!(videos[3]["cfg"], 0);
        assert_eq!(rec.for_topic("cs_alert")[0]["cfg"], json!({"alert": 1}));
    }

    #[tokio::test]
    async fn change_password_hashes_new_password() {
        let (h, rec) = handler();
        let (c, _rx) = logged_in(&h, 7, "USER");
        h.handle_message(
            &c,
            json!({"action": "user", "data": {"password": "new", "oldPassword": "old"}}),
        )
        .await;
        let m = &rec.for_topic("cs_user_save")[0];
        assert_eq!(m["cfg"]["mode"], "changePwd");
        assert_eq!(m["cfg"]["userId"], 7);
        assert_eq!(m["cfg"]["oldPwd"], "old");
        assert!(m["cfg"]["newPwd"].as_str().unwrap().starts_with("$2"));
        // without password Node still sent the message, just without newPwd / oldPwd
        h.handle_message(&c, json!({"action": "user", "data": {}}))
            .await;
        let msgs = rec.for_topic("cs_user_save");
        assert_eq!(msgs.len(), 2);
        assert!(msgs[1]["cfg"].get("newPwd").is_none());
        assert!(msgs[1]["cfg"].get("oldPwd").is_none());
        assert_eq!(msgs[1]["cfg"]["mode"], "changePwd");
        // without a data object Node threw before sending anything
        h.handle_message(&c, json!({"action": "user"})).await;
        assert_eq!(rec.for_topic("cs_user_save").len(), 2);
    }

    #[tokio::test]
    async fn mu_is_forwarded_to_other_sessions_of_same_user() {
        let (h, _) = handler();
        let (a, mut ra) = logged_in(&h, 7, "USER");
        let (_b, mut rb) = logged_in(&h, 7, "USER");
        let (_c, mut rc) = logged_in(&h, 8, "USER");
        h.handle_message(&a, json!({"action": "mu", "data": {"v": 1}}))
            .await;
        assert!(drain(&mut ra).is_empty());
        let m = drain(&mut rb);
        assert_eq!(m[0]["action"], "mu");
        assert_eq!(m[0]["data"], json!({"v": 1}));
        assert!(drain(&mut rc).is_empty());
    }

    #[tokio::test]
    async fn instant_messages_are_broadcast_and_replayed() {
        let (h, _) = handler();
        let (a, mut ra) = h.register_client("ip".into(), "ua".into());
        a.apply_login(Some(LoginInfo {
            user_id: 1,
            user_name: "A".into(),
            user_role: "USER".into(),
            instant_messenger_id: 5,
            instant_messenger_active: true,
        }));
        let (_b, mut rb) = logged_in(&h, 2, "USER"); // messenger inactive
        h.handle_message(
            &a,
            json!({"action": "sendInstantMessage", "data": {"text": "hi"}}),
        )
        .await;
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "updInstantMessage");
        let inner: Value = serde_json::from_str(m[0]["data"]["msg"].as_str().unwrap()).unwrap();
        assert_eq!(inner["text"], "hi");
        assert_eq!(inner["from"]["first_name"], "A");
        assert!(drain(&mut rb).is_empty());

        // a later messenger login gets the history
        let (c, mut rc) = h.register_client("ip".into(), "ua".into());
        c.apply_login(Some(LoginInfo {
            user_id: 3,
            user_name: "C".into(),
            user_role: "USER".into(),
            instant_messenger_id: 9,
            instant_messenger_active: true,
        }));
        h.send_instant_messenger_messages(&c);
        assert_eq!(drain(&mut rc).len(), 1);
    }

    #[test]
    fn heartbeat_sweep_counts_and_removes() {
        let (h, rec) = handler();
        let (a, mut ra) = logged_in(&h, 1, "ADMIN");
        let (b, mut rb) = logged_in(&h, 1, "ADMIN");
        let (c, mut rc) = logged_in(&h, 2, "USER");
        c.set_last_msg_ms(chrono::Utc::now().timestamp_millis() - CLIENT_TIMEOUT_MS - 1000);
        let stats = h.check_heartbeat_timeouts();
        assert_eq!(stats.connections, 3);
        assert_eq!(stats.distinct_users, 2);
        assert_eq!(stats.distinct_admins, 1);
        assert_eq!(stats.removed, vec![c.id]);
        assert_eq!(stats.systems.get("MACOS"), Some(&3));
        assert_eq!(h.client_count(), 2);
        assert_eq!(drain(&mut ra)[0]["action"], "resHeartbeat");
        assert_eq!(drain(&mut rb)[0]["action"], "resHeartbeat");
        assert_eq!(rc.try_recv().unwrap(), OutMsg::Close);
        let prices = rec.for_topic("prices");
        assert_eq!(prices.len(), 3);
        assert_eq!(prices[0]["i"], "USERS.N1");
        assert_eq!(prices[0]["p"][0], 2);
        assert_eq!(prices[1]["p"][0], 3);
        assert_eq!(prices[2]["i"], "WS.SYSTEM.N1.MACOS");
        let _ = (a, b);
    }

    #[test]
    fn heartbeat_sweep_keeps_clients_with_recent_pong() {
        let (h, _) = handler();
        let (stale, mut rs) = logged_in(&h, 1, "USER");
        let (ponging, mut rp) = logged_in(&h, 2, "USER");
        let old = chrono::Utc::now().timestamp_millis() - CLIENT_TIMEOUT_MS - 1000;
        stale.set_last_msg_ms(old);
        ponging.set_last_msg_ms(old);
        ponging.record_pong();
        assert!(ponging.last_activity_ms() > old);
        assert_eq!(stale.last_activity_ms(), old);

        let stats = h.check_heartbeat_timeouts();
        assert_eq!(stats.removed, vec![stale.id]);
        assert_eq!(h.client_count(), 1);
        assert_eq!(rs.try_recv().unwrap(), OutMsg::Close);
        assert_eq!(drain(&mut rp)[0]["action"], "resHeartbeat");
    }

    #[test]
    fn merge_live_update_fields() {
        let mut m = json!({
            "userId": 5, "time": "t", "views": "v", "headline": "H", "inactive": true,
            "cfg": {"title": "T", "abos": [1]}
        })
        .as_object()
        .unwrap()
        .clone();
        merge_live_update(&mut m);
        let cfg = &m["cfg"];
        assert_eq!(cfg["user"], 5);
        assert_eq!(cfg["time"], "t");
        assert_eq!(cfg["views"], "v");
        assert_eq!(cfg["headline"], "H");
        assert_eq!(cfg["title"], "T");
        assert_eq!(cfg["abos"], json!([1]));
        assert_eq!(cfg["inactive"], true);
        assert!(cfg.get("description").is_none());

        // cfg.inactive wins over msg.inactive; user falls back to `user`
        let mut m = json!({"user": "u", "inactive": true, "cfg": {"inactive": false}})
            .as_object()
            .unwrap()
            .clone();
        merge_live_update(&mut m);
        assert_eq!(m["cfg"]["inactive"], false);
        assert_eq!(m["cfg"]["user"], "u");

        // `cfg.user = userId || user` with both missing removes an existing cfg.user;
        // `cfg.time = msg.time` removes cfg.time when msg.time is absent
        let mut m = json!({"cfg": {"user": "old", "time": "t0", "title": "", "headline": "H"}})
            .as_object()
            .unwrap()
            .clone();
        merge_live_update(&mut m);
        assert!(m["cfg"].get("user").is_none());
        assert!(m["cfg"].get("time").is_none());
        assert!(
            m["cfg"].get("title").is_none(),
            "falsy title with no fallback disappears"
        );
        assert_eq!(m["cfg"]["headline"], "H");
    }

    #[test]
    fn kafka_routing_user_ids() {
        let (h, _) = handler();
        let (_a, mut ra) = logged_in(&h, 1, "USER");
        let (_b, mut rb) = logged_in(&h, 2, "USER");
        h.on_kafka_message("cs_user_update", r#"{"userIds":[2],"data":{"x":1}}"#);
        assert!(drain(&mut ra).is_empty());
        let m = drain(&mut rb);
        assert_eq!(m[0]["action"], "user_update");
        assert_eq!(m[0]["data"], json!({"x": 1}));

        // string ids never match (`===`), falsy data becomes {}
        h.on_kafka_message("cs_user_update", r#"{"userIds":["2", 1],"data":null}"#);
        assert!(drain(&mut rb).is_empty());
        let m = drain(&mut ra);
        assert_eq!(m[0]["data"], json!({}));

        // a topic of 3 characters or less skips the userIds branch and takes the generic path
        h.on_kafka_message("abc", r#"{"userIds":[2],"userId":0,"data":{"y":1}}"#);
        assert!(drain(&mut ra).is_empty());
        assert!(drain(&mut rb).is_empty());
        // an empty array is truthy in JS: userIds branch, nobody matches
        h.on_kafka_message("cs_x", r#"{"userIds":[],"userId":0,"data":{"y":1}}"#);
        assert!(drain(&mut ra).is_empty());
        assert!(drain(&mut rb).is_empty());
        // a falsy userIds takes the generic path (userId 0 -> everyone)
        h.on_kafka_message("cs_x", r#"{"userIds":null,"userId":0,"data":{"y":1}}"#);
        assert_eq!(drain(&mut ra)[0]["action"], "x");
        assert_eq!(drain(&mut rb)[0]["data"], json!({"y": 1}));
    }

    #[test]
    fn kafka_routing_alert_update() {
        let (h, _) = handler();
        let (a, mut ra) = logged_in(&h, 1, "USER");
        let (_b, mut rb) = logged_in(&h, 2, "USER");
        let payload = format!(r#"{{"uuid":"{}"}}"#, a.uuid);
        h.on_kafka_message("cs_alert_update", &payload);
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "updAlert");
        assert_eq!(m[0]["data"]["msg"], payload);
        assert!(drain(&mut rb).is_empty());
        h.on_kafka_message("cs_alert_update", r#"{"uuid":""}"#);
        assert_eq!(drain(&mut ra).len(), 1);
        assert_eq!(drain(&mut rb).len(), 1);
        // missing uuid: nobody
        h.on_kafka_message("cs_alert_update", r#"{}"#);
        assert!(drain(&mut ra).is_empty());
    }

    #[test]
    fn kafka_routing_live_update() {
        let (h, _) = handler();
        let (a, mut ra) = logged_in(&h, 1, "USER");
        let (_b, mut rb) = logged_in(&h, 2, "USER");

        // resLastView only to the matching uuid
        h.on_kafka_message(
            "cs_config_live_update",
            &format!(
                r#"{{"uuid":"{}","cfg":{{"mode":"resLastView","v":1}}}}"#,
                a.uuid
            ),
        );
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "resLastView");
        assert_eq!(m[0]["data"]["v"], 1);
        assert!(drain(&mut rb).is_empty());

        // uuid == "" -> everyone gets updLiveChart with merged cfg
        h.on_kafka_message(
            "cs_config_live_update",
            r#"{"uuid":"","userId":9,"cfg":{"mode":"full"}}"#,
        );
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "updLiveChart");
        assert_eq!(m[0]["data"]["user"], 9);
        assert_eq!(drain(&mut rb).len(), 1);

        // mode part without uuid_org -> everyone except the origin uuid
        h.on_kafka_message(
            "cs_config_live_update",
            &format!(r#"{{"uuid":"{}","cfg":{{"mode":"part"}}}}"#, a.uuid),
        );
        assert!(drain(&mut ra).is_empty());
        assert_eq!(drain(&mut rb).len(), 1);

        // mode part with uuid_org -> only the origin uuid
        h.on_kafka_message(
            "cs_config_live_update",
            &format!(
                r#"{{"uuid":"{}","uuid_org":"x","cfg":{{"mode":"part"}}}}"#,
                a.uuid
            ),
        );
        assert_eq!(drain(&mut ra).len(), 1);
        assert!(drain(&mut rb).is_empty());

        // no cfg and no empty uuid -> nobody
        h.on_kafka_message("cs_config_live_update", r#"{"uuid":"zzz"}"#);
        // merge_live_update always creates cfg, so the "cfg exists" branch applies:
        // (part && !org)=false != (same uuid)=false -> false -> nobody
        assert!(drain(&mut ra).is_empty());
        assert!(drain(&mut rb).is_empty());
    }

    #[test]
    fn kafka_routing_generic_topics() {
        let (h, _) = handler();
        let (a, mut ra) = logged_in(&h, 1, "USER");
        let (_b, mut rb) = logged_in(&h, 2, "USER");

        // no uuid + matching userId (as string)
        h.on_kafka_message("cs_tools_update", r#"{"userId":"1","data":{"action":"x"}}"#);
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "tools_update");
        assert_eq!(m[0]["data"]["action"], "x");
        assert!(drain(&mut rb).is_empty());

        // userId 0 -> everyone
        h.on_kafka_message("cs_tools_update", r#"{"userId":0,"data":{}}"#);
        assert_eq!(drain(&mut ra).len(), 1);
        assert_eq!(drain(&mut rb).len(), 1);

        // public flag -> everyone
        h.on_kafka_message(
            "cs_tools_update",
            r#"{"uuid":"other","userId":5,"data":{"isPublic":true}}"#,
        );
        assert_eq!(drain(&mut ra).len(), 1);
        assert_eq!(drain(&mut rb).len(), 1);

        // nested userTools public flag
        h.on_kafka_message(
            "cs_tools_update",
            r#"{"uuid":"other","userId":5,"data":{"data":{"userTools":[{"userTool":{"isPublic":1}}]}}}"#,
        );
        assert_eq!(drain(&mut ra).len(), 1);
        assert_eq!(drain(&mut rb).len(), 1);

        // uuid match only
        h.on_kafka_message(
            "cs_tools_update",
            &format!(r#"{{"uuid":"{}","userId":5,"data":{{}}}}"#, a.uuid),
        );
        assert_eq!(drain(&mut ra).len(), 1);
        assert!(drain(&mut rb).is_empty());

        // short topic name: nothing
        h.on_kafka_message("cs", r#"{"userId":0,"data":{}}"#);
        assert!(drain(&mut ra).is_empty());

        // invalid json: no panic, nothing sent
        h.on_kafka_message("cs_tools_update", "not json");
        assert!(drain(&mut ra).is_empty());
    }

    #[test]
    fn prices_broadcast() {
        let (h, _) = handler();
        let (_a, mut ra) = logged_in(&h, 1, "USER");
        h.on_prices_message("updPrice", &json!({"c": 1.5}));
        let m = drain(&mut ra);
        assert_eq!(m[0]["action"], "updPrice");
        assert_eq!(m[0]["data"]["c"], 1.5);
    }
}
