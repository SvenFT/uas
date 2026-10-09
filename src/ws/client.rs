//! Port of `src/connection/ApiConnectionClient.js`: per-connection state and the outgoing
//! message envelope `{action, requestId, data}`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use serde_json::{Value, json};
use tokio::sync::mpsc;

/// Actions whose name is moved from `data.action` into the envelope's `action` field.
pub const MARKET_LIST_ACTIONS: &[&str] = &[
    "setMarketList",
    "getMarketLists",
    "deleteMarketLists",
    "setMarketListDetail",
    "updateMarketListDetail",
    "deleteMarketListDetail",
    "getMarketListDetails",
    "getLastView",
];

/// JavaScript truthiness for JSON values.
pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// JavaScript `value + 0` for the truthy values that reach the envelope.
fn js_plus_zero(v: Value) -> Value {
    match v {
        Value::String(s) => Value::String(format!("{s}0")),
        Value::Bool(true) => json!(1),
        other => other,
    }
}

/// `sendMessage(type, msg)`: deep-copies `msg`, strips `requestId` (which becomes the
/// envelope's `requestId`, default `0`) and, for market-list actions, promotes `data.action`
/// to the envelope action.
pub fn build_envelope(action: &str, data: Value) -> String {
    let mut data = data;
    let mut send_type = action.to_string();
    let mut request_id = json!(0);
    if let Some(obj) = data.as_object_mut() {
        if let Some(rid) = obj.shift_remove("requestId")
            && truthy(&rid)
        {
            request_id = js_plus_zero(rid);
        }
        let promote = matches!(obj.get("action"), Some(Value::String(a)) if MARKET_LIST_ACTIONS.contains(&a.as_str()));
        if promote && let Some(Value::String(a)) = obj.shift_remove("action") {
            send_type = a;
        }
    }
    json!({"action": send_type, "requestId": request_id, "data": data}).to_string()
}

/// Payload of the `login` message on success (constant in the Node server).
pub fn login_ok_payload(use_messenger: bool) -> Value {
    json!({
        "result": "ok",
        "useMessanger": use_messenger,
        "rights": [
            {"id": 1, "type": "SYMBOLS", "subtype": 1},
            {"id": 2, "type": "INDICATORS", "subtype": 1},
            {"id": 3, "type": "PUBLISH", "subtype": 2},
            {"id": 4, "type": "LAYOUT", "subtype": 2},
            {"id": 5, "type": "TEMPLATES", "subtype": 2},
            {"id": 6, "type": "CHARTS", "subtype": 2}
        ],
        "abos": [
            {"id": 1, "title": "Fibopaket", "name": "fibos", "publish": true, "state": 1, "aboUntil": 1680544059000i64},
            {"id": 2, "title": "Clusterpaket", "name": "clusters", "publish": true, "state": 1, "aboUntil": 0},
            {"id": 3, "title": "Analysenpaket", "name": "performance-depot", "publish": true, "state": 0, "aboUntil": 0},
            {"id": 4, "title": "Plus-Paket", "name": "plus-paket", "publish": true, "state": 0, "aboUntil": 0},
            {"id": 5, "title": "Chartsoftware-Plus", "name": "cs-plus", "publish": false, "state": 0, "aboUntil": 0}
        ]
    })
}

pub fn login_failed_payload() -> Value {
    json!({"result": "failed"})
}

/// Outgoing frame for the connection writer task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutMsg {
    Text(String),
    Close,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoginInfo {
    pub user_id: i64,
    pub user_name: String,
    pub user_role: String,
    pub instant_messenger_id: i64,
    /// Node semantics: active unless `TELEGRAM_USER_ID` in the token is exactly `0`.
    pub instant_messenger_active: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DaxenUser {
    pub in_groups: Vec<i64>,
}

#[derive(Default)]
struct ClientState {
    login: LoginInfo,
    logged_in: bool,
    daxen: Option<DaxenUser>,
}

pub struct Client {
    pub id: u64,
    /// `<systemId>#<clientId>`
    pub uuid: String,
    pub ip_address: String,
    pub user_agent: String,
    tx: mpsc::UnboundedSender<OutMsg>,
    state: Mutex<ClientState>,
    last_msg_ms: AtomicI64,
    last_pong_ms: AtomicI64,
    pong_count: AtomicU64,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Client {
    pub fn new(
        system_id: &str,
        id: u64,
        ip_address: String,
        user_agent: String,
        tx: mpsc::UnboundedSender<OutMsg>,
    ) -> Self {
        Client {
            id,
            uuid: format!("{system_id}#{id}"),
            ip_address,
            user_agent,
            tx,
            state: Mutex::new(ClientState::default()),
            last_msg_ms: AtomicI64::new(now_ms()),
            last_pong_ms: AtomicI64::new(0),
            pong_count: AtomicU64::new(0),
        }
    }

    /// Queue an enveloped message; returns `false` when the connection is gone.
    pub fn send_message(&self, action: &str, data: Value) -> bool {
        self.tx
            .send(OutMsg::Text(build_envelope(action, data)))
            .is_ok()
    }

    pub fn close(&self) {
        let _ = self.tx.send(OutMsg::Close);
    }

    pub fn touch(&self) {
        self.last_msg_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn last_msg_ms(&self) -> i64 {
        self.last_msg_ms.load(Ordering::Relaxed)
    }

    /// A protocol-level PONG arrived (reply to the server's PING). Together with the last
    /// application message it defines [`Client::last_activity_ms`], which the sweep uses.
    pub fn record_pong(&self) {
        self.last_pong_ms.store(now_ms(), Ordering::Relaxed);
        self.pong_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Timestamp (ms) of the last PONG, `0` if none was received yet.
    pub fn last_pong_ms(&self) -> i64 {
        self.last_pong_ms.load(Ordering::Relaxed)
    }

    pub fn pong_count(&self) -> u64 {
        self.pong_count.load(Ordering::Relaxed)
    }

    /// Most recent sign of life: the later of the last application message and the last PONG.
    /// A hidden browser tab keeps answering PINGs even when its JavaScript is frozen, so it
    /// is not dropped by the heartbeat sweep.
    pub fn last_activity_ms(&self) -> i64 {
        self.last_msg_ms().max(self.last_pong_ms())
    }

    #[cfg(test)]
    pub fn set_last_msg_ms(&self, ms: i64) {
        self.last_msg_ms.store(ms, Ordering::Relaxed);
    }

    /// Age the client so heartbeat checks treat it as timed out (tests / admin tooling).
    pub fn backdate_last_msg(&self, by_ms: i64) {
        self.last_msg_ms.fetch_sub(by_ms, Ordering::Relaxed);
    }

    pub fn apply_login(&self, info: Option<LoginInfo>) -> bool {
        let mut st = self.state.lock().unwrap();
        st.login = info.unwrap_or_default();
        st.logged_in = st.login.user_id > 0;
        st.logged_in
    }

    pub fn is_logged_in(&self) -> bool {
        self.state.lock().unwrap().logged_in
    }

    pub fn login(&self) -> LoginInfo {
        self.state.lock().unwrap().login.clone()
    }

    pub fn user_id(&self) -> i64 {
        self.state.lock().unwrap().login.user_id
    }

    pub fn user_name(&self) -> String {
        self.state.lock().unwrap().login.user_name.clone()
    }

    pub fn user_role(&self) -> String {
        self.state.lock().unwrap().login.user_role.clone()
    }

    pub fn instant_messenger_id(&self) -> i64 {
        self.state.lock().unwrap().login.instant_messenger_id
    }

    pub fn is_instant_messenger_active(&self) -> bool {
        self.state.lock().unwrap().login.instant_messenger_active
    }

    pub fn daxen(&self) -> Option<DaxenUser> {
        self.state.lock().unwrap().daxen.clone()
    }

    pub fn set_daxen(&self, d: DaxenUser) {
        self.state.lock().unwrap().daxen = Some(d);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: String) -> Value {
        serde_json::from_str(&s).unwrap()
    }

    #[test]
    fn envelope_defaults() {
        let v = parse(build_envelope("resHeartbeat", json!({})));
        assert_eq!(
            v,
            json!({"action": "resHeartbeat", "requestId": 0, "data": {}})
        );
    }

    #[test]
    fn envelope_moves_request_id() {
        let v = parse(build_envelope(
            "updLiveChart",
            json!({"requestId": 7, "x": 1}),
        ));
        assert_eq!(v["requestId"], 7);
        assert_eq!(v["data"], json!({"x": 1}));
        // falsy requestId stays 0 but is still removed from data
        let v = parse(build_envelope("a", json!({"requestId": 0, "x": 1})));
        assert_eq!(v["requestId"], 0);
        assert_eq!(v["data"], json!({"x": 1}));
    }

    #[test]
    fn envelope_promotes_market_list_actions() {
        let v = parse(build_envelope(
            "updLiveChart",
            json!({"action": "getMarketLists", "requestId": 3, "lists": []}),
        ));
        assert_eq!(v["action"], "getMarketLists");
        assert_eq!(v["requestId"], 3);
        assert_eq!(v["data"], json!({"lists": []}));
        // other actions are left alone
        let v = parse(build_envelope(
            "updLiveChart",
            json!({"action": "somethingElse"}),
        ));
        assert_eq!(v["action"], "updLiveChart");
        assert_eq!(v["data"], json!({"action": "somethingElse"}));
    }

    #[test]
    fn envelope_request_id_follows_js_plus_zero() {
        // Node: `data.requestId + 0` – strings are concatenated, numbers stay numbers
        let v = parse(build_envelope("a", json!({"requestId": "5"})));
        assert_eq!(v["requestId"], "50");
        let v = parse(build_envelope("a", json!({"requestId": true})));
        assert_eq!(v["requestId"], 1);
        let v = parse(build_envelope("a", json!({"requestId": 2.5})));
        assert_eq!(v["requestId"], 2.5);
    }

    #[test]
    fn envelope_with_non_object_data() {
        let v = parse(build_envelope("updAlert", json!("raw")));
        assert_eq!(
            v,
            json!({"action": "updAlert", "requestId": 0, "data": "raw"})
        );
    }

    #[test]
    fn truthiness() {
        assert!(!truthy(&Value::Null));
        assert!(!truthy(&json!(0)));
        assert!(!truthy(&json!("")));
        assert!(!truthy(&json!(false)));
        assert!(truthy(&json!([])));
        assert!(truthy(&json!({})));
        assert!(truthy(&json!("0")));
        assert!(truthy(&json!(-1)));
    }

    #[test]
    fn client_state_and_send() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let c = Client::new("WSS#001", 5, "1.2.3.4".into(), "ua".into(), tx);
        assert_eq!(c.uuid, "WSS#001#5");
        assert!(!c.is_logged_in());
        assert!(c.apply_login(Some(LoginInfo {
            user_id: 9,
            user_name: "n".into(),
            user_role: "USER".into(),
            instant_messenger_id: 0,
            instant_messenger_active: false,
        })));
        assert!(c.is_logged_in());
        assert_eq!(c.user_id(), 9);
        assert!(!c.is_instant_messenger_active());
        assert!(!c.apply_login(None));
        assert!(!c.is_logged_in());
        assert!(c.send_message("x", json!({"a": 1})));
        match rx.try_recv().unwrap() {
            OutMsg::Text(t) => assert_eq!(parse(t)["action"], "x"),
            OutMsg::Close => panic!(),
        }
        c.close();
        assert_eq!(rx.try_recv().unwrap(), OutMsg::Close);
        drop(rx);
        assert!(!c.send_message("x", json!({})));
    }

    #[test]
    fn login_payload_shape() {
        let p = login_ok_payload(true);
        assert_eq!(p["result"], "ok");
        assert_eq!(p["useMessanger"], true);
        assert_eq!(p["rights"].as_array().unwrap().len(), 6);
        assert_eq!(p["abos"].as_array().unwrap().len(), 5);
        assert_eq!(login_failed_payload(), json!({"result": "failed"}));
    }
}
