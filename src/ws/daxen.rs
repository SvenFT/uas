//! Port of `src/handler/DaxenHandler.js` (static demo data for the "daxen" feature).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Value, json};

use super::client::{Client, DaxenUser};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Group {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveRun {
    pub id: i64,
    pub group_id: i64,
    pub symbol: String,
    pub open_until: i64,
    pub price_time: i64,
}

pub struct DaxenHandler {
    groups: BTreeMap<i64, Group>,
    active_runs: BTreeMap<i64, ActiveRun>,
}

impl Default for DaxenHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl DaxenHandler {
    pub fn new() -> Self {
        let mut h = DaxenHandler {
            groups: BTreeMap::new(),
            active_runs: BTreeMap::new(),
        };
        h.init();
        h
    }

    fn init(&mut self) {
        self.groups.clear();
        for (id, name) in [
            (1, "Admins"),
            (2, "FormationsTrader-Admins"),
            (3, "FormationsTrader-User"),
            (4, "FormationsTrader-VIPs"),
        ] {
            self.groups.insert(
                id,
                Group {
                    id,
                    name: name.into(),
                },
            );
        }
        let runs = [
            (1, 2, 1637345542000, 1637355600000),
            (2, 4, 1637571600000, 1637614800000),
            (3, 4, 1637744400000, 1637787600000),
        ];
        for (id, group_id, open_until, price_time) in runs {
            self.active_runs.insert(
                id,
                ActiveRun {
                    id,
                    group_id,
                    symbol: "ETHUSDT.BINANCE".into(),
                    open_until,
                    price_time,
                },
            );
        }
    }

    pub fn handle_message(&self, client: &Client, data: &Value) {
        if data.get("type").and_then(Value::as_str) == Some("abo") {
            let daxen = client.daxen().unwrap_or_else(|| {
                let d = DaxenUser {
                    in_groups: vec![1, 2, 4],
                };
                client.set_daxen(d.clone());
                d
            });
            let groups: Vec<&Group> = self
                .groups
                .values()
                .filter(|g| daxen.in_groups.contains(&g.id))
                .collect();
            client.send_message(
                "daxen",
                json!({
                    "type": "info",
                    "user": {"inGroups": daxen.in_groups},
                    "general": {"groups": groups},
                }),
            );
            self.send_active_runs(client);
        }
    }

    pub fn send_active_run(&self, client: &Client, run: &ActiveRun) {
        if let Some(d) = client.daxen()
            && d.in_groups.contains(&run.group_id)
        {
            client.send_message("daxen", json!({"type": "activeRun", "data": run}));
        }
    }

    pub fn send_active_runs(&self, client: &Client) {
        for run in self.active_runs.values() {
            self.send_active_run(client, run);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws::client::OutMsg;
    use tokio::sync::mpsc;

    fn client() -> (Client, mpsc::UnboundedReceiver<OutMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Client::new("S", 1, "ip".into(), "ua".into(), tx), rx)
    }

    fn drain(rx: &mut mpsc::UnboundedReceiver<OutMsg>) -> Vec<Value> {
        let mut out = vec![];
        while let Ok(OutMsg::Text(t)) = rx.try_recv() {
            out.push(serde_json::from_str(&t).unwrap());
        }
        out
    }

    #[test]
    fn abo_sends_info_and_runs_for_default_groups() {
        let h = DaxenHandler::new();
        let (c, mut rx) = client();
        h.handle_message(&c, &json!({"type": "abo"}));
        let msgs = drain(&mut rx);
        // info + 3 active runs (groups 2, 4, 4 are all within [1,2,4])
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0]["data"]["type"], "info");
        assert_eq!(msgs[0]["data"]["user"], json!({"inGroups": [1, 2, 4]}));
        let names: Vec<&str> = msgs[0]["data"]["general"]["groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["Admins", "FormationsTrader-Admins", "FormationsTrader-VIPs"]
        );
        assert_eq!(msgs[1]["data"]["type"], "activeRun");
        assert_eq!(msgs[1]["data"]["data"]["symbol"], "ETHUSDT.BINANCE");
        assert_eq!(msgs[1]["data"]["data"]["openUntil"], 1637345542000i64);
    }

    #[test]
    fn runs_are_filtered_by_group_membership() {
        let h = DaxenHandler::new();
        let (c, mut rx) = client();
        c.set_daxen(DaxenUser { in_groups: vec![2] });
        h.handle_message(&c, &json!({"type": "abo"}));
        let msgs = drain(&mut rx);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[1]["data"]["data"]["id"], 1);
    }

    #[test]
    fn other_types_are_ignored() {
        let h = DaxenHandler::new();
        let (c, mut rx) = client();
        h.handle_message(&c, &json!({"type": "other"}));
        assert!(drain(&mut rx).is_empty());
    }
}
