//! Versand über den Produce-Modus des kafkaproxy (rohes TCP, z. B. mcl.anfin.com:9900) in dessen
//! Kafka – für den uas auf IONOS, der das alte Kafka (Topic `prices`, Nutzerzahlen für hiku/Charts)
//! nicht direkt erreicht. Protokoll (Big-Endian, siehe kafkaproxy `src/wire.rs`):
//!
//! ```text
//! Handshake  [u32 len][u8 4][u8 token_len][token]            -> Ack mit Text "ready"
//! Nachricht  [u32 len][u8 5][u8 topic_len][topic][u16 key_len][key][payload]
//! Antwort    [u32 len][u8 6][u8 status][i32 partition][i64 offset][u16 text_len][text]
//! ```
//!
//! Der Server trennt ohne Antwort, wenn Token, Absender oder Modus nicht passen. Nachrichten gehen
//! ohne Warten hintereinander raus (Pipelining, Fenster `window`), der Server bestätigt in Reihenfolge.
//! Ohne Ack innerhalb der Frist oder bei Verbindungsfehler wird neu verbunden und jede unbestätigte
//! Nachricht genau einmal wiederholt (Duplikat möglich, Verlust nicht – außer beim zweiten Fehlschlag).
//! `enqueue` blockiert nie; volle Warteschlange = verwerfen und zählen. Das Token kommt nur aus der
//! Umgebung (`KAFKAPROXY_TOKEN`) und steht nie im Log. Gleiches Verhalten wie `anfin/utils/kafkaproxy`
//! (Go) in fixserver und Configserver.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, watch};

pub const MSG_HELLO_PRODUCE: u8 = 4;
pub const MSG_PRODUCE: u8 = 5;
pub const MSG_PRODUCE_ACK: u8 = 6;
pub const ACK_OK: u8 = 0;
pub const ACK_ERROR: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct Msg {
    pub topic: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ack {
    pub status: u8,
    pub partition: i32,
    pub offset: i64,
    pub text: String,
}

/// (eingereiht, unterwegs, bestätigt, verloren, verworfen, neu verbunden)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Stats {
    pub queued: i64,
    pub pending: i64,
    pub acked: i64,
    pub failed: i64,
    pub dropped: i64,
    pub reconnects: i64,
}

#[derive(Clone)]
struct Inflight {
    m: Msg,
    retried: bool,
}

pub struct ProxySink {
    addr: String,
    token: String,
    tx: mpsc::Sender<Msg>,
    rx: tokio::sync::Mutex<Option<mpsc::Receiver<Msg>>>,
    window: usize,
    queued: AtomicI64,
    acked: AtomicI64,
    failed: AtomicI64,
    dropped: AtomicI64,
    reconnects: AtomicI64,
    last_log: AtomicI64,
    last_status: AtomicI64,
    pub ack_timeout: Duration,
    pub backoff_max: Duration,
    pub status_secs: i64,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl ProxySink {
    /// `pending_max` Kapazität der Warteschlange (0 = 5000), `window` unbestätigte Nachrichten (0 = 200).
    pub fn new(addr: &str, token: &str, pending_max: usize, window: usize) -> Arc<ProxySink> {
        let pending_max = if pending_max == 0 { 5000 } else { pending_max };
        let (tx, rx) = mpsc::channel(pending_max);
        Arc::new(ProxySink {
            addr: addr.to_string(),
            token: token.trim().to_string(),
            tx,
            rx: tokio::sync::Mutex::new(Some(rx)),
            window: if window == 0 { 200 } else { window },
            queued: AtomicI64::new(0),
            acked: AtomicI64::new(0),
            failed: AtomicI64::new(0),
            dropped: AtomicI64::new(0),
            reconnects: AtomicI64::new(0),
            last_log: AtomicI64::new(0),
            last_status: AtomicI64::new(0),
            ack_timeout: Duration::from_secs(10),
            backoff_max: Duration::from_secs(30),
            status_secs: 600,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn window(&self) -> usize {
        self.window
    }

    pub fn queue_cap(&self) -> usize {
        self.tx.max_capacity()
    }

    /// Startet die Sende-Aufgabe (einmal).
    pub fn start(self: &Arc<Self>) {
        let me = self.clone();
        tokio::spawn(async move { me.run().await });
    }

    /// Reiht ein, ohne zu blockieren. false = Warteschlange voll, verworfen.
    pub fn enqueue(&self, topic: &str, key: &[u8], payload: &[u8]) -> bool {
        let m = Msg { topic: topic.to_string(), key: key.to_vec(), payload: payload.to_vec() };
        match self.tx.try_send(m) {
            Ok(()) => {
                self.queued.fetch_add(1, Ordering::SeqCst);
                true
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                self.logf(&format!("kafkaproxy: Warteschlange voll ({}), verwerfe", self.queue_cap()));
                false
            }
        }
    }

    pub fn stats(&self) -> Stats {
        let queued = self.queued.load(Ordering::SeqCst);
        let acked = self.acked.load(Ordering::SeqCst);
        let failed = self.failed.load(Ordering::SeqCst);
        Stats {
            queued,
            pending: queued - acked - failed,
            acked,
            failed,
            dropped: self.dropped.load(Ordering::SeqCst),
            reconnects: self.reconnects.load(Ordering::SeqCst),
        }
    }

    async fn run(self: Arc<Self>) {
        let mut rx = match self.rx.lock().await.take() {
            Some(rx) => rx,
            None => return, // schon gestartet
        };
        let mut backoff = Duration::from_secs(1);
        let mut carry: Vec<Inflight> = Vec::new();
        loop {
            let (rd, wr) = match self.connect().await {
                Ok(c) => c,
                Err(e) => {
                    self.logf(&format!("kafkaproxy: {} - neuer Versuch in {:?}", e, backoff));
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.backoff_max);
                    continue;
                }
            };
            backoff = Duration::from_secs(1);
            let unacked = self.session(rd, wr, &mut rx, std::mem::take(&mut carry)).await;
            self.reconnects.fetch_add(1, Ordering::SeqCst);
            for mut it in unacked {
                if it.retried {
                    self.failed.fetch_add(1, Ordering::SeqCst);
                    self.logf("kafkaproxy: Nachricht nach Wiederholung verloren");
                } else {
                    it.retried = true;
                    carry.push(it);
                }
            }
            if !carry.is_empty() {
                self.logf(&format!("kafkaproxy: Verbindung verloren, verbinde neu und wiederhole {} Nachrichten", carry.len()));
            }
        }
    }

    async fn connect(&self) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf), String> {
        let stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&self.addr))
            .await
            .map_err(|_| format!("Verbindung zu {}: Zeitüberschreitung", self.addr))?
            .map_err(|e| format!("Verbindung zu {}: {}", self.addr, e))?;
        stream.set_nodelay(true).ok();
        let (rd, mut wr) = stream.into_split();
        let mut rd = BufReader::new(rd);
        tokio::time::timeout(self.ack_timeout, wr.write_all(&encode_hello(self.token.as_bytes())))
            .await
            .map_err(|_| "Handshake senden: Zeitüberschreitung".to_string())?
            .map_err(|e| format!("Handshake senden: {}", e))?;
        match tokio::time::timeout(self.ack_timeout, read_ack(&mut rd)).await {
            Err(_) => Err("Handshake-Antwort: Zeitüberschreitung".into()),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Err(format!("Server {} hat getrennt: Token, Absender oder Produce-Modus abgelehnt", self.addr))
            }
            Ok(Err(e)) => Err(format!("Handshake-Antwort: {}", e)),
            Ok(Ok(a)) if a.status != ACK_OK => Err(format!("Handshake abgelehnt: {}", a.text)),
            Ok(Ok(a)) => {
                tracing::info!("kafkaproxy: verbunden mit {} ({}), Fenster {}", self.addr, a.text, self.window);
                Ok((rd, wr))
            }
        }
    }

    /// Schreiber (hier) und Leser (Task) teilen sich das Fenster `inflight`; der Leser nimmt die Acks in
    /// Reihenfolge ab. Bei Fehlern endet die Sitzung; zurück kommen die unbestätigten Nachrichten.
    async fn session(
        self: &Arc<Self>,
        rd: BufReader<OwnedReadHalf>,
        mut wr: OwnedWriteHalf,
        rx: &mut mpsc::Receiver<Msg>,
        carry: Vec<Inflight>,
    ) -> Vec<Inflight> {
        let (in_tx, in_rx) = mpsc::channel::<Inflight>(self.window);
        let (fail_tx, mut fail_rx) = watch::channel(false);
        let ack_timeout = self.ack_timeout;
        let reader = tokio::spawn(reader_task(self.clone(), rd, in_rx, fail_tx.clone(), ack_timeout));
        let mut unsent: Vec<Inflight> = Vec::new();
        let mut carry = carry.into_iter();
        let mut writer_fail = fail_rx.clone();
        loop {
            let it = match carry.next() {
                Some(it) => it,
                None => tokio::select! {
                    m = rx.recv() => match m { Some(m) => Inflight { m, retried: false }, None => break },
                    _ = writer_fail.changed() => break,
                },
            };
            tokio::select! {
                r = in_tx.send(it.clone()) => { if r.is_err() { unsent.push(it); break; } }
                _ = writer_fail.changed() => { unsent.push(it); break; }
            }
            let frame = encode_produce(&it.m.topic, &it.m.key, &it.m.payload);
            match tokio::time::timeout(ack_timeout, wr.write_all(&frame)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.logf(&format!("kafkaproxy: Senden fehlgeschlagen: {}", e));
                    let _ = fail_tx.send(true);
                    break;
                }
                Err(_) => {
                    self.logf("kafkaproxy: Senden fehlgeschlagen: Zeitüberschreitung");
                    let _ = fail_tx.send(true);
                    break;
                }
            }
        }
        let _ = fail_tx.send(true);
        drop(in_tx);
        drop(wr);
        let _ = fail_rx.changed().await;
        let mut rest = reader.await.unwrap_or_default();
        rest.extend(unsent);
        rest.extend(carry);
        rest
    }

    fn logf(&self, msg: &str) {
        let now = now_secs();
        let last = self.last_log.load(Ordering::SeqCst);
        if now - last < 60 || self.last_log.compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        let s = self.stats();
        tracing::warn!(
            "{} (unterwegs {}, bestaetigt {}, verloren {}, verworfen {}, gesamt {}, neu verbunden {})",
            msg, s.pending, s.acked, s.failed, s.dropped, s.queued, s.reconnects
        );
    }

    fn status(&self) {
        let now = now_secs();
        let last = self.last_status.load(Ordering::SeqCst);
        if now - last < self.status_secs
            || self.last_status.compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst).is_err()
        {
            return;
        }
        let s = self.stats();
        tracing::info!(
            "kafkaproxy: {} bestaetigt, {} unterwegs, {} verloren, {} verworfen, {} gesamt, {} mal neu verbunden",
            s.acked, s.pending, s.failed, s.dropped, s.queued, s.reconnects
        );
    }
}

/// Liest Acks für die Nachrichten im Fenster und zählt sie laufend; liefert die unbestätigten
/// Nachrichten (die beim Leser plus alle noch im Fenster), wenn die Sitzung endet.
async fn reader_task(
    sink: Arc<ProxySink>,
    mut rd: BufReader<OwnedReadHalf>,
    mut in_rx: mpsc::Receiver<Inflight>,
    fail_tx: watch::Sender<bool>,
    ack_timeout: Duration,
) -> Vec<Inflight> {
    let mut cur: Option<Inflight> = None;
    let mut fail_rx = fail_tx.subscribe();
    loop {
        let it = tokio::select! {
            it = in_rx.recv() => match it { Some(it) => it, None => break },
            _ = fail_rx.changed() => break,
        };
        cur = Some(it);
        match tokio::time::timeout(ack_timeout, read_ack(&mut rd)).await {
            Ok(Ok(a)) => {
                cur = None;
                if a.status == ACK_OK {
                    sink.acked.fetch_add(1, Ordering::SeqCst);
                } else {
                    sink.failed.fetch_add(1, Ordering::SeqCst);
                    sink.logf(&format!("kafkaproxy: Nachricht abgelehnt: {}", a.text));
                }
                sink.status();
            }
            Ok(Err(e)) => {
                sink.logf(&format!("kafkaproxy: keine Bestaetigung: {}", e));
                break;
            }
            Err(_) => {
                sink.logf("kafkaproxy: keine Bestaetigung: Zeitüberschreitung");
                break;
            }
        }
    }
    let _ = fail_tx.send(true);
    let mut rest: Vec<Inflight> = cur.into_iter().collect();
    in_rx.close();
    while let Ok(it) = in_rx.try_recv() {
        rest.push(it);
    }
    rest
}

// ---- Codec ----

pub fn encode_hello(token: &[u8]) -> Vec<u8> {
    let n = token.len().min(255);
    let mut out = Vec::with_capacity(6 + n);
    out.extend_from_slice(&((2 + n) as u32).to_be_bytes());
    out.push(MSG_HELLO_PRODUCE);
    out.push(n as u8);
    out.extend_from_slice(&token[..n]);
    out
}

pub fn encode_produce(topic: &str, key: &[u8], payload: &[u8]) -> Vec<u8> {
    let tl = topic.len().min(255);
    let kl = key.len().min(u16::MAX as usize);
    let len = 1 + 1 + tl + 2 + kl + payload.len();
    let mut out = Vec::with_capacity(4 + len);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.push(MSG_PRODUCE);
    out.push(tl as u8);
    out.extend_from_slice(&topic.as_bytes()[..tl]);
    out.extend_from_slice(&(kl as u16).to_be_bytes());
    out.extend_from_slice(&key[..kl]);
    out.extend_from_slice(payload);
    out
}

pub fn encode_ack(a: &Ack) -> Vec<u8> {
    let text = a.text.as_bytes();
    let tl = text.len().min(u16::MAX as usize);
    let len = 1 + 1 + 4 + 8 + 2 + tl;
    let mut out = Vec::with_capacity(4 + len);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.push(MSG_PRODUCE_ACK);
    out.push(a.status);
    out.extend_from_slice(&a.partition.to_be_bytes());
    out.extend_from_slice(&a.offset.to_be_bytes());
    out.extend_from_slice(&(tl as u16).to_be_bytes());
    out.extend_from_slice(&text[..tl]);
    out
}

pub async fn read_frame<R: AsyncReadExt + Unpin>(rd: &mut R) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    rd.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 || n > 1 << 20 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Rahmenlaenge {}", n)));
    }
    let mut body = vec![0u8; n];
    rd.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn read_ack<R: AsyncReadExt + Unpin>(rd: &mut R) -> std::io::Result<Ack> {
    let body = read_frame(rd).await?;
    decode_ack(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub fn decode_ack(body: &[u8]) -> Result<Ack, String> {
    if body.len() < 16 || body[0] != MSG_PRODUCE_ACK {
        return Err(format!("kein Ack ({} Bytes, Typ {})", body.len(), body.first().copied().unwrap_or(0)));
    }
    let tl = u16::from_be_bytes([body[14], body[15]]) as usize;
    if 16 + tl > body.len() {
        return Err("Ack-Text abgeschnitten".into());
    }
    Ok(Ack {
        status: body[1],
        partition: i32::from_be_bytes(body[2..6].try_into().unwrap()),
        offset: i64::from_be_bytes(body[6..14].try_into().unwrap()),
        text: String::from_utf8_lossy(&body[16..16 + tl]).into_owned(),
    })
}

/// MSG_PRODUCE-Rumpf zerlegen (Fake-Server in Tests).
pub fn decode_produce(body: &[u8]) -> Result<Msg, String> {
    if body.len() < 4 || body[0] != MSG_PRODUCE {
        return Err("kein MSG_PRODUCE-Rahmen".into());
    }
    let tl = body[1] as usize;
    if 4 + tl > body.len() {
        return Err("Topic abgeschnitten".into());
    }
    let topic = String::from_utf8_lossy(&body[2..2 + tl]).into_owned();
    let kl = u16::from_be_bytes([body[2 + tl], body[3 + tl]]) as usize;
    if 4 + tl + kl > body.len() {
        return Err("Schluessel abgeschnitten".into());
    }
    Ok(Msg { topic, key: body[4 + tl..4 + tl + kl].to_vec(), payload: body[4 + tl + kl..].to_vec() })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    /// Fake-Proxy: Token prüfen (falsch = trennen), "ready", je Rahmen ein Ack in Reihenfolge.
    /// drop_after: nach so vielen Rahmen einmalig ohne Ack trennen; reject_topic: Fehler-Ack.
    pub struct FakeProxy {
        pub addr: String,
        pub got: Arc<Mutex<Vec<Msg>>>,
        pub sessions: Arc<Mutex<usize>>,
    }

    pub async fn fake_proxy(token: &'static str, drop_after: usize, reject_topic: &'static str) -> FakeProxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let got = Arc::new(Mutex::new(Vec::new()));
        let sessions = Arc::new(Mutex::new(0usize));
        let dropped = Arc::new(Mutex::new(false));
        let (g, s) = (got.clone(), sessions.clone());
        tokio::spawn(async move {
            loop {
                let (sock, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let (g, s, dropped) = (g.clone(), s.clone(), dropped.clone());
                tokio::spawn(async move {
                    let (rd, mut wr) = sock.into_split();
                    let mut rd = BufReader::new(rd);
                    let hello = match read_frame(&mut rd).await {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    if hello.len() < 2 || hello[0] != MSG_HELLO_PRODUCE || &hello[2..] != token.as_bytes() {
                        return;
                    }
                    *s.lock().unwrap() += 1;
                    wr.write_all(&encode_ack(&Ack { status: ACK_OK, partition: -1, offset: -1, text: "ready".into() })).await.ok();
                    let mut n = 0i64;
                    loop {
                        let body = match read_frame(&mut rd).await {
                            Ok(b) => b,
                            Err(_) => return,
                        };
                        let m = decode_produce(&body).unwrap();
                        n += 1;
                        g.lock().unwrap().push(m.clone());
                        if drop_after > 0 && n as usize >= drop_after {
                            let mut d = dropped.lock().unwrap();
                            if !*d {
                                *d = true;
                                return;
                            }
                        }
                        let ack = if m.topic == reject_topic {
                            Ack { status: ACK_ERROR, partition: -1, offset: -1, text: format!("Topic '{}' nicht freigegeben", m.topic) }
                        } else {
                            Ack { status: ACK_OK, partition: 0, offset: n - 1, text: String::new() }
                        };
                        if wr.write_all(&encode_ack(&ack)).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        FakeProxy { addr, got, sessions }
    }

    fn fast(addr: &str, token: &str, window: usize) -> Arc<ProxySink> {
        let mut s = ProxySink::new(addr, token, 100, window);
        let inner = Arc::get_mut(&mut s).unwrap();
        inner.ack_timeout = Duration::from_secs(2);
        inner.backoff_max = Duration::from_millis(50);
        s
    }

    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "{} nicht innerhalb von 10 s", what);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn codec_roundtrip() {
        let h = encode_hello(b"geheim");
        assert_eq!(&h[..6], &[0, 0, 0, 8, MSG_HELLO_PRODUCE, 6]);
        let f = encode_produce("prices", b"K", b"{}");
        assert_eq!(f.len(), 4 + 1 + 1 + 6 + 2 + 1 + 2);
        let m = decode_produce(&f[4..]).unwrap();
        assert_eq!(m, Msg { topic: "prices".into(), key: b"K".to_vec(), payload: b"{}".to_vec() });
        let a = Ack { status: ACK_OK, partition: 2, offset: 1234567890123, text: "ready".into() };
        assert_eq!(decode_ack(&encode_ack(&a)[4..]).unwrap(), a);
        assert!(decode_ack(&[9, 0]).is_err());
    }

    #[tokio::test]
    async fn sends_in_order_and_counts_acks() {
        let f = fake_proxy("geheim", 0, "").await;
        let s = fast(&f.addr, "geheim", 50);
        s.start();
        assert!(s.enqueue("prices", b"", b"{\"a\":1}"));
        assert!(s.enqueue("trades", b"k", b"{\"b\":2}"));
        let s2 = s.clone();
        wait_until("2 Acks", move || s2.stats().acked == 2).await;
        let got = f.got.lock().unwrap().clone();
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].topic.as_str(), got[0].payload.as_slice()), ("prices", b"{\"a\":1}".as_slice()));
        assert_eq!((got[1].topic.as_str(), got[1].key.as_slice()), ("trades", b"k".as_slice()));
        let st = s.stats();
        assert_eq!((st.pending, st.failed, st.dropped, st.reconnects), (0, 0, 0, 0));
    }

    #[tokio::test]
    async fn reconnects_and_repeats_unacked_once() {
        let f = fake_proxy("geheim", 2, "").await;
        let s = fast(&f.addr, "geheim", 1);
        s.start();
        for d in ["1", "2", "3"] {
            s.enqueue("trades", b"", d.as_bytes());
        }
        let s2 = s.clone();
        wait_until("3 Acks nach Wiederverbindung", move || s2.stats().acked == 3).await;
        let seq: Vec<String> = f.got.lock().unwrap().iter().map(|m| String::from_utf8_lossy(&m.payload).into_owned()).collect();
        assert_eq!(seq.join(","), "1,2,2,3");
        let st = s.stats();
        assert_eq!((st.failed, st.pending, st.reconnects), (0, 0, 1));
    }

    #[tokio::test]
    async fn pipelined_reconnect_repeats_all_unacked_in_order() {
        let f = fake_proxy("geheim", 3, "").await;
        let s = fast(&f.addr, "geheim", 50);
        s.start();
        for d in ["1", "2", "3", "4", "5"] {
            s.enqueue("trades", b"", d.as_bytes());
        }
        let s2 = s.clone();
        wait_until("5 Acks", move || s2.stats().acked == 5).await;
        let seq: Vec<String> = f.got.lock().unwrap().iter().map(|m| String::from_utf8_lossy(&m.payload).into_owned()).collect();
        let cut = seq.iter().position(|v| v == "3").unwrap() + 1;
        assert_eq!(seq[..cut].join(","), "1,2,3");
        let second = seq[cut..].join(",");
        assert!(second.ends_with('5') && second.contains("3,4,5"), "{}", second);
        assert_eq!((s.stats().failed, s.stats().reconnects), (0, 1));
    }

    #[tokio::test]
    async fn wrong_token_keeps_trying_without_blocking() {
        let f = fake_proxy("geheim", 0, "").await;
        let s = fast(&f.addr, "falsch", 50);
        s.start();
        assert!(s.enqueue("trades", b"", b"x"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        let st = s.stats();
        assert_eq!((st.acked, st.failed, st.pending), (0, 0, 1));
        assert_eq!(*f.sessions.lock().unwrap(), 0, "falsches Token darf keine Sitzung bekommen");
    }

    #[tokio::test]
    async fn error_ack_is_counted_connection_stays() {
        let f = fake_proxy("geheim", 0, "cs_was").await;
        let s = fast(&f.addr, "geheim", 50);
        s.start();
        s.enqueue("cs_was", b"", b"x");
        s.enqueue("prices", b"", b"y");
        let s2 = s.clone();
        wait_until("Ack + Fehler", move || { let st = s2.stats(); st.acked == 1 && st.failed == 1 }).await;
        assert_eq!(s.stats().reconnects, 0);
    }

    #[tokio::test]
    async fn drops_when_queue_full() {
        let s = ProxySink::new("127.0.0.1:1", "t", 2, 1); // nicht gestartet
        let ok = (0..5).filter(|_| s.enqueue("trades", b"", b"x")).count();
        let st = s.stats();
        assert_eq!((ok, st.queued, st.dropped), (2, 2, 3));
    }

    /// Gegen einen echten kafkaproxy: `KAFKAPROXY_LIVE_HOST=host:port KAFKAPROXY_LIVE_TOKEN=… cargo test
    /// --lib live_against_real_proxy -- --ignored`; Topic trades muss dort freigegeben sein.
    #[tokio::test]
    #[ignore]
    async fn live_against_real_proxy() {
        let (addr, token) = match (std::env::var("KAFKAPROXY_LIVE_HOST"), std::env::var("KAFKAPROXY_LIVE_TOKEN")) {
            (Ok(a), Ok(t)) if !a.is_empty() && !t.is_empty() => (a, t),
            _ => {
                eprintln!("KAFKAPROXY_LIVE_HOST/KAFKAPROXY_LIVE_TOKEN fehlen, Test uebersprungen");
                return;
            }
        };
        let s = ProxySink::new(&addr, &token, 2000, 200);
        s.start();
        s.enqueue("cs_was", b"", b"{}");
        let n = 1000;
        let start = std::time::Instant::now();
        for _ in 0..n {
            assert!(s.enqueue("trades", b"", b"{\"c\":false,\"i\":\"LIVETEST\",\"e\":\"UAS\",\"t\":1,\"p\":[1,1,1,1,0]}"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while s.stats().acked < n {
            assert!(tokio::time::Instant::now() < deadline, "nur {} von {} bestaetigt", s.stats().acked, n);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let d = start.elapsed();
        let st = s.stats();
        eprintln!("live: {} Nachrichten in {:?} ({:.0}/s), abgelehnt {}, neu verbunden {}", n, d, n as f64 / d.as_secs_f64(), st.failed, st.reconnects);
        assert_eq!((st.failed, st.pending, st.reconnects), (1, 0, 0), "cs_was abgelehnt, sonst alles bestaetigt");
    }
}
