# uas – User API Server (Rust)

Rust-Port des Node.js-Servers `FT/UserApiServer` ("Chart Analyzing Service") – **nur noch der
WebSocket-Server**. Gleiches WebSocket-Protokoll, gleiche Kafka-Topics, gleicher Port 7073.
Die HTTP-JSON-API des Node-Servers und mit ihr alle Datenbankzugriffe (MySQL) und der Mailversand
sind seit 09.10.2026 entfernt (Svens Vorgabe): gespeichert wird über Kafka im Configserver
(`cs_config_save`, `cs_user_save`), der Chart meldet sich mit dem JWT des Authservers an.

| Komponente     | Node                          | Rust                              |
|----------------|-------------------------------|-----------------------------------|
| WebSocket      | ws (Port 7073)                | tokio-tungstenite                 |
| Kafka          | kafkajs                       | rdkafka (librdkafka via cmake, TLS über ssl-vendored) |
| JWT            | jsonwebtoken HS256            | jsonwebtoken HS256                |
| Passwörter     | bcrypt (cost 10)              | bcrypt (cost 10, nur Passwortwechsel über WS) |
| Logging        | winston, stündliche Rotation  | tracing + tracing-appender (`LOG_FILES=0`: nur Konsole) |

## Bauen, testen, starten

```bash
cargo build --release
cargo test                  # Unit-, WebSocket- und Binary-Tests ohne externe Systeme
JWT_SECRET=… cargo run      # liest .env; WebSocket auf WS_PORT (7073)
```

Docker (Multi-Stage, librdkafka wird im Build-Image kompiliert, das Image exponiert 7073):

```bash
docker build --build-arg versionInfo=1.00.006 -t uas .
```

`GET /health` auf dem WebSocket-Port (für Docker und Load Balancer): 503, bis Kafka dem Consumer
Partitionen zugeteilt hat, dann 200. Der Test `tests/bin_modes.rs` startet das echte Binary und
prüft: nur der WebSocket-Port lauscht (auch `WS_ONLY=0`/`API_PORT` von früher öffnen nichts mehr),
`/health` ohne Kafka 503, ohne `JWT_SECRET` startet der Server nicht.

## Konfiguration

Alle Variablen sind in `.env.example` dokumentiert. `.env` wird beim Start geladen (dotenvy
sucht vom Arbeitsverzeichnis aufwärts). Erkannte Werte für Schalter: `1/true/yes/on` und
`0/false/no/off`; alles andere fällt auf den Default zurück.

`JWT_SECRET` ist Pflicht (ohne endet der Start mit Meldung; derselbe Schlüssel, mit dem der
Authserver signiert – früher stand er als Vorgabe im Quelltext). Kafka-TLS und Kompression über
`src/kafkasecurity.rs` (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_*`, `KAFKA_COMPRESSION_TYPE`,
`KAFKA_SESSION_TIMEOUT_MS`), Consumer-Gruppe `KAFKA_GROUP` (Vorgabe `KAFKA_CLIENT_ID`) und
`KAFKA_AUTO_OFFSET_RESET` (Vorgabe `earliest`), `KAFKA_PRICES_BROKERS` gesetzt und leer = zweiter
Cluster aus (keine Nutzerzahlen ins Topic `prices`), `LOG_FILES=0` = nur Konsole. Mehrere Instanzen
brauchen je eine eigene `SYSTEMID` (Anfang der Verbindungskennungen, über die der Configserver
antwortet) und eine eigene `KAFKA_GROUP`.

## Projektstruktur

```
src/
  main.rs            Bootstrap (app.js), nur WebSocket
  config.rs          Env-Konfiguration
  logging.rs         Konsole (+ logs/logs stündlich und logs/exceptionLogs, außer LOG_FILES=0)
  auth.rs            JWT sign/verify, bcrypt
  kafka.rs           Producer (main + optional prices), Consumer, Nachrichtenaufbau, Bereitschaft
  kafkasecurity.rs   Kafka-TLS, Session-Timeout, Kompression (wie Configserver/was/Alertserver)
  prices.rs          PriceConnection.js (im Original deaktiviert, Stub)
  telegram.rs        telegram/connection.js (im Original deaktiviert, Stub)
  useragent.rs       OS-Erkennung für die WS.SYSTEM-Metrik
  ws/                Client, Handler (Kafka-Fan-out, Heartbeat), Daxen, Server (+ GET /health)
tests/
  ws_server.rs       WebSocket-Tests mit echtem Socket und Recording-Kafka
  bin_modes.rs       startet das Binary und prüft Ports, /health und JWT_SECRET-Pflicht
```

## WebSocket-Protokoll

Client → Server: `{"action": ..., "requestId": n, "data": {...}}`.
Server → Client: `{"action": ..., "requestId": n, "data": {...}}` (bei Marktlisten-Antworten
wird `data.action` zur Envelope-Action).

Aktionen: `login` (JWT in `data.token`), `heartbeat`, `chart_cfg` und die Marktlisten-Aktionen
(→ Kafka `cs_config_save`), `priceAbo`, `abo_info` (→ `cs_abo_info`), `video_info`
(→ `cs_video_info`), `user` (Passwortwechsel → `cs_user_save`), `chart_alert` (→ `cs_alert`),
`daxen`, `sendInstantMessage`, `mu`.

Kafka-Consumer auf `cs_config_live_update` und `cs_user_update`; Fan-out nach `uuid`,
`userId`, `userIds`, `isPublic` und `cfg.mode` wie im Node-Handler (siehe `ws/handler.rs`,
Funktion `route_kafka_message`, vollständig durch Unit-Tests abgedeckt).

Alle 25 s (`WS_PING_INTERVAL_SECS`) schickt der Server einen WebSocket-PING-Control-Frame.
Der Browser antwortet im Netzwerk-Stack mit PONG, ohne JavaScript; damit bleibt der Socket auch
in gedrosselten oder eingefrorenen Hintergrund-Tabs offen. Der Client braucht dafür keine
Änderung.

Alle 65 s: Heartbeat-Sweep, `resHeartbeat` an alle Clients, Nutzerzahlen gehen als Ticks auf
das Topic `prices`. Entfernt und geschlossen werden Clients, deren letzte Aktivität länger als
5 min zurückliegt; Aktivität ist das Maximum aus letzter Anwendungsnachricht und letztem PONG.
Ein eingefrorener Hintergrund-Tab bleibt damit verbunden, solange sein Netzwerk-Stack die
PINGs beantwortet.

## Bewusste Abweichungen vom Node-Server

- WS-Aktion `mu`: leitet `data` an die anderen Sessions desselben Users weiter (Node warf
  eine ReferenceError und tat nichts).
- Beim Heartbeat-Timeout wird die Verbindung geschlossen (Node ließ sie offen, nachfolgende
  Nachrichten crashten). Grundlage ist das Maximum aus letzter Nachricht und letztem PONG.
- Beim WS-Login wird das mitgesendete Passwort nicht mehr gehasht (der Hash wurde nie benutzt).
- Keine HTTP-API, keine Datenbank, keine Mails (seit 09.10.2026).

Beibehaltene Eigenheiten (Kompatibilität): `requestId` im WS-Envelope folgt JS `+ 0`
(Strings werden zu `"…0"`), der Instant Messenger gilt als aktiv, solange `TELEGRAM_USER_ID`
im Token nicht exakt `0` ist, eine WS-Aktion `user` ohne Passwort geht ohne `newPwd` an Kafka,
`cs_config_live_update` entfernt beim Zusammenführen Felder, die JS auf `undefined` gesetzt hätte.

Verbleibende, bewusst nicht nachgebildete Kleinigkeiten:

- WS-Clients werden beim Verbindungsaufbau registriert, in Node erst mit der ersten Nachricht;
  stille Verbindungen zählen daher im Sweep mit und erhalten Broadcasts.
- Nur HS256 wird als JWT-Algorithmus akzeptiert (Node zusätzlich HS384/HS512; ausgestellt wurde
  immer HS256).
