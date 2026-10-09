//! Kafka-Verbindung: unverschluesselt (Vorgabe) oder TLS mit Client-Zertifikat.
//!
//! IONOS Managed Kafka verlangt TLS mit Client-Zertifikat, der bisherige Kafka laeuft
//! ohne. Uebernommen aus dem Alertserver (urspruenglich was src/was/kafkasecurity.rs; uas 09.10.2026); die Einstellungen
//! kommen aus denselben Umgebungsvariablen wie bei Configserver und was:
//!
//!   KAFKA_SECURITY_PROTOCOL                      plaintext (Vorgabe) | ssl
//!   KAFKA_SSL_CA_LOCATION                        CA der Broker (PEM)
//!   KAFKA_SSL_CERTIFICATE_LOCATION               Client-Zertifikat (PEM)
//!   KAFKA_SSL_KEY_LOCATION                       privater Schluessel dazu (PEM)
//!   KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM  https (Vorgabe) | none
//!   KAFKA_SESSION_TIMEOUT_MS                     Consumer, Vorgabe 10000
//!   KAFKA_COMPRESSION_TYPE                       Producer: gzip (Vorgabe) | snappy | lz4 | none
//!
//! "none" prueft nur die CA-Kette, nicht den Namen im Broker-Zertifikat - noetig, wenn die
//! Broker per IP angesprochen werden (IONOS).
//!
//! Kompression: IONOS Kafka nimmt je Nachricht hoechstens 1 MB an (max.message.bytes laesst
//! sich dort nicht aendern), gemessen NACH der Kompression. Die Nachrichten des Alertservers
//! sind klein, gzip ist trotzdem Vorgabe wie bei Configserver und was; jeder Leser versteht
//! es (librdkafka, kafka-go, kafkajs).

use std::fmt;
use std::sync::OnceLock;

use rdkafka::ClientConfig;

/// Vorgabe fuer session.timeout.ms des Consumers. librdkafka nimmt sonst 45 s: so lange
/// gilt ein hart beendeter uas noch als Mitglied seiner Gruppe, und so lange haengt
/// nach einem Neustart der Beitritt. IONOS trennt dabei jede Verbindung, die laenger als 5 s
/// auf Antwort wartet - mit 10 s ist der Neustart nach wenigen Versuchen wieder in der Gruppe.
const DEFAULT_SESSION_TIMEOUT_MS: u32 = 10_000;
/// Kafka lehnt kuerzere Werte ab (group.min.session.timeout.ms).
const MIN_SESSION_TIMEOUT_MS: u32 = 6_000;

#[derive(Debug, Clone, PartialEq)]
pub enum KafkaSecurity {
    Plaintext,
    Ssl {
        ca: String,
        certificate: String,
        key: String,
        /// false = nur die CA-Kette pruefen, nicht den Namen im Broker-Zertifikat
        verify_hostname: bool,
    },
}

impl KafkaSecurity {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Wie from_env, aber mit austauschbarer Quelle - so ist die Auswertung ohne echte
    /// Umgebungsvariablen testbar.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let protocol = get("KAFKA_SECURITY_PROTOCOL").unwrap_or_default().trim().to_lowercase();
        match protocol.as_str() {
            "" | "plaintext" => Ok(KafkaSecurity::Plaintext),
            "ssl" => {
                let file = |name: &str| -> Result<String, String> {
                    let path = get(name).unwrap_or_default().trim().to_string();
                    if path.is_empty() {
                        return Err(format!("{} fehlt", name));
                    }
                    std::fs::File::open(&path)
                        .map_err(|e| format!("{}={} ist nicht lesbar: {}", name, path, e))?;
                    Ok(path)
                };
                let algorithm = get("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM")
                    .map(|v| v.trim().to_lowercase());
                let verify_hostname = match algorithm.as_deref() {
                    None | Some("https") => true,
                    Some("") | Some("none") => false,
                    Some(other) => {
                        return Err(format!(
                            "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM={} unbekannt (https oder none)",
                            other
                        ))
                    }
                };
                Ok(KafkaSecurity::Ssl {
                    ca: file("KAFKA_SSL_CA_LOCATION")?,
                    certificate: file("KAFKA_SSL_CERTIFICATE_LOCATION")?,
                    key: file("KAFKA_SSL_KEY_LOCATION")?,
                    verify_hostname,
                })
            }
            other => Err(format!("KAFKA_SECURITY_PROTOCOL={} unbekannt (plaintext oder ssl)", other)),
        }
    }

    /// Traegt die Einstellungen in eine Client-Konfiguration ein - fuer Producer und Consumer.
    pub fn apply_to(&self, config: &mut ClientConfig) {
        if let KafkaSecurity::Ssl { ca, certificate, key, verify_hostname } = self {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set(
                    "ssl.endpoint.identification.algorithm",
                    if *verify_hostname { "https" } else { "none" },
                );
        }
        // IONOS trennt unbenutzte Verbindungen nach 5 s, librdkafka baut sie bei Bedarf
        // wieder auf. Ohne diese Einstellung schreibt es dabei alle paar Sekunden
        // "Disconnected ... closed by peer" ins Log. Fuer jeden anderen Kafka unkritisch.
        config.set("log.connection.close", "false");
    }
}

impl fmt::Display for KafkaSecurity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaSecurity::Plaintext => write!(f, "unverschluesselt (plaintext)"),
            KafkaSecurity::Ssl { ca, certificate, verify_hostname, .. } => write!(
                f,
                "TLS mit Client-Zertifikat (CA {}, Zertifikat {}, Pruefung: {})",
                ca,
                certificate,
                if *verify_hostname { "CA-Kette und Name" } else { "nur CA-Kette" }
            ),
        }
    }
}

/// Die Kafka-Sicherheit dieses Prozesses, einmal aus der Umgebung gelesen. Eine falsche
/// Konfiguration beendet den Prozess mit Meldung: main() ruft das auf, bevor die
/// Kafka-Threads starten - ein Fehler erst dort wuerde nur den Thread beenden und der
/// Alertserver liefe ohne Kafka weiter.
pub fn kafka_security() -> &'static KafkaSecurity {
    static SECURITY: OnceLock<KafkaSecurity> = OnceLock::new();
    SECURITY.get_or_init(|| {
        KafkaSecurity::from_env().unwrap_or_else(|e| {
            eprintln!("Kafka-Verbindung falsch konfiguriert: {}", e);
            std::process::exit(1);
        })
    })
}

/// session.timeout.ms des Consumers aus KAFKA_SESSION_TIMEOUT_MS; fehlt der Wert oder ist
/// er ungueltig, gilt die Vorgabe.
pub fn consumer_session_timeout_ms() -> String {
    parse_session_timeout(std::env::var("KAFKA_SESSION_TIMEOUT_MS").ok()).to_string()
}

fn parse_session_timeout(value: Option<String>) -> u32 {
    match value {
        None => DEFAULT_SESSION_TIMEOUT_MS,
        Some(raw) => match raw.trim().parse::<u32>() {
            Ok(ms) if ms >= MIN_SESSION_TIMEOUT_MS => ms,
            _ => {
                println!(
                    "env KAFKA_SESSION_TIMEOUT_MS ungueltig ({:?}), nutze {}",
                    raw, DEFAULT_SESSION_TIMEOUT_MS
                );
                DEFAULT_SESSION_TIMEOUT_MS
            }
        },
    }
}

/// Kompression des Producers aus KAFKA_COMPRESSION_TYPE: Some(codec) fuer compression.type,
/// None fuer unkomprimiert. Ein unbekannter Wert beendet den Prozess mit Meldung (wie eine
/// falsche TLS-Einstellung) - stillschweigend unkomprimiert zu senden hiesse, Nachrichten
/// ueber 1 MB wieder zu verlieren.
pub fn kafka_compression() -> Option<String> {
    parse_compression(std::env::var("KAFKA_COMPRESSION_TYPE").ok()).unwrap_or_else(|e| {
        eprintln!("Kafka-Kompression falsch konfiguriert: {}", e);
        std::process::exit(1);
    })
}

/// zstd fehlt in dieser librdkafka (ohne zstd-Feature gebaut), deshalb nicht angeboten.
const COMPRESSION_CODECS: [&str; 3] = ["gzip", "snappy", "lz4"];
const DEFAULT_COMPRESSION: &str = "gzip";

fn parse_compression(value: Option<String>) -> Result<Option<String>, String> {
    let codec = value.unwrap_or_default().trim().to_lowercase();
    if codec.is_empty() {
        return Ok(Some(DEFAULT_COMPRESSION.to_string()));
    }
    if codec == "none" {
        return Ok(None);
    }
    if COMPRESSION_CODECS.contains(&codec.as_str()) {
        return Ok(Some(codec));
    }
    Err(format!("KAFKA_COMPRESSION_TYPE={} unbekannt (gzip, snappy, lz4 oder none)", codec))
}

#[cfg(test)]
mod compression_tests {
    use super::parse_compression;

    // Vorgabe gzip, "none" schaltet ab, Codecs ohne Ruecksicht auf Schreibweise, Unbekanntes
    // ist ein Fehler.
    #[test]
    fn kompression_aus_der_umgebung() {
        assert_eq!(parse_compression(None), Ok(Some("gzip".to_string())));
        assert_eq!(parse_compression(Some("  ".to_string())), Ok(Some("gzip".to_string())));
        assert_eq!(parse_compression(Some("none".to_string())), Ok(None));
        assert_eq!(parse_compression(Some(" LZ4 ".to_string())), Ok(Some("lz4".to_string())));
        assert_eq!(parse_compression(Some("snappy".to_string())), Ok(Some("snappy".to_string())));
        assert!(parse_compression(Some("zstd".to_string())).is_err());
        assert!(parse_compression(Some("brotli".to_string())).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |name| map.get(name).cloned()
    }

    /// Legt eine lesbare Datei an und liefert ihren Pfad.
    fn temp_file(name: &str) -> String {
        let path = std::env::temp_dir().join(format!("alertserver-kafkasecurity-{}-{}", std::process::id(), name));
        std::fs::write(&path, "test").unwrap();
        path.to_string_lossy().to_string()
    }

    #[test]
    fn ohne_angabe_unverschluesselt() {
        assert_eq!(KafkaSecurity::from_lookup(lookup(&[])), Ok(KafkaSecurity::Plaintext));
        assert_eq!(
            KafkaSecurity::from_lookup(lookup(&[("KAFKA_SECURITY_PROTOCOL", " PLAINTEXT ")])),
            Ok(KafkaSecurity::Plaintext)
        );
        // die TLS-Pfade duerfen gesetzt sein (die compose.yml traegt sie immer ein) - sie
        // zaehlen nur bei ssl
        assert_eq!(
            KafkaSecurity::from_lookup(lookup(&[
                ("KAFKA_SECURITY_PROTOCOL", "plaintext"),
                ("KAFKA_SSL_CA_LOCATION", "/gibt/es/nicht"),
            ])),
            Ok(KafkaSecurity::Plaintext)
        );
    }

    #[test]
    fn ssl_mit_dateien() {
        let (ca, cert, key) = (temp_file("ca"), temp_file("cert"), temp_file("key"));
        let security = KafkaSecurity::from_lookup(lookup(&[
            ("KAFKA_SECURITY_PROTOCOL", "SSL"),
            ("KAFKA_SSL_CA_LOCATION", &ca),
            ("KAFKA_SSL_CERTIFICATE_LOCATION", &cert),
            ("KAFKA_SSL_KEY_LOCATION", &key),
            ("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "none"),
        ]))
        .unwrap();
        assert_eq!(
            security,
            KafkaSecurity::Ssl { ca: ca.clone(), certificate: cert.clone(), key: key.clone(), verify_hostname: false }
        );

        let mut config = ClientConfig::new();
        security.apply_to(&mut config);
        assert_eq!(config.get("security.protocol"), Some("ssl"));
        assert_eq!(config.get("ssl.ca.location"), Some(ca.as_str()));
        assert_eq!(config.get("ssl.certificate.location"), Some(cert.as_str()));
        assert_eq!(config.get("ssl.key.location"), Some(key.as_str()));
        assert_eq!(config.get("ssl.endpoint.identification.algorithm"), Some("none"));
        assert_eq!(config.get("log.connection.close"), Some("false"));
        for f in [ca, cert, key] {
            let _ = std::fs::remove_file(f);
        }
    }

    #[test]
    fn namenspruefung_ist_vorgabe() {
        let (ca, cert, key) = (temp_file("ca2"), temp_file("cert2"), temp_file("key2"));
        let base = [
            ("KAFKA_SECURITY_PROTOCOL", "ssl"),
            ("KAFKA_SSL_CA_LOCATION", ca.as_str()),
            ("KAFKA_SSL_CERTIFICATE_LOCATION", cert.as_str()),
            ("KAFKA_SSL_KEY_LOCATION", key.as_str()),
        ];
        let verify = |algorithm: Option<&str>| {
            let mut pairs = base.to_vec();
            if let Some(a) = algorithm {
                pairs.push(("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", a));
            }
            KafkaSecurity::from_lookup(lookup(&pairs))
        };
        assert!(matches!(verify(None), Ok(KafkaSecurity::Ssl { verify_hostname: true, .. })));
        assert!(matches!(verify(Some("https")), Ok(KafkaSecurity::Ssl { verify_hostname: true, .. })));
        assert!(matches!(verify(Some("")), Ok(KafkaSecurity::Ssl { verify_hostname: false, .. })));
        assert!(matches!(verify(Some("None")), Ok(KafkaSecurity::Ssl { verify_hostname: false, .. })));
        assert!(verify(Some("tls")).unwrap_err().contains("unbekannt"));
        for f in [ca, cert, key] {
            let _ = std::fs::remove_file(f);
        }
    }

    #[test]
    fn fehler_werden_benannt() {
        let ca = temp_file("ca3");
        assert_eq!(
            KafkaSecurity::from_lookup(lookup(&[("KAFKA_SECURITY_PROTOCOL", "sasl_ssl")])),
            Err("KAFKA_SECURITY_PROTOCOL=sasl_ssl unbekannt (plaintext oder ssl)".to_string())
        );
        assert_eq!(
            KafkaSecurity::from_lookup(lookup(&[("KAFKA_SECURITY_PROTOCOL", "ssl")])),
            Err("KAFKA_SSL_CA_LOCATION fehlt".to_string())
        );
        let missing = KafkaSecurity::from_lookup(lookup(&[
            ("KAFKA_SECURITY_PROTOCOL", "ssl"),
            ("KAFKA_SSL_CA_LOCATION", &ca),
            ("KAFKA_SSL_CERTIFICATE_LOCATION", "/gibt/es/nicht.pem"),
        ]))
        .unwrap_err();
        assert!(missing.starts_with("KAFKA_SSL_CERTIFICATE_LOCATION=/gibt/es/nicht.pem ist nicht lesbar"), "{}", missing);
        let _ = std::fs::remove_file(ca);
    }

    #[test]
    fn plaintext_setzt_keine_tls_werte() {
        let mut config = ClientConfig::new();
        KafkaSecurity::Plaintext.apply_to(&mut config);
        assert_eq!(config.get("security.protocol"), None);
        assert_eq!(config.get("ssl.ca.location"), None);
        assert_eq!(config.get("log.connection.close"), Some("false"));
    }

    #[test]
    fn session_timeout() {
        assert_eq!(parse_session_timeout(None), 10_000);
        assert_eq!(parse_session_timeout(Some(" 30000 ".to_string())), 30_000);
        assert_eq!(parse_session_timeout(Some("6000".to_string())), 6_000);
        // zu klein, leer oder keine Zahl: Vorgabe
        assert_eq!(parse_session_timeout(Some("3000".to_string())), 10_000);
        assert_eq!(parse_session_timeout(Some("".to_string())), 10_000);
        assert_eq!(parse_session_timeout(Some("abc".to_string())), 10_000);
    }
}
