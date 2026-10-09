//! Logging setup, modelled after `config/log.js` (winston):
//! console output plus hourly rotated files under `./logs/logs`, and a separate
//! `./logs/exceptionLogs` file for panics.

use std::io::Write;
use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Keeps the non-blocking file writer alive; drop it on shutdown to flush.
pub struct LogGuard {
    _file: Option<WorkerGuard>,
}

/// `LOG_FILES=0`: nur Konsole (Container, schreibgeschütztes Dateisystem – das Log sammelt Docker).
pub fn init(log_dir: &Path) -> anyhow::Result<LogGuard> {
    if !crate::config::env_flag("LOG_FILES", true) {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new("debug,rdkafka=info,tungstenite=info")
        });
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(false))
            .try_init()?;
        return Ok(LogGuard { _file: None });
    }
    let app_dir = log_dir.join("logs");
    let exc_dir = log_dir.join("exceptionLogs");
    std::fs::create_dir_all(&app_dir)?;
    std::fs::create_dir_all(&exc_dir)?;

    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::HOURLY)
        .filename_prefix("application")
        .filename_suffix("log")
        .max_log_files(14 * 24)
        .build(&app_dir)?;
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("debug,rdkafka=info,tungstenite=info")
    });

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false))
        .with(fmt::layer().with_ansi(false).with_writer(file_writer))
        .try_init()?;

    install_panic_hook(exc_dir);

    Ok(LogGuard { _file: Some(guard) })
}

/// Mirrors winston's `exceptionHandlers`: panics are appended to the exception log.
fn install_panic_hook(exc_dir: std::path::PathBuf) {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
        let file = exc_dir.join(format!(
            "exceptions-{}.log",
            chrono::Local::now().format("%Y-%m-%d-%H")
        ));
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file)
        {
            let _ = writeln!(f, "{now} uncaught panic: {info}");
        }
        tracing::error!("uncaught panic: {info}");
        default_hook(info);
    }));
}
