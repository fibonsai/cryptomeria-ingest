use log::LevelFilter;
use rasant::{Level, Logger, sink::stdout::StdoutConfig};
use std::env;
use std::sync::{Mutex, OnceLock};

static LOGGER: OnceLock<Mutex<Logger>> = OnceLock::new();

fn init_logger() -> &'static Mutex<Logger> {
    LOGGER.get_or_init(|| {
        let mut logger = Logger::new();
        let level = env::var("RUST_LOG")
            .ok()
            .and_then(|s| s.parse::<LevelFilter>().ok())
            .map(|l| match l {
                LevelFilter::Trace => Level::Trace,
                LevelFilter::Debug => Level::Debug,
                LevelFilter::Info => Level::Info,
                LevelFilter::Warn => Level::Warning,
                LevelFilter::Error => Level::Error,
                _ => Level::Info,
            })
            .unwrap_or(Level::Info);
        logger.set_level(level);
        let stdout_config = StdoutConfig {
            flush_on_write: true,
            ..Default::default()
        };
        logger.add_sink(rasant::sink::stdout::new(stdout_config));
        Mutex::new(logger)
    })
}

/// Initialize the logger (idempotent).
pub fn init() {
    init_logger();
}

fn log(level: Level, source: &str, msg: String) {
    let logger = init_logger();
    let mut logger = logger.lock().unwrap();
    logger.log(level, &format!("[{}] {}", source, msg));
}

pub fn info(source: &str, msg: &str) {
    log(Level::Info, source, msg.to_string());
}

pub fn warn(source: &str, msg: &str) {
    log(Level::Warning, source, msg.to_string());
}

pub fn error(source: &str, msg: &str) {
    log(Level::Error, source, msg.to_string());
}

pub fn debug(source: &str, msg: &str) {
    log(Level::Debug, source, msg.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_output() {
        init();
        info("test", "info message");
        warn("test", "warn message");
        error("test", "error message");
        debug("test", "debug message");
    }
}
