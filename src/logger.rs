use rasant::{Level, Logger, sink::stdout::StdoutConfig};
use std::env;
use std::sync::{Mutex, OnceLock};

static LOGGER: OnceLock<Mutex<Logger>> = OnceLock::new();

pub(crate) fn logger() -> &'static Mutex<Logger> {
    LOGGER.get_or_init(|| {
        let mut logger = Logger::new();
        logger.set_level(level_from_env());
        logger.add_sink(rasant::sink::stdout::new(StdoutConfig {
            flush_on_write: true,
            ..Default::default()
        }));
        Mutex::new(logger)
    })
}

fn level_from_env() -> Level {
    env::var("RUST_LOG")
        .ok()
        .as_deref()
        .map(parse_level)
        .unwrap_or(Level::Info)
}

fn parse_level(s: &str) -> Level {
    if s.eq_ignore_ascii_case("warn") {
        return Level::Warning;
    }
    Level::try_from(s.trim()).unwrap_or(Level::Info)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_warn_alias() {
        assert_eq!(parse_level("warn"), Level::Warning);
    }

    #[test]
    fn parses_rasant_level_names() {
        assert_eq!(parse_level("warning"), Level::Warning);
        assert_eq!(parse_level("INFO"), Level::Info);
        assert_eq!(parse_level("trace"), Level::Trace);
        assert_eq!(parse_level("error"), Level::Error);
    }

    #[test]
    fn unknown_or_empty_falls_back_to_info() {
        assert_eq!(parse_level(""), Level::Info);
        assert_eq!(parse_level("bogus"), Level::Info);
    }
}
