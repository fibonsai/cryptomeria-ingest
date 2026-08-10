use std::sync::Once;

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default())
            .format_timestamp_secs()
            .init();
    });
}
