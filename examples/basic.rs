use amimono::config::{AppBuilder, AppConfig};

fn configure() -> AppConfig {
    AppBuilder::new("1")
        .install(amimono_haze::installer())
        .build()
}

fn main() {
    env_logger::init();
    amimono::entry(configure());
}
