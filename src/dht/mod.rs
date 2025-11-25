use amimono::config::AppBuilder;

pub mod controller;

pub fn install(app: &mut AppBuilder, prefix: &str) {
    controller::install(app, prefix);
}
