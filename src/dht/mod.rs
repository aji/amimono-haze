use amimono::config::AppBuilder;

pub mod controller;

pub(crate) fn install(app: &mut AppBuilder, prefix: &str) {
    controller::install(app, prefix);
}
