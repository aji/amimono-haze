use amimono::config::AppBuilder;

#[cfg(feature = "blob")]
pub mod blob;
pub mod crdt;
#[cfg(feature = "dashboard")]
pub mod dashboard;
#[cfg(feature = "dht")]
pub mod dht;
#[cfg(feature = "metadata")]
pub mod metadata;

pub(crate) mod util;

pub fn installer() -> impl FnOnce(&mut AppBuilder) {
    installer_with_prefix("haze")
}

pub fn installer_with_prefix(prefix: &str) -> impl FnOnce(&mut AppBuilder) {
    |app| {
        #[cfg(feature = "blob")]
        blob::install(app, prefix);
        crdt::install(app, prefix);
        #[cfg(feature = "dashboard")]
        dashboard::install(app, prefix);
        #[cfg(feature = "dht")]
        dht::install(app, prefix);
        #[cfg(feature = "metadata")]
        metadata::install(app, prefix);
    }
}
