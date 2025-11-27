use amimono::config::{AppBuilder, JobBuilder};

#[cfg(feature = "blob")]
pub mod blob;
#[cfg(feature = "crdt")]
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
    move |app| {
        #[cfg(feature = "controller")]
        app.add_job({
            let mut controller = JobBuilder::new();
            controller.with_label(format!("{prefix}-controller"));

            #[cfg(feature = "crdt")]
            crdt::install_controller(&mut controller, prefix);

            controller.build()
        });

        #[cfg(feature = "blob")]
        blob::install(app, prefix);
        #[cfg(feature = "crdt")]
        crdt::install(app, prefix);
        #[cfg(feature = "dashboard")]
        dashboard::install(app, prefix);
        #[cfg(feature = "dht")]
        dht::install(app, prefix);
        #[cfg(feature = "metadata")]
        metadata::install(app, prefix);
    }
}
