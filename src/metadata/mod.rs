use amimono::config::{AppBuilder, JobBuilder};

pub(crate) mod router;
pub(crate) mod storage;

pub use router::MetadataClient;

pub(crate) fn install(app: &mut AppBuilder, prefix: &str) {
    app.add_job(
        JobBuilder::new()
            .with_label(format!("{}metadata", prefix))
            .add_component(router::component(prefix))
            .add_component(storage::component(prefix)),
    );
}
