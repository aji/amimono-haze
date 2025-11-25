use std::net::SocketAddr;

use amimono::{
    config::{AppBuilder, Binding, BindingType, ComponentConfig, JobBuilder},
    runtime::{self, Component},
};
use axum::{Router, extract::Path, routing::get};
use futures::future::BoxFuture;

use crate::dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult};

pub mod tree;

#[cfg(feature = "dht")]
mod dht;
#[cfg(feature = "metadata")]
mod metadata;

struct DashboardDirectory;

impl Directory for DashboardDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let mut entries = Vec::new();

        if cfg!(feature = "dht") {
            entries.push(DirEntry::dir("dht"));
        }
        if cfg!(feature = "metadata") {
            entries.push(DirEntry::dir("metadata"));
        }

        Ok(entries)
    }

    async fn open_dir(&self, name: &str) -> TreeResult<Box<dyn BoxDirectory>> {
        match name {
            #[cfg(feature = "dht")]
            "dht" => Ok(dht::DhtDirectory.boxed()),
            #[cfg(feature = "metadata")]
            "metadata" => Ok(metadata::MetadataDirectory.boxed()),
            _ => Err(TreeError::NotFound),
        }
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }
}

fn app_router() -> Router<()> {
    Router::new()
        .route(
            "/",
            get(async || tree::render(DashboardDirectory, "").await),
        )
        .route(
            "/{*path}",
            get(async |Path(path): Path<String>| tree::render(DashboardDirectory, &path).await),
        )
}

#[allow(dead_code)]
fn service_disabled() -> Router<()> {
    Router::new().route("/", get(async || "service not installed"))
}

async fn dashboard_entry_impl() {
    let binding: SocketAddr = match runtime::binding::<DashboardComponent>() {
        Binding::None => panic!("no binding configured for dashboard"),
        Binding::Http(port) => ([0, 0, 0, 0], port).into(),
    };
    let app = app_router();
    let listener = tokio::net::TcpListener::bind(&binding)
        .await
        .expect("failed to bind dashboard listener");
    log::info!("haze dashboard listening on {}", binding);
    axum::serve(listener, app)
        .await
        .expect("dashboard server failed");
}

fn dashboard_entry() -> BoxFuture<'static, ()> {
    Box::pin(dashboard_entry_impl())
}

struct DashboardComponent;

impl Component for DashboardComponent {
    type Instance = ();
}

fn component(prefix: &str) -> ComponentConfig {
    ComponentConfig {
        label: format!("{}dashboard", prefix),
        id: DashboardComponent::id(),
        binding: BindingType::HttpFixed(8585),
        is_stateful: false,
        entry: dashboard_entry,
    }
}

pub fn install(app: &mut AppBuilder, prefix: &str) {
    app.add_job(JobBuilder::new().add_component(component(prefix)));
}
