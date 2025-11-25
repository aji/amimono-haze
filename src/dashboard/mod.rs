use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{LazyLock, RwLock},
};

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

struct DashboardSysDirectory;

impl Directory for DashboardSysDirectory {
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

    async fn open_dir(&self, name: &str) -> TreeResult<BoxDirectory> {
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

struct DashboardDirectory;

impl Directory for DashboardDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let res = DIRECTORIES
            .read()
            .unwrap()
            .keys()
            .copied()
            .map(DirEntry::dir)
            .collect();
        Ok(res)
    }
    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }
    async fn open_dir(&self, name: &str) -> TreeResult<BoxDirectory> {
        match DIRECTORIES.read().unwrap().get(name) {
            Some(x) => Ok(x.clone()),
            None => Err(TreeError::NotFound),
        }
    }
}

static DIRECTORIES: LazyLock<RwLock<HashMap<&'static str, BoxDirectory>>> = LazyLock::new(|| {
    let mut dirs = HashMap::new();
    dirs.insert("haze", DashboardSysDirectory.boxed());
    RwLock::new(dirs)
});

pub fn add_directory<D: Directory>(name: &'static str, dir: D) {
    let is_new = DIRECTORIES
        .write()
        .unwrap()
        .insert(name, dir.boxed())
        .is_none();
    if !is_new {
        panic!(
            "cannot use {} as dashboard directory name: already used",
            name
        );
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

pub(crate) fn install(app: &mut AppBuilder, prefix: &str) {
    app.add_job(JobBuilder::new().add_component(component(prefix)));
}
