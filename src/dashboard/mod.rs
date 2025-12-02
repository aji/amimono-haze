use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{LazyLock, RwLock},
};

use amimono::{
    config::{AppBuilder, Binding, ComponentConfig, JobBuilder},
    runtime::{self, Component},
};
use axum::{Router, http::Uri, response::IntoResponse, routing::get};
use futures::future::BoxFuture;

use crate::dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult};

pub mod tree;

#[cfg(feature = "crdt")]
mod crdt;

const PORT: u16 = 8585;

struct DashboardSysDirectory;

impl Directory for DashboardSysDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let mut entries = Vec::new();

        if cfg!(feature = "crdt") {
            entries.push(DirEntry::dir("crdt"));
        }

        Ok(entries)
    }

    async fn open_dir(&self, name: &str) -> TreeResult<BoxDirectory> {
        match name {
            #[cfg(feature = "crdt")]
            "crdt" => Ok(crdt::CrdtDirectory.boxed()),
            _ => Err(TreeError::NotFound),
        }
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }
}

struct DashboardAmimonoDirectory;

impl Directory for DashboardAmimonoDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let res = runtime::config()
            .jobs()
            .map(|j| DirEntry::dir(j.label()))
            .collect();
        Ok(res)
    }

    async fn open_dir(&self, name: &str) -> TreeResult<BoxDirectory> {
        match runtime::config().job(name) {
            Some(j) => Ok(DashboardJobDirectory(j.label().to_owned()).boxed()),
            None => Err(TreeError::NotFound),
        }
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }
}

struct DashboardJobDirectory(String);

impl Directory for DashboardJobDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let res = runtime::config()
            .job(self.0.as_str())
            .ok_or(TreeError::NotFound)?
            .components()
            .map(|c| DirEntry::item(&c.label))
            .collect();
        Ok(res)
    }

    async fn open_dir(&self, _name: &str) -> TreeResult<BoxDirectory> {
        Err(TreeError::NotFound)
    }

    async fn open_item(&self, name: &str) -> TreeResult<Item> {
        match runtime::config().component(name) {
            Some(c) => {
                let discovery = match runtime::discover_by_label(name).await {
                    Err(e) => format!("  Error: {e:?}"),
                    Ok(locs) => {
                        if locs.len() > 0 {
                            locs.into_iter()
                                .map(|x| format!("- {x:?}"))
                                .collect::<Vec<_>>()
                                .join("\n")
                        } else {
                            "  (empty)".to_string()
                        }
                    }
                };

                let res = Item::new(format!(
                    "\
                    Binding: {:?}\n\
                    Discovery:\n\
                    {}\n",
                    c.binding, discovery
                ));
                Ok(res)
            }
            None => Err(TreeError::NotFound),
        }
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
    dirs.insert("amimono", DashboardAmimonoDirectory.boxed());
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

async fn render_tree(uri: Uri) -> impl IntoResponse {
    tree::render(DashboardDirectory, uri.path()).await
}

fn app_router() -> Router<()> {
    Router::new()
        .route("/", get(render_tree))
        .route("/{*path}", get(render_tree))
}

async fn dashboard_entry_impl() {
    let binding: SocketAddr = ([0, 0, 0, 0], PORT).into();
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
        label: format!("{prefix}-dashboard"),
        id: DashboardComponent::id(),
        binding: Binding::Tcp(PORT),
        is_stateful: false,
        entry: dashboard_entry,
    }
}

pub(crate) fn install(app: &mut AppBuilder, prefix: &str) {
    app.add_job(JobBuilder::new().add_component(component(prefix)));
}
