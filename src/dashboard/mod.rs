use std::net::SocketAddr;

use amimono::{
    config::{AppBuilder, Binding, BindingType, ComponentConfig, JobBuilder},
    runtime::{self, Component},
};
use axum::{Router, routing::get};
use futures::future::BoxFuture;

use crate::dashboard::html::{Dir, DirEntry};

pub mod html;

#[cfg(feature = "dht")]
mod dht;
#[cfg(feature = "dht")]
const DHT_ROUTER: fn() -> Router<()> = dht::router;
#[cfg(not(feature = "dht"))]
const DHT_ROUTER: fn() -> Router<()> = service_disabled;

#[cfg(feature = "metadata")]
mod metadata;
#[cfg(feature = "metadata")]
const METADATA_ROUTER: fn() -> Router<()> = metadata::router;
#[cfg(not(feature = "metadata"))]
const METADATA_ROUTER: fn() -> Router<()> = service_disabled;

fn app_router() -> Router<()> {
    Router::new()
        .route(
            "/",
            get(|| async {
                let ents = {
                    let mut ents = Vec::new();
                    if cfg!(feature = "dht") {
                        ents.push(DirEntry("dht".to_owned(), None));
                    }
                    if cfg!(feature = "metadata") {
                        ents.push(DirEntry("metadata".to_owned(), None));
                    }
                    ents
                };
                let dir = Dir("/".to_string(), Some("Installed services"), Ok(ents));
                dir.render()
            }),
        )
        .nest("/dht", DHT_ROUTER())
        .nest("/metadata", METADATA_ROUTER())
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
