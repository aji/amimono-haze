use std::time::Duration;

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component, Location},
};
use futures::future::BoxFuture;

use crate::crdt::{
    router::{CrdtRouterClient, CrdtRouterComponent},
    types::RingConfig,
};

struct Controller {
    router: CrdtRouterClient,
}

impl Controller {
    fn new() -> Controller {
        Controller {
            router: CrdtRouterClient::new(),
        }
    }

    async fn run_once(&self) -> Result<(), &'static str> {
        let routers = runtime::discover_all::<CrdtRouterComponent>()
            .await
            .map_err(|_| "failed to discover routers")?
            .into_iter()
            .flat_map(|x| x.http())
            .collect::<Vec<_>>();
        if routers.len() == 1 {
            let config = RingConfig::singleton(&routers[0]);
            self.router
                .at(Location::Http(routers[0].to_owned()))
                .set_ring(config)
                .await
                .map_err(|_| "router set_ring failed")?;
            Ok(())
        } else {
            Err("not implemented")
        }
    }

    fn main() -> BoxFuture<'static, ()> {
        Box::pin(async {
            let controller = Controller::new();
            loop {
                if let Err(e) = controller.run_once().await {
                    log::warn!("controller iter failed: {e:?}");
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

impl Component for Controller {
    type Instance = ();
}

pub fn component(prefix: &str) -> ComponentConfig {
    ComponentConfig {
        label: format!("{prefix}-crdt-controller"),
        id: Controller::id(),
        binding: BindingType::None,
        is_stateful: false,
        entry: Controller::main,
    }
}
