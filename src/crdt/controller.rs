use std::time::Duration;

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;

use crate::crdt::{
    ring::RingConfig,
    router::{CrdtRouterClient, CrdtRouterComponent},
};

struct Controller {
    last_known_config: Option<RingConfig>,
    router: CrdtRouterClient,
}

impl Controller {
    fn new() -> Controller {
        Controller {
            last_known_config: None,
            router: CrdtRouterClient::new(),
        }
    }

    async fn get_desired_config(&self) -> Result<RingConfig, &'static str> {
        let routers = runtime::discover_all::<CrdtRouterComponent>()
            .await
            .map_err(|_| "failed to discover routers")?
            .into_iter()
            .flat_map(|x| x.http())
            .collect::<Vec<_>>();
        if routers.len() == 1 {
            Ok(RingConfig::ugly_singleton_delete_me(&routers[0]))
        } else {
            Err("not implemented")
        }
    }

    async fn run_once(&mut self) -> Result<(), &'static str> {
        let desired = self.get_desired_config().await?;
        let do_update = self
            .last_known_config
            .as_ref()
            .map(|x| x != &desired)
            .unwrap_or(true);
        if do_update {
            for router in desired.nodes.values() {
                self.router
                    .at(router.as_location())
                    .set_ring(desired.clone())
                    .await
                    .map_err(|_| "update failed")?;
            }
            self.last_known_config = Some(desired);
        }
        Ok(())
    }

    fn main() -> BoxFuture<'static, ()> {
        Box::pin(async {
            let mut controller = Controller::new();
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
