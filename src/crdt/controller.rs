use std::time::Duration;

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::Component,
};
use futures::future::BoxFuture;

struct Controller;

impl Controller {
    fn new() -> Controller {
        Controller
    }

    async fn run_once(&self) -> Result<(), &'static str> {
        Err("not implemented")
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
