use std::{collections::HashSet, time::Duration};

use amimono::{
    config::{AppBuilder, AppConfig, Binding, ComponentConfig},
    rpc::RpcResult,
    runtime::Component,
};
use amimono_haze::crdt::{Crdt, CrdtClient, StoredCrdt, crdt::Version};
use serde::{Deserialize, Serialize};

const SCOPE: &'static str = "crdt-example";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MyCrdt {
    value: Version<u64, HashSet<u64>>,
}

impl Crdt for MyCrdt {
    fn merge_from(&mut self, other: Self) {
        self.value.merge_from(other.value);
    }
}

impl StoredCrdt for MyCrdt {}

impl Default for MyCrdt {
    fn default() -> Self {
        Self {
            value: Version(0, Default::default()),
        }
    }
}

struct Driver {
    client: CrdtClient<MyCrdt>,
}

impl Driver {
    async fn new() -> Driver {
        Driver {
            client: CrdtClient::new(SCOPE),
        }
    }

    async fn run(self) {
        loop {
            if let Err(e) = self.run_once().await {
                log::error!("driver iter failed: {e:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn run_once(&self) -> RpcResult<()> {
        let key = self.choose_key();
        let old_item = self.client.get_or_default(&key).await?;
        let new_item = self.choose_modify(old_item);
        log::info!("updating {key} with {new_item:?}");
        self.client.put(&key, new_item).await?;
        Ok(())
    }

    fn choose_key(&self) -> String {
        let n = rand::random_range(0..=9);
        format!("my-key-{n}")
    }

    fn choose_modify(&self, mut item: MyCrdt) -> MyCrdt {
        if rand::random_bool(0.2) {
            item.value.0 += 1;
            item.value.1.clear();
        } else {
            item.value.1.insert(rand::random_range(10..99));
        }
        item
    }

    async fn main() {
        Driver::new().await.run().await
    }
}

impl Component for Driver {
    type Instance = ();
}

fn driver_component() -> ComponentConfig {
    ComponentConfig {
        label: "crdt-driver".to_owned(),
        id: Driver::id(),
        binding: Binding::None,
        is_stateful: false,
        entry: || Box::pin(Driver::main()),
    }
}

fn configure() -> AppConfig {
    MyCrdt::bind(SCOPE);
    AppBuilder::new("1")
        .install(amimono_haze::installer())
        .add_job(driver_component())
        .build()
}

fn main() {
    env_logger::init();
    amimono::entry(configure());
}
