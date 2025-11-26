use std::path::PathBuf;

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;

pub struct StorageInstance {
    _root: PathBuf,
}

impl StorageInstance {
    pub async fn new() -> StorageInstance {
        let root = runtime::storage::<StorageComponent>()
            .await
            .expect("failed to get storage location");
        StorageInstance { _root: root }
    }
}

struct StorageComponent;

impl Component for StorageComponent {
    type Instance = StorageInstance;
}

fn storage_main() -> BoxFuture<'static, ()> {
    Box::pin(async {
        let instance = StorageInstance::new().await;
        runtime::set_instance::<StorageComponent>(instance);
    })
}

pub fn _instance() -> &'static StorageInstance {
    runtime::get_instance::<StorageComponent>()
}

pub fn component(prefix: &str) -> ComponentConfig {
    ComponentConfig {
        label: format!("{prefix}-crdt-storage"),
        id: StorageComponent::id(),
        binding: BindingType::None,
        is_stateful: true,
        entry: storage_main,
    }
}
