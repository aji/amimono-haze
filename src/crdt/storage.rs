use std::{collections::HashMap, io, path::PathBuf};

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;
use sha2::Digest;
use tokio::sync::RwLock;

use crate::{
    crdt::{merge_in_scope, types::RingConfig},
    util::{hashring::HashRing, hex::Hex},
};

pub struct StorageInstance {
    ring: RwLock<RingStorage>,
    root: RwLock<PathBuf>,
}

impl StorageInstance {
    pub async fn new() -> StorageInstance {
        let root = runtime::storage::<StorageComponent>()
            .await
            .expect("failed to get storage location");
        tokio::fs::create_dir_all(root.join("storage"))
            .await
            .unwrap();
        let ring = RingStorage::load(root.join("ring.json")).await;
        StorageInstance {
            ring: RwLock::new(ring),
            root: RwLock::new(root),
        }
    }

    pub async fn get_ring_config(&self) -> RingConfig {
        self.ring.read().await.config.clone()
    }

    pub async fn set_ring_config(&self, ring: RingConfig) -> () {
        self.ring.write().await.set(ring).await;
    }

    pub async fn ring_lookup(&self, scope: &str, key: &str) -> Option<String> {
        let composite = mk_composite_key(scope, key);
        self.ring
            .read()
            .await
            .ring
            .lookup(&composite)
            .next()
            .cloned()
    }

    pub async fn get_here(&self, scope: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        let root = self.root.read().await;
        let path = root.join("storage").join(mk_sha256(scope, key));
        if path.exists() {
            tokio::fs::read(path).await.map(|x| Some(x))
        } else {
            Ok(None)
        }
    }

    pub async fn put_here(&self, scope: &str, key: &str, data: &[u8]) -> io::Result<Vec<u8>> {
        let root = self.root.write().await;
        let path = root.join("storage").join(mk_sha256(scope, key));
        if path.exists() {
            let current = tokio::fs::read(&path).await?;
            let next = merge_in_scope(scope, &current, data).map_err(io::Error::other)?;
            tokio::fs::write(path, &next).await?;
            Ok(next)
        } else {
            tokio::fs::write(path, data).await?;
            Ok(data.to_owned())
        }
    }
}

fn mk_composite_key(scope: &str, key: &str) -> String {
    format!("{scope}\0{key}")
}

fn mk_sha256(scope: &str, key: &str) -> String {
    let digest = sha2::Sha256::digest(&mk_composite_key(scope, key));
    let bytes: [u8; 32] = digest.try_into().unwrap();
    format!("{}", Hex(&bytes))
}

struct RingStorage {
    path: PathBuf,
    config: RingConfig,
    ring: HashRing<String>,
}

impl RingStorage {
    async fn load(path: PathBuf) -> RingStorage {
        let config: RingConfig = if path.exists() {
            let data = tokio::fs::read(&path)
                .await
                .expect("could not read ring config file");
            serde_json::from_slice(&data).expect("could not parse ring config file")
        } else {
            RingConfig {
                nodes: HashMap::new(),
            }
        };
        let members = config.nodes.iter().map(|(k, v)| (k, v.clone()));
        let ring = HashRing::from_members(members);
        RingStorage { path, config, ring }
    }

    async fn set(&mut self, ring: RingConfig) {
        let data = serde_json::to_vec(&ring).expect("could not convert ring config to json");
        tokio::fs::write(&self.path, data)
            .await
            .expect("could not write ring config file");
        let members = ring.nodes.iter().map(|(k, v)| (k, v.clone()));
        self.ring = HashRing::from_members(members);
        self.config = ring;
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

pub fn instance() -> &'static StorageInstance {
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
