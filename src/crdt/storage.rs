use std::{io, path::PathBuf};

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;
use lockable::LockPool;
use tokio::sync::RwLock;

use crate::crdt::{
    merge_in_scope,
    ring::{HashRing, RingConfig},
};

pub struct StorageInstance {
    root: PathBuf,
    ring: RwLock<RingStorage>,
    files: LockPool<PathBuf>,
}

impl StorageInstance {
    pub async fn new() -> StorageInstance {
        let root = runtime::storage::<StorageComponent>()
            .await
            .expect("failed to get storage location");
        let ring = RingStorage::load(root.join("ring.json")).await;
        StorageInstance {
            root,
            ring: RwLock::new(ring),
            files: LockPool::new(),
        }
    }

    pub async fn get_ring_config(&self) -> Option<RingConfig> {
        self.ring
            .read()
            .await
            .config
            .as_ref()
            .map(|(x, _)| x.clone())
    }

    pub async fn set_ring_config(&self, ring: RingConfig) -> () {
        self.ring.write().await.set(ring).await;
    }

    async fn with_lock<F, T>(&self, scope: &str, key: &str, handle: F) -> T
    where
        F: AsyncFnOnce(PathBuf) -> T,
    {
        let path = mk_path(scope, key);
        let lock = self.files.async_lock(path.clone()).await;
        let res = handle(self.root.join("storage").join(path)).await;
        std::mem::drop(lock);
        res
    }

    pub async fn with_ring<F, T>(&self, handle: F) -> Option<T>
    where
        F: FnOnce(&RingConfig, &HashRing) -> T,
    {
        self.ring
            .read()
            .await
            .config
            .as_ref()
            .map(|(cf, r)| handle(&cf, &r))
    }

    pub async fn get_here(&self, scope: &str, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.with_lock(scope, key, async |path| {
            if path.exists() {
                tokio::fs::read(path).await.map(|x| Some(x))
            } else {
                Ok(None)
            }
        })
        .await
    }

    pub async fn put_here(&self, scope: &str, key: &str, data: &[u8]) -> io::Result<Vec<u8>> {
        self.with_lock(scope, key, async |path| {
            if path.exists() {
                let current = tokio::fs::read(&path).await?;
                let next = merge_in_scope(scope, &current, data).map_err(io::Error::other)?;
                tokio::fs::write(path, &next).await?;
                Ok(next)
            } else {
                tokio::fs::create_dir_all(path.parent().unwrap()).await?;
                tokio::fs::write(path, data).await?;
                Ok(data.to_owned())
            }
        })
        .await
    }

    pub async fn delete_here(&self, scope: &str, key: &str) -> io::Result<()> {
        self.with_lock(scope, key, async |path| {
            if path.exists() {
                tokio::fs::remove_file(path).await?;
            }
            Ok(())
        })
        .await
    }
}

fn mk_path(scope: &str, key: &str) -> PathBuf {
    let scope = mk_sanitized(scope);
    let key = mk_sanitized(key);
    scope.join(key)
}

fn mk_sanitized(x: &str) -> PathBuf {
    let x = x
        .replace("%", "%25")
        .replace("*", "%2A")
        .replace("/", "%2F")
        .replace("?", "%3F")
        .replace("\0", "%00");
    PathBuf::from(x)
}

struct RingStorage {
    path: PathBuf,
    config: Option<(RingConfig, HashRing)>,
}

impl RingStorage {
    async fn load(path: PathBuf) -> RingStorage {
        let config = if path.exists() {
            let data = tokio::fs::read(&path)
                .await
                .expect("could not read ring config file");
            let config = serde_json::from_slice(&data).expect("could not parse ring config file");
            let ring = HashRing::from_config(&config);
            Some((config, ring))
        } else {
            None
        };
        RingStorage { path, config }
    }

    async fn set(&mut self, config: RingConfig) {
        let data = serde_json::to_vec(&config).expect("could not convert ring config to json");
        tokio::fs::write(&self.path, data)
            .await
            .expect("could not write ring config file");
        let ring = HashRing::from_config(&config);
        self.config = Some((config, ring));
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
