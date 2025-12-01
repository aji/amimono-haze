use std::{io, path::PathBuf, time::Duration};

use amimono::{
    config::{Binding, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;
use lockable::LockPool;
use tokio::sync::{Mutex, RwLock};

use crate::crdt::{
    merge_in_scope,
    ring::{HashRing, NetworkId, RingConfig, RingUpdateConfig, VirtualNodeId},
    router::{CompositeKey, CrdtRouterClient},
};

pub struct StorageInstance {
    root: PathBuf,
    ring: RwLock<RingStorage>,
    updater: Mutex<Option<RingUpdateConfig>>,
    files: LockPool<PathBuf>,
}

impl StorageInstance {
    pub async fn new() -> StorageInstance {
        let root = runtime::storage::<StorageComponent>()
            .await
            .expect("failed to get storage location");
        let ring = RingStorage::load(root.join("ring.json")).await;
        std::fs::create_dir_all(root.join("storage")).unwrap();
        StorageInstance {
            root,
            ring: RwLock::new(ring),
            updater: Mutex::new(None),
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

    pub async fn updating(&self) -> bool {
        self.updater.lock().await.is_some()
    }

    pub async fn sync_updater(&'static self) {
        let ring = self.ring.read().await;
        let mut updater = self.updater.lock().await;

        let have_update = updater.as_ref();
        let want_update = ring.config.as_ref().and_then(|x| x.0.update.as_ref());

        match (have_update, want_update) {
            (None, None) => {}
            (None, Some(b)) => {
                *updater = Some(b.clone());
                tokio::spawn(self.run_update(b.clone()));
            }
            (Some(a), None) => panic!("update {a:?} canceled by ring config update!"),
            (Some(a), Some(b)) => {
                if a != b {
                    panic!("update replaced with non-equivalent update! {a:?} {b:?}");
                }
            }
        }
    }

    async fn run_update(&self, update: RingUpdateConfig) {
        match update {
            RingUpdateConfig::ToAdd { vn, ni } => self.run_to_add(vn, ni).await,
            RingUpdateConfig::ToRemove { .. } => todo!(),
        }

        let mut updater = self.updater.lock().await;
        *updater = None;
    }

    async fn run_to_add(&self, vn: VirtualNodeId, ni: NetworkId) {
        let router = CrdtRouterClient::new().at(ni.as_location());

        let range = {
            let range = self.ring.read().await.config.as_ref().unwrap().1.range(&vn);
            range.trim_start(vn)
        };

        loop {
            let mut num_failures = 0;
            let mut num_transferred = 0;
            for scope in self.list_scopes() {
                for key in self.list_keys_in_scope(&scope) {
                    let path = self.root.join("storage").join(mk_path(&scope, &key));
                    let ck = CompositeKey {
                        scope: scope.clone(),
                        key: key.clone(),
                    };
                    if range.contains(&ck) {
                        let data = tokio::fs::read(&path).await.unwrap();
                        let res = router.put_here(scope.clone(), key.clone(), data).await;
                        if let Ok(_) = res {
                            num_transferred += 1;
                            tokio::fs::remove_file(&path).await.unwrap();
                        } else {
                            num_failures += 1;
                        }
                    }
                }
            }
            if num_failures == 0 && num_transferred == 0 {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    fn list_scopes(&self) -> impl Iterator<Item = String> {
        std::fs::read_dir(self.root.join("storage"))
            .unwrap()
            .map(|x| x.unwrap().file_name().to_str().unwrap().to_owned())
    }

    fn list_keys_in_scope(&self, scope: &str) -> impl Iterator<Item = String> {
        std::fs::read_dir(self.root.join("storage").join(scope))
            .unwrap()
            .map(|x| x.unwrap().file_name().to_str().unwrap().to_owned())
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
        let data =
            serde_json::to_vec_pretty(&config).expect("could not convert ring config to json");
        tokio::fs::write(&self.path, data)
            .await
            .expect("write ring config failed");
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
        binding: Binding::None,
        is_stateful: true,
        entry: storage_main,
    }
}
