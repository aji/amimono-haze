use std::path::PathBuf;

use amimono::{
    config::{BindingType, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;
use sha2::Digest;

pub struct StorageInstance {
    root: PathBuf,
}

impl StorageInstance {
    async fn new() -> Self {
        StorageInstance {
            root: runtime::storage::<StorageComponent>()
                .await
                .expect("could not get storage root"),
        }
    }

    pub async fn put_here(&self, data: &[u8]) -> Result<String, String> {
        let id = {
            let digest = sha2::Sha256::digest(data);
            let id = digest.as_slice();
            assert!(id.len() == 32);
            id.iter().map(|b| format!("{:02x}", b)).collect::<String>()
        };

        let path_parent = self.root.join(&id[..2]);
        let path = path_parent.join(&id[2..]);
        std::fs::create_dir_all(path_parent)
            .map_err(|e| format!("failed to create blob dir: {e:?}"))?;
        std::fs::write(&path, data).map_err(|e| format!("failed to write blob data: {e:?}"))?;

        Ok(format!("blob:{}", id))
    }

    pub async fn get_here(&self, id: &str) -> Result<Option<Vec<u8>>, String> {
        let hash = if !id.starts_with("blob:") || id.len() != 37 {
            return Err("invalid blob id".to_string());
        } else {
            &id[5..]
        };

        let path_parent = self.root.join(&hash[..2]);
        let path = path_parent.join(&hash[2..]);

        if path.exists() {
            let data =
                std::fs::read(&path).map_err(|e| format!("failed to read blob data: {e:?}"))?;
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

struct StorageComponent;

impl Component for StorageComponent {
    type Instance = StorageInstance;
}

fn blob_storage_entry() -> BoxFuture<'static, ()> {
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
        label: format!("{prefix}-blob-storage"),
        id: StorageComponent::id(),
        binding: BindingType::None,
        is_stateful: true,
        entry: blob_storage_entry,
    }
}
