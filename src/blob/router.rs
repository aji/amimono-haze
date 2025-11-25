use amimono::{
    config::ComponentConfig,
    rpc::{RpcError, RpcResult},
    runtime,
};
use rand::seq::{IndexedRandom, SliceRandom};

use crate::blob::storage;

mod ops {
    amimono::rpc_ops! {
        fn put(data: Vec<u8>) -> String;
        fn get(id: String) -> Option<Vec<u8>>;
        fn delete(id: String) -> bool;

        fn put_here(data: Vec<u8>) -> String;
        fn get_here(id: String) -> Option<Vec<u8>>;
    }
}

pub struct BlobService {
    client: ops::Client<BlobService>,
}

impl ops::Handler for BlobService {
    async fn new() -> Self {
        BlobService {
            client: ops::Client::new(),
        }
    }

    async fn put(&self, data: Vec<u8>) -> RpcResult<String> {
        let peer = {
            let peers = runtime::discover_all::<ops::Component<Self>>()
                .await
                .map_err(|e| RpcError::Misc(e.to_string()))?;
            peers.choose(&mut rand::rng()).cloned()
        };

        match peer {
            Some(other) => self.client.at(other).put_here(data).await,
            None => self.put_here(data).await,
        }
    }

    async fn get(&self, id: String) -> RpcResult<Option<Vec<u8>>> {
        if let Ok(Some(data)) = self.get_here(id.clone()).await {
            return Ok(Some(data));
        }

        let peers = {
            let mut peers = runtime::discover_all::<ops::Component<Self>>()
                .await
                .map_err(|e| RpcError::Misc(e.to_string()))?;
            peers.shuffle(&mut rand::rng());
            peers
        };

        for peer in peers.into_iter() {
            if let Ok(Some(data)) = self.client.at(peer).get_here(id.clone()).await {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    async fn delete(&self, _id: String) -> RpcResult<bool> {
        Err(RpcError::Misc(
            "blob delete not implemented yet".to_string(),
        ))
    }

    async fn put_here(&self, data: Vec<u8>) -> RpcResult<String> {
        storage::instance()
            .put_here(&data[..])
            .await
            .map_err(|e| RpcError::Misc(e))
    }

    async fn get_here(&self, id: String) -> RpcResult<Option<Vec<u8>>> {
        storage::instance()
            .get_here(&id[..])
            .await
            .map_err(|e| RpcError::Misc(e))
    }
}

pub type BlobClient = ops::Client<BlobService>;

pub fn component(prefix: &str) -> ComponentConfig {
    ops::component::<BlobService>(format!("{}blob-router", prefix))
}
