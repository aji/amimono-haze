use amimono::{
    config::ComponentConfig,
    rpc::{RpcError, RpcResult},
    runtime::Location,
};

use crate::crdt::{
    storage::{self, StorageInstance},
    types::RingConfig,
};

mod ops {
    use crate::crdt::types::RingConfig;

    amimono::rpc_ops! {
        fn get(scope: String, key: String) -> Option<Vec<u8>>;
        fn put(scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        fn get_here(scope: String, key: String) -> Option<Vec<u8>>;
        fn put_here(scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        fn get_ring() -> RingConfig;
        fn set_ring(ring: RingConfig) -> ();
    }
}

pub struct CrdtRouter {
    storage: &'static StorageInstance,
    router: CrdtRouterClient,
}

impl ops::Handler for CrdtRouter {
    async fn new() -> CrdtRouter {
        CrdtRouter {
            storage: storage::instance(),
            router: CrdtRouterClient::new(),
        }
    }

    async fn get(&self, scope: String, key: String) -> RpcResult<Option<Vec<u8>>> {
        match self.storage.ring_lookup(&scope, &key).await {
            Some(tgt) => {
                self.router
                    .at(Location::Http(tgt.clone()))
                    .get_here(scope, key)
                    .await
            }
            None => Err(RpcError::Misc(format!("no ring configuration"))),
        }
    }

    async fn put(&self, scope: String, key: String, data: Vec<u8>) -> RpcResult<Vec<u8>> {
        match self.storage.ring_lookup(&scope, &key).await {
            Some(tgt) => {
                self.router
                    .at(Location::Http(tgt.clone()))
                    .put_here(scope, key, data)
                    .await
            }
            None => Err(RpcError::Misc(format!("no ring configuration"))),
        }
    }

    async fn get_here(&self, scope: String, key: String) -> RpcResult<Option<Vec<u8>>> {
        self.storage
            .get_here(&scope, &key)
            .await
            .map_err(|e| RpcError::Misc(format!("get failed: {e}")))
    }

    async fn put_here(&self, scope: String, key: String, data: Vec<u8>) -> RpcResult<Vec<u8>> {
        self.storage
            .put_here(&scope, &key, &data)
            .await
            .map_err(|e| RpcError::Misc(format!("put failed: {e}")))
    }

    async fn get_ring(&self) -> RpcResult<RingConfig> {
        Ok(self.storage.get_ring_config().await)
    }

    async fn set_ring(&self, ring: RingConfig) -> RpcResult<()> {
        Ok(self.storage.set_ring_config(ring).await)
    }
}

pub type CrdtRouterComponent = ops::Component<CrdtRouter>;

pub type CrdtRouterClient = ops::Client<CrdtRouter>;

pub fn component(prefix: &str) -> ComponentConfig {
    ops::component::<CrdtRouter>(format!("{prefix}-crdt-router"))
}
