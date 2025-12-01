use amimono::{
    config::ComponentConfig,
    rpc::{RpcError, RpcResult},
    runtime::{self, Location},
};
use sha2::Digest;

use crate::crdt::{
    ring::{HashRing, NetworkId, RingConfig, RingKey},
    storage::{self, StorageInstance},
};

mod ops {
    use crate::crdt::ring::RingConfig;

    amimono::rpc_ops! {
        // public endpoints
        fn get(scope: String, key: String) -> Option<Vec<u8>>;
        fn put(scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        // storage layer endpoints
        fn get_here(scope: String, key: String) -> Option<Vec<u8>>;
        fn put_here(scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        // controller endpoints
        fn updating() -> bool;
        fn get_ring() -> Option<RingConfig>;
        fn set_ring(ring: RingConfig) -> ();
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Verb {
    Get,
    Put,
}

enum Action {
    Forward(NetworkId),
    Store,
}

pub struct CrdtRouter {
    myself: NetworkId,
    storage: &'static StorageInstance,
    router: CrdtRouterClient,
}

impl CrdtRouter {
    fn action(&self, _verb: Verb, ck: &CompositeKey, cf: &RingConfig, ring: &HashRing) -> Action {
        let ni = cf.network_id(ring.cursor(ck).get()).unwrap().clone();
        if ni == self.myself {
            Action::Store
        } else {
            Action::Forward(ni)
        }
    }
}

impl ops::Handler for CrdtRouter {
    async fn new() -> CrdtRouter {
        let myself = {
            let loc = runtime::myself::<CrdtRouterComponent>()
                .await
                .expect("could not get my location");
            match loc {
                Location::Ephemeral(_) => panic!("CrdtRouter cannot have an ephemeral location"),
                Location::Stable(x) => NetworkId(x),
            }
        };

        CrdtRouter {
            myself,
            storage: storage::instance(),
            router: CrdtRouterClient::new(),
        }
    }

    async fn get(&self, scope: String, key: String) -> RpcResult<Option<Vec<u8>>> {
        let ck = CompositeKey { scope, key };
        let action = self
            .storage
            .with_ring(|cf, ring| self.action(Verb::Get, &ck, cf, ring))
            .await
            .ok_or(RpcError::Misc(format!("no ring config")))?;
        if let Action::Forward(to) = action {
            self.router.at(to.as_location()).get(ck.scope, ck.key).await
        } else {
            self.get_here(ck.scope, ck.key).await
        }
    }

    async fn put(&self, scope: String, key: String, data: Vec<u8>) -> RpcResult<Vec<u8>> {
        let ck = CompositeKey { scope, key };
        let action = self
            .storage
            .with_ring(|cf, ring| self.action(Verb::Put, &ck, cf, ring))
            .await
            .ok_or(RpcError::Misc(format!("no ring config")))?;
        if let Action::Forward(to) = action {
            self.router
                .at(to.as_location())
                .put(ck.scope, ck.key, data)
                .await
        } else {
            self.put_here(ck.scope, ck.key, data).await
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

    async fn updating(&self) -> RpcResult<bool> {
        Ok(false)
    }

    async fn get_ring(&self) -> RpcResult<Option<RingConfig>> {
        Ok(self.storage.get_ring_config().await)
    }

    async fn set_ring(&self, ring: RingConfig) -> RpcResult<()> {
        Ok(self.storage.set_ring_config(ring).await)
    }
}

pub struct CompositeKey {
    pub scope: String,
    pub key: String,
}

impl RingKey for CompositeKey {
    fn as_sha256(&self) -> [u8; 32] {
        let data = format!("{}\0{}", self.scope, self.key);
        let hash = sha2::Sha256::digest(data);
        hash.try_into().unwrap()
    }
}

pub type CrdtRouterComponent = ops::Component<CrdtRouter>;

pub type CrdtRouterClient = ops::Client<CrdtRouter>;

pub fn component(prefix: &str) -> ComponentConfig {
    ops::component::<CrdtRouter>(format!("{prefix}-crdt-router"))
}
