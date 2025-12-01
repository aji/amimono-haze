use amimono::{
    config::ComponentConfig,
    rpc::{RpcError, RpcResult},
    runtime::{self, Location},
};
use futures::join;
use rand::seq::SliceRandom;
use sha2::Digest;

use crate::crdt::{
    merge_in_scope,
    ring::{HashRing, NetworkId, RingConfig, RingKey, RingUpdateConfig},
    storage::{self, StorageInstance},
};

mod ops {
    use crate::crdt::ring::RingConfig;

    amimono::rpc_ops! {
        // router endpoints
        fn get(ttl: u32, scope: String, key: String) -> Option<Vec<u8>>;
        fn put(ttl: u32, scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        // storage layer endpoints
        fn get_here(scope: String, key: String) -> Option<Vec<u8>>;
        fn put_here(scope: String, key: String, data: Vec<u8>) -> Vec<u8>;

        // controller endpoints
        fn updating() -> bool;
        fn get_ring() -> Option<RingConfig>;
        fn set_ring(ring: RingConfig) -> ();
    }
}

enum Action {
    Forward(NetworkId),
    Store,
    StoreAdding(NetworkId),
}

pub struct CrdtRouter {
    myself: NetworkId,
    storage: &'static StorageInstance,
    router: CrdtRouterClient,
}

impl CrdtRouter {
    fn action(&self, ck: &CompositeKey, cf: &RingConfig, ring: &HashRing) -> RpcResult<Action> {
        use RingUpdateConfig::*;

        let range = ring.range(ck);
        let vn0 = range.start();
        let ni0 = cf
            .network_id(vn0)
            .ok_or(RpcError::Misc(format!("ring config corrupted: {vn0:?}")))?
            .clone();

        if ni0 != self.myself {
            // not my circus, not my monkeys
            return Ok(Action::Forward(ni0));
        }

        let action = match &cf.update {
            Some(ToAdd { vn, ni }) => {
                if range.contains(vn) {
                    // this key is being migrated and may already have been moved
                    Action::StoreAdding(ni.clone())
                } else {
                    Action::Store
                }
            }
            Some(ToRemove { .. }) => {
                todo!()
            }
            None => Action::Store,
        };

        Ok(action)
    }

    async fn random_peer(&self) -> RpcResult<NetworkId> {
        // This is needed in the exceptional circumstance where a node has
        // freshly booted up, has no config, and receives a request. The request
        // is simply forwarded to a random node in the hopes that the target
        // will have a config.

        let myself = self.myself.as_location();
        let mut peers: Vec<NetworkId> = runtime::discover::<CrdtRouterComponent>()
            .await
            .map_err(|e| RpcError::Misc(format!("discovery failed: {e}")))?
            .into_iter()
            .filter(|x| *x != myself)
            .flat_map(|x| match x {
                Location::Ephemeral(_) => None,
                Location::Stable(s) => Some(NetworkId(s)),
            })
            .collect();
        peers.shuffle(&mut rand::rng());
        peers
            .into_iter()
            .next()
            .ok_or(RpcError::Misc(format!("no other peers")))
    }

    async fn sync_updater(&self) {
        // TODO
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

        let router = CrdtRouter {
            myself,
            storage: storage::instance(),
            router: CrdtRouterClient::new(),
        };
        router.sync_updater().await;
        router
    }

    async fn get(&self, ttl: u32, scope: String, key: String) -> RpcResult<Option<Vec<u8>>> {
        if ttl == 0 {
            return Err(RpcError::Misc(format!("ttl expired")));
        }

        let ck = CompositeKey { scope, key };

        let maybe_action = self
            .storage
            .with_ring(|cf, ring| self.action(&ck, cf, ring))
            .await;

        let action = match maybe_action {
            Some(a) => a?,
            None => Action::Forward(self.random_peer().await?),
        };

        match action {
            Action::Forward(to) => {
                self.router
                    .at(to.as_location())
                    .get(ttl - 1, ck.scope, ck.key)
                    .await
            }

            Action::Store => self.get_here(ck.scope, ck.key).await,

            Action::StoreAdding(to) => {
                let tgt = self.router.at(to.as_location());
                let (a, b) = join!(
                    self.get_here(ck.scope.clone(), ck.key.clone()),
                    tgt.get(ttl - 1, ck.scope.clone(), ck.key.clone())
                );
                let res = match (a?, b?) {
                    (None, None) => None,
                    (None, Some(b)) => Some(b),
                    (Some(a), None) => Some(a),
                    (Some(a), Some(b)) => {
                        let merged = merge_in_scope(&ck.scope, &a[..], &b[..])
                            .map_err(|e| RpcError::Misc(format!("merge failed: {e}")))?;
                        Some(merged)
                    }
                };
                Ok(res)
            }
        }
    }

    async fn put(&self, ttl: u32, scope: String, key: String, data: Vec<u8>) -> RpcResult<Vec<u8>> {
        if ttl == 0 {
            return Err(RpcError::Misc(format!("ttl expired")));
        }

        let ck = CompositeKey { scope, key };

        let maybe_action = self
            .storage
            .with_ring(|cf, ring| self.action(&ck, cf, ring))
            .await;

        let action = match maybe_action {
            Some(a) => a?,
            None => Action::Forward(self.random_peer().await?),
        };

        match action {
            Action::Forward(to) | Action::StoreAdding(to) => {
                self.router
                    .at(to.as_location())
                    .put(ttl - 1, ck.scope, ck.key, data)
                    .await
            }

            Action::Store => self.put_here(ck.scope, ck.key, data).await,
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
        Ok(rand::random_bool(0.9))
    }

    async fn get_ring(&self) -> RpcResult<Option<RingConfig>> {
        Ok(self.storage.get_ring_config().await)
    }

    async fn set_ring(&self, ring: RingConfig) -> RpcResult<()> {
        self.storage.set_ring_config(ring).await;
        self.sync_updater().await;
        Ok(())
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
