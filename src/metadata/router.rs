use amimono::{
    config::ComponentConfig,
    rpc::{RpcError, RpcResult},
};

use crate::metadata::storage;

mod ops {
    amimono::rpc_ops! {
        fn delete(scope: String, key: String) -> bool;
        fn get(scope: String, key: String) -> Option<String>;
        fn put(scope: String, key: String, value: String) -> bool;
        fn list(scope: String, key_prefix: String) -> Vec<(String, String)>;
        fn list_scopes() -> Vec<String>;
    }
}

pub struct MetadataService;

impl ops::Handler for MetadataService {
    async fn new() -> Self {
        MetadataService
    }

    async fn delete(&self, scope: String, key: String) -> RpcResult<bool> {
        storage::instance().delete(scope, key).map_err(lmdb_to_rpc)
    }

    async fn get(&self, scope: String, key: String) -> RpcResult<Option<String>> {
        storage::instance().get(scope, key).map_err(lmdb_to_rpc)
    }

    async fn put(&self, scope: String, key: String, value: String) -> RpcResult<bool> {
        storage::instance()
            .put(scope, key, value)
            .map_err(lmdb_to_rpc)
    }

    async fn list(&self, scope: String, key_prefix: String) -> RpcResult<Vec<(String, String)>> {
        storage::instance()
            .list(scope, key_prefix)
            .map_err(lmdb_to_rpc)
    }

    async fn list_scopes(&self) -> RpcResult<Vec<String>> {
        storage::instance().list_scopes().map_err(lmdb_to_rpc)
    }
}

fn lmdb_to_rpc(e: lmdb::Error) -> RpcError {
    RpcError::Misc(format!("{}", e))
}

pub type MetadataClient = ops::Client<MetadataService>;

pub fn component(prefix: &str) -> ComponentConfig {
    ops::component::<MetadataService>(format!("{}md-router", prefix))
}
