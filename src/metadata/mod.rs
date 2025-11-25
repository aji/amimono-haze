use std::collections::{BTreeMap, HashMap};

use amimono::{
    config::{AppBuilder, JobBuilder},
    rpc::RpcResult,
};
use tokio::sync::RwLock;

mod ops {
    amimono::rpc_ops! {
        fn delete(scope: String, key: String) -> bool;
        fn get(scope: String, key: String) -> Option<String>;
        fn put(scope: String, key: String, value: String) -> bool;
        fn list(scope: String, key_prefix: String) -> Vec<(String, String)>;
        fn list_scopes() -> Vec<String>;
    }
}

pub struct MetadataService {
    data: RwLock<HashMap<String, BTreeMap<String, String>>>,
}

impl ops::Handler for MetadataService {
    async fn new() -> Self {
        MetadataService {
            data: RwLock::new(HashMap::new()),
        }
    }

    async fn delete(&self, scope: String, key: String) -> RpcResult<bool> {
        let res = self
            .data
            .write()
            .await
            .get_mut(&scope)
            .and_then(|map| map.remove(&key))
            .is_some();
        Ok(res)
    }

    async fn get(&self, scope: String, key: String) -> RpcResult<Option<String>> {
        let res = self
            .data
            .read()
            .await
            .get(&scope)
            .and_then(|x| x.get(&key).cloned());
        Ok(res)
    }

    async fn put(&self, scope: String, key: String, value: String) -> RpcResult<bool> {
        self.data
            .write()
            .await
            .entry(scope)
            .or_default()
            .insert(key, value);
        Ok(true)
    }

    async fn list(&self, scope: String, key_prefix: String) -> RpcResult<Vec<(String, String)>> {
        let res = self
            .data
            .read()
            .await
            .get(&scope)
            .map(|map| {
                map.range(key_prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(&key_prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(res)
    }

    async fn list_scopes(&self) -> RpcResult<Vec<String>> {
        let res = self.data.read().await.keys().cloned().collect();
        Ok(res)
    }
}

pub type MetadataClient = ops::Client<MetadataService>;

pub fn install(app: &mut AppBuilder, prefix: &str) {
    let label = format!("{}metadata", prefix);
    app.add_job(JobBuilder::new().add_component(ops::component::<MetadataService>(label)));
}
