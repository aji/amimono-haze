use amimono::{
    config::{AppBuilder, JobBuilder},
    rpc::{RpcComponent, RpcError, RpcResult},
    runtime,
};

use crate::metadata::MetadataClient;

mod ops {
    amimono::rpc_ops! {
        fn put(scope: String, key: String, value: Vec<u8>) -> bool;
        fn get(scope: String, key: String) -> Option<Vec<u8>>;
        fn list(scope: String, key_prefix: String) -> Vec<(String, String)>;
        fn list_scopes() -> Vec<String>;
    }
}

pub struct DhtService {
    label: String,
    md: MetadataClient,
}

impl ops::Handler for DhtService {
    async fn new() -> Self {
        let label = runtime::label::<RpcComponent<ops::Instance<Self>>>().to_owned();
        let md = MetadataClient::new();

        if let Err(e) = md
            .put(label.clone(), "config".to_string(), "default".to_string())
            .await
        {
            panic!("failed to initialize dht metadata: {e:?}");
        }

        DhtService { label, md }
    }

    async fn put(&self, scope: String, key: String, value: Vec<u8>) -> RpcResult<bool> {
        let data = match str::from_utf8(value.as_slice()) {
            Ok(s) => s.to_owned(),
            Err(e) => return Err(RpcError::Misc(e.to_string())),
        };
        self.md
            .put(self.label.clone(), format!("data/{}/{}", scope, key), data)
            .await?;
        Ok(true)
    }

    async fn get(&self, scope: String, key: String) -> RpcResult<Option<Vec<u8>>> {
        let res = self
            .md
            .get(self.label.clone(), format!("data/{}/{}", scope, key))
            .await?
            .map(|s| s.into_bytes());
        Ok(res)
    }

    async fn list(&self, _scope: String, _key_prefix: String) -> RpcResult<Vec<(String, String)>> {
        Err(RpcError::Misc("Not implemented".to_string()))
    }

    async fn list_scopes(&self) -> RpcResult<Vec<String>> {
        Err(RpcError::Misc("Not implemented".to_string()))
    }
}

pub type DhtClient = ops::Client<DhtService>;

pub fn install(app: &mut AppBuilder, prefix: &str) {
    let label = format!("{}dht", prefix);
    app.add_job(JobBuilder::new().add_component(ops::component::<DhtService>(label)));
}
