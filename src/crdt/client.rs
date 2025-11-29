use std::marker::PhantomData;

use amimono::rpc::{RpcError, RpcResult};

use crate::crdt::{StoredCrdt, check_scope, router::CrdtRouterClient};

/// A CRDT client bound to a particular scope.
pub struct CrdtClient<T: StoredCrdt> {
    scope: String,
    router: CrdtRouterClient,
    _marker: PhantomData<T>,
}

impl<T: StoredCrdt> CrdtClient<T> {
    /// Create a new CRDT client bound to a particular scope. The type parameter
    /// `T` must have been previously bound to the same scope.
    pub fn new(scope: String) -> CrdtClient<T> {
        if !check_scope::<T>(&scope) {
            panic!("wrong StoredCrdt impl for scope {scope}");
        }
        CrdtClient {
            scope: scope,
            router: CrdtRouterClient::new(),
            _marker: PhantomData,
        }
    }

    /// Get a value.
    pub async fn get(&self, key: &str) -> RpcResult<Option<T>> {
        let data = self.router.get(self.scope.clone(), key.to_owned()).await?;
        let res = match data {
            Some(x) => {
                let parse = serde_json::from_slice(&x)
                    .map_err(|e| RpcError::Misc(format!("parse failed: {e}")))?;
                Some(parse)
            }
            None => None,
        };
        Ok(res)
    }

    /// Put a value, and return the updated value.
    pub async fn put(&self, key: &str, value: T) -> RpcResult<T> {
        let data = serde_json::to_vec(&value)
            .map_err(|e| RpcError::Misc(format!("serialize failed: {e}")))?;
        let res = self
            .router
            .put(self.scope.clone(), key.to_owned(), data)
            .await?;
        let res_parsed = serde_json::from_slice(&res)
            .map_err(|e| RpcError::Misc(format!("parse failed: {e}")))?;
        Ok(res_parsed)
    }
}

impl<T: StoredCrdt + Default> CrdtClient<T> {
    /// Get a value, or return the default value.
    pub async fn get_or_default(&self, key: &str) -> RpcResult<T> {
        self.get(key).await.map(|x| x.unwrap_or_default())
    }
}
