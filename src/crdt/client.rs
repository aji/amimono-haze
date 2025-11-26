use std::marker::PhantomData;

use crate::crdt::{StoredCrdt, check_scope};

pub struct CrdtClient<T: StoredCrdt> {
    _scope: String,
    _marker: PhantomData<T>,
}

impl<T: StoredCrdt> CrdtClient<T> {
    pub fn new(scope: String) -> CrdtClient<T> {
        if !check_scope::<T>(&scope) {
            panic!("wrong StoredCrdt impl for scope {scope}");
        }
        CrdtClient {
            _scope: scope,
            _marker: PhantomData,
        }
    }

    pub async fn get(&self, _key: &str) -> Option<T> {
        None
    }
}

impl<T: StoredCrdt + Default> CrdtClient<T> {
    pub async fn get_or_default(&self, key: &str) -> T {
        self.get(key).await.unwrap_or_default()
    }
}
