use amimono::{
    config::{Binding, ComponentConfig},
    runtime::{self, Component},
};
use futures::future::BoxFuture;
use lmdb::{Cursor, Transaction};

pub struct StorageInstance {
    env: lmdb::Environment,
    scopes: lmdb::Database,
    scope_data: lmdb::Database,
}

impl StorageInstance {
    async fn new() -> StorageInstance {
        let path = runtime::storage::<StorageComponent>()
            .await
            .expect("could not get storage location");
        let env = lmdb::Environment::new()
            .set_max_dbs(2)
            .open(&path)
            .expect("could not open LMDB environment");
        let scopes = env
            .create_db(Some("scopes"), lmdb::DatabaseFlags::empty())
            .expect("could not open LMDB 'scopes' database");
        let scope_data = env
            .create_db(Some("scope_data"), lmdb::DatabaseFlags::empty())
            .expect("could not open LMDB 'scope_data' database");
        StorageInstance {
            env,
            scopes,
            scope_data,
        }
    }

    fn encode_scope_key(&self, scope: &str, key: &str) -> Vec<u8> {
        format!("{}/{}", scope, key).into_bytes()
    }

    fn decode_scope_key<'a>(&self, scope_key: &'a [u8]) -> (&'a str, &'a str) {
        let scope_key = str::from_utf8(scope_key).unwrap();
        scope_key.split_once('/').unwrap()
    }

    pub fn delete(&self, scope: String, key: String) -> lmdb::Result<bool> {
        let scope_key = self.encode_scope_key(&scope, &key);
        let mut tx = self.env.begin_rw_txn()?;
        tx.del(self.scope_data, &scope_key, None)?;
        tx.commit()?;
        Ok(true)
    }

    pub fn get(&self, scope: String, key: String) -> lmdb::Result<Option<String>> {
        let scope_key = self.encode_scope_key(&scope, &key);
        let tx = self.env.begin_ro_txn()?;
        let res = match tx.get(self.scope_data, &scope_key) {
            Ok(item) => Ok(Some(str::from_utf8(item).unwrap().to_owned())),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        };
        tx.commit()?;
        res
    }

    pub fn list(&self, scope: String, key_prefix: String) -> lmdb::Result<Vec<(String, String)>> {
        let scope_key = self.encode_scope_key(&scope, &key_prefix);
        let tx = self.env.begin_ro_txn()?;
        let res = {
            let mut cur = tx.open_ro_cursor(self.scope_data)?;
            cur.iter_from(&scope_key)
                .take_while(|(k, _)| k.starts_with(&scope_key))
                .map(|(k, v)| {
                    let (_, key) = self.decode_scope_key(k);
                    let val = str::from_utf8(v).unwrap();
                    (key.to_owned(), val.to_owned())
                })
                .collect()
        };
        tx.commit()?;
        Ok(res)
    }

    pub fn list_scopes(&self) -> lmdb::Result<Vec<String>> {
        let tx = self.env.begin_ro_txn()?;
        let res = {
            let mut cur = tx.open_ro_cursor(self.scopes)?;
            cur.iter_start()
                .map(|(k, _)| str::from_utf8(k).unwrap().to_owned())
                .collect()
        };
        tx.commit()?;
        Ok(res)
    }

    pub fn put(&self, scope: String, key: String, val: String) -> lmdb::Result<bool> {
        let scope_key = self.encode_scope_key(&scope, &key);
        let mut tx = self.env.begin_rw_txn()?;
        tx.put(
            self.scopes,
            &scope.as_bytes(),
            &[],
            lmdb::WriteFlags::empty(),
        )?;
        tx.put(
            self.scope_data,
            &scope_key,
            &val.as_bytes(),
            lmdb::WriteFlags::empty(),
        )?;
        tx.commit()?;
        Ok(true)
    }
}

struct StorageComponent;

impl Component for StorageComponent {
    type Instance = StorageInstance;
}

fn md_storage_entry() -> BoxFuture<'static, ()> {
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
        label: format!("{prefix}-md-storage"),
        id: StorageComponent::id(),
        binding: Binding::None,
        is_stateful: true,
        entry: md_storage_entry,
    }
}
