use crate::{
    dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult},
    dht::controller::DhtClient,
};

pub struct DhtDirectory;

impl Directory for DhtDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let res = DhtClient::new()
            .list_scopes()
            .await?
            .into_iter()
            .map(DirEntry::dir)
            .collect();
        Ok(res)
    }

    async fn open_dir(&self, name: &str) -> TreeResult<BoxDirectory> {
        Ok(DhtScopeDirectory(name.to_owned()).boxed())
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }
}

pub struct DhtScopeDirectory(String);

impl Directory for DhtScopeDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let res = DhtClient::new()
            .list(self.0.clone(), "".to_string())
            .await?
            .into_iter()
            .map(DirEntry::item)
            .collect();
        Ok(res)
    }

    async fn open_dir(&self, _name: &str) -> TreeResult<BoxDirectory> {
        Err(TreeError::NotFound)
    }

    async fn open_item(&self, name: &str) -> TreeResult<Item> {
        let item = DhtClient::new()
            .get(self.0.clone(), name.to_owned())
            .await?
            .ok_or(TreeError::NotFound)?;
        Ok(Item::new(String::from_utf8_lossy(&item)))
    }
}
