use crate::{
    dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult},
    metadata::MetadataClient,
};

pub struct MetadataDirectory;

impl Directory for MetadataDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let scopes = MetadataClient::new()
            .list_scopes()
            .await?
            .into_iter()
            .map(DirEntry::dir)
            .collect();

        Ok(scopes)
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::NotFound)
    }

    async fn open_dir(&self, name: &str) -> TreeResult<Box<dyn BoxDirectory>> {
        Ok(MetadataScopeDirectory(name.to_owned()).boxed())
    }
}

struct MetadataScopeDirectory(String);

impl Directory for MetadataScopeDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        let items = MetadataClient::new()
            .list(self.0.clone(), format!(""))
            .await?
            .into_iter()
            .map(|(k, _)| DirEntry::item(k))
            .collect();

        Ok(items)
    }

    async fn open_item(&self, name: &str) -> TreeResult<Item> {
        MetadataClient::new()
            .get(self.0.clone(), name.to_owned())
            .await?
            .ok_or(TreeError::NotFound)
            .map(|it| Item { value: it })
    }

    async fn open_dir(&self, _name: &str) -> TreeResult<Box<dyn BoxDirectory>> {
        Err(TreeError::NotFound)
    }
}
