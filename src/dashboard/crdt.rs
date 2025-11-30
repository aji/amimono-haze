use crate::{
    crdt::router::CrdtRouterClient,
    dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult},
};

pub struct CrdtDirectory;

impl Directory for CrdtDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        Ok(vec![DirEntry::item("config")])
    }

    async fn open_dir(&self, _name: &str) -> TreeResult<BoxDirectory> {
        Err(TreeError::NotFound)
    }

    async fn open_item(&self, name: &str) -> TreeResult<Item> {
        match name {
            "config" => Ok(Item::json(&CrdtRouterClient::new().get_ring().await?)),
            _ => Err(TreeError::NotFound),
        }
    }
}
