use crate::dashboard::tree::{BoxDirectory, DirEntry, Directory, Item, TreeError, TreeResult};

pub struct DhtDirectory;

impl Directory for DhtDirectory {
    async fn list(&self) -> TreeResult<Vec<DirEntry>> {
        Err(TreeError::Other(format!("not implemented")))
    }

    async fn open_dir(&self, _name: &str) -> TreeResult<Box<dyn BoxDirectory>> {
        Err(TreeError::Other(format!("not implemented")))
    }

    async fn open_item(&self, _name: &str) -> TreeResult<Item> {
        Err(TreeError::Other(format!("not implemented")))
    }
}
