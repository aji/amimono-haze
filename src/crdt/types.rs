use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A full configuration of a consistent hashing ring.
#[derive(Clone, Serialize, Deserialize)]
pub struct RingConfig {
    /// A map of virtual ring node IDs to network IDs.
    pub nodes: HashMap<String, String>,
}

impl RingConfig {
    pub fn singleton(node: &str) -> RingConfig {
        let mut nodes = HashMap::new();
        nodes.insert(node.to_owned(), node.to_owned());
        RingConfig { nodes }
    }
}
