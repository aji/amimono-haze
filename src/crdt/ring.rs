use std::collections::{HashMap, HashSet};

use amimono::runtime::Location;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::util::hex::Hex;

/// Types that can be used as keys in the hash ring
pub trait RingKey {
    fn as_sha256(&self) -> [u8; 32];
}

/// A virtual node ID. Each physical node will have multiple of these in the
/// consistent hash ring, for a more even partitioning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VirtualNodeId(pub String);

impl RingKey for VirtualNodeId {
    fn as_sha256(&self) -> [u8; 32] {
        let hash = sha2::Sha256::digest(&self.0);
        hash.try_into().unwrap()
    }
}

/// A stable network ID. Each physical node has exactly one of these.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NetworkId(pub String);

impl NetworkId {
    pub fn as_location(&self) -> Location {
        Location::Stable(self.0.clone())
    }
}

/// A full configuration of a consistent hash ring.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RingConfig {
    /// A map of virtual ring node IDs to network IDs. The ring is implied by
    /// the sha256 hashes of the virtual nodes.
    pub nodes: HashMap<VirtualNodeId, NetworkId>,

    /// In-progress modification.
    pub update: Option<RingUpdateConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RingUpdateConfig {
    ToAdd { vn: VirtualNodeId, ni: NetworkId },
    ToRemove { vn: VirtualNodeId, ni: NetworkId },
}

impl RingConfig {
    /// Get the network ID for a virtual node
    pub fn network_id(&self, vn: &VirtualNodeId) -> Option<&NetworkId> {
        self.nodes.get(vn)
    }

    /// Get the weight of each network ID in the ring
    pub fn weights(&self) -> HashMap<NetworkId, usize> {
        let mut res = HashMap::new();
        for (_, ni) in self.nodes.iter() {
            *res.entry(ni.clone()).or_insert(0) += 1;
        }
        res
    }
}

/// A queryable hash ring data structure.
pub struct HashRing {
    /// A sorted list of (hash, node) pairs, where each item represents a key
    /// range starting from the given hash inclusively and continuing up to the
    /// next hash in the Vec (or wrapping around)
    data: Vec<(String, VirtualNodeId)>,
}

impl HashRing {
    /// Create a new hash ring from a RingConfig
    pub fn from_config(cf: &RingConfig) -> HashRing {
        HashRing::from_nodes(cf.nodes.keys().cloned())
    }

    /// Create a new hash ring from a list of virtual nodes.
    pub fn from_nodes(nodes: impl Iterator<Item = VirtualNodeId>) -> HashRing {
        let mut data: Vec<(String, VirtualNodeId)> = nodes
            .map(|n| (format!("{}", Hex(n.as_sha256())), n))
            .collect();
        data.sort();
        HashRing { data }
    }

    /// Create a new hash ring that includes the given virtual node.
    pub fn with_node(&self, vn: VirtualNodeId) -> HashRing {
        let mut data = self.data.clone();
        data.push((format!("{}", Hex(vn.as_sha256())), vn));
        data.sort();
        HashRing { data }
    }

    /// A cursor for navigating the hash ring, starting at a given point.
    pub fn cursor(&'_ self, start: &impl RingKey) -> HashRingCursor<'_> {
        HashRingCursor::new(self, start)
    }
}

/// A cursor around a hash ring.
pub struct HashRingCursor<'r> {
    ring: &'r HashRing,
    i: usize,
}

impl<'r> HashRingCursor<'r> {
    /// Create a new hash ring cursor pointing at the range in which the given
    /// key falls.
    pub fn new(ring: &'r HashRing, at: &impl RingKey) -> HashRingCursor<'r> {
        let hash = format!("{}", Hex(at.as_sha256()));
        let n = ring.data.len();
        let i = match ring.data.binary_search_by(|x| x.0.cmp(&hash)) {
            Ok(i) => i,
            Err(i) => (i + n - 1) % n,
        };
        HashRingCursor { ring, i }
    }

    /// Get the virtual node that represents the current key range.
    pub fn get(&self) -> &'r VirtualNodeId {
        &self.ring.data[self.i].1
    }

    /// Get a new cursor pointing at the previous range.
    pub fn prev(&self) -> HashRingCursor<'r> {
        let n = self.ring.data.len();
        let i = (self.i + n - 1) % n;
        HashRingCursor { ring: self.ring, i }
    }
}
