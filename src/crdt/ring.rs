use std::collections::HashMap;

use amimono::runtime::Location;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::util::hex::Hex;

/// Types that can be used as keys in the hash ring
pub trait RingKey {
    fn as_sha256(&self) -> [u8; 32];

    fn as_sha256_string(&self) -> String {
        format!("{}", Hex(self.as_sha256()))
    }
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
        let mut data: Vec<(String, VirtualNodeId)> =
            nodes.map(|n| (n.as_sha256_string(), n)).collect();
        data.sort();
        HashRing { data }
    }

    /// A cursor for navigating the hash ring, starting at a given point.
    pub fn cursor(&'_ self, start: &impl RingKey) -> HashRingCursor<'_> {
        HashRingCursor::new(self, start)
    }

    /// Get the range including the given point
    pub fn range(&'_ self, containing: &impl RingKey) -> HashRingRange {
        self.cursor(containing).range()
    }
}

/// A cursor for navigating the hash ring
pub struct HashRingCursor<'r> {
    ring: &'r HashRing,
    i: usize,
}

impl<'r> HashRingCursor<'r> {
    /// Create a new hash ring cursor pointing at the range in which the given
    /// key falls.
    pub fn new(ring: &'r HashRing, at: &impl RingKey) -> HashRingCursor<'r> {
        let hash = at.as_sha256_string();
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

    /// Get a cursor representing the next range.
    pub fn next(&self) -> HashRingCursor<'r> {
        let n = self.ring.data.len();
        HashRingCursor {
            ring: self.ring,
            i: (self.i + 1) % n,
        }
    }

    /// Get a range object for the current cursor position
    pub fn range(&self) -> HashRingRange {
        let a = self.get().clone();
        let b = self.next().get().clone();
        HashRingRange::new(a, b)
    }
}

/// A range in a hash ring, represented by a pair of virtual nodes.
pub struct HashRingRange {
    a_hash: String,
    b_hash: String,
    a: VirtualNodeId,
    b: VirtualNodeId,
}

impl HashRingRange {
    fn new(a: VirtualNodeId, b: VirtualNodeId) -> HashRingRange {
        HashRingRange {
            a_hash: a.as_sha256_string(),
            b_hash: b.as_sha256_string(),
            a,
            b,
        }
    }

    /// Get the virtual node ID for the start of the range
    pub fn start(&self) -> &VirtualNodeId {
        &self.a
    }

    /// Tests whether the given point is contained in this range
    pub fn contains(&self, pt: &impl RingKey) -> bool {
        use std::cmp::Ordering::*;
        let x_hash = pt.as_sha256_string();
        match self.a_hash.cmp(&self.b_hash) {
            Equal => false, // range is empty. this should never happen, though
            Less => self.a_hash <= x_hash && x_hash < self.b_hash,
            Greater => self.a_hash <= x_hash || x_hash < self.b_hash,
        }
    }

    /// Creates a new range with an updated starting node ID
    pub fn trim_start(&self, vn: VirtualNodeId) -> HashRingRange {
        assert!(self.contains(&vn));
        HashRingRange {
            a_hash: vn.as_sha256_string(),
            b_hash: self.b_hash.clone(),
            a: vn,
            b: self.b.clone(),
        }
    }
}
