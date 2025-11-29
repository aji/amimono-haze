use std::{collections::BTreeMap, fmt::Debug};

use sha2::Digest;

/// A basic consistent hash ring data structure.
pub struct HashRing<M> {
    ring: BTreeMap<u128, M>,
}

impl<M: Debug> HashRing<M> {
    /// Create an empty hash ring.
    pub fn new() -> HashRing<M> {
        HashRing {
            ring: BTreeMap::new(),
        }
    }

    /// Create a hash ring from a list of members
    pub fn from_members(mems: impl Iterator<Item = (impl AsRef<str>, M)>) -> HashRing<M> {
        let mut ring = HashRing::new();
        for (name, member) in mems {
            ring.add_member(name, member);
        }
        ring
    }

    /// Add a member to the consistent hash ring. The "name" must be a stable
    /// identifier, while the "member" value can be anything deemed useful.
    pub fn add_member(&mut self, name: impl AsRef<str>, member: M) {
        let id = sha256_u128(name.as_ref());

        // We can't check the member data is the same, and adding a member with
        // the same name probably indicates a bug or configuration problem.
        //
        // There is a small probability that two distinct members hash to the
        // same value. The odds of hitting this (assuming the hashes are
        // uniformly pseudorandom) are astronomically low, however, even if
        // we've already inserted billions of keys. If somehow you are supremely
        // unlucky and it happens to you: congrats, you've been hit by an
        // asteroid. Back up your data ASAP and get in touch.
        if let Some(exists) = self.ring.insert(id, member) {
            panic!("name collision on {}: {:?}", name.as_ref(), exists);
        }
    }

    /// Return an iterator over members starting from the given key.
    pub fn lookup(&self, key: impl AsRef<[u8]>) -> impl Iterator<Item = &M> {
        let at = sha256_u128(key);
        self.ring
            .range(at..)
            .chain(self.ring.range(..at))
            .map(|(_, v)| v)
    }
}

fn sha256_u128(key: impl AsRef<[u8]>) -> u128 {
    let hash = sha2::Sha256::digest(key);
    let bytes: [u8; 16] = hash[..16].try_into().unwrap();
    u128::from_be_bytes(bytes)
}
