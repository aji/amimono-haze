use std::collections::{BTreeMap, HashMap, HashSet};

use sha2::Digest;

type MemberID = u128;

/// A basic consistent hash ring data structure.
pub struct HashRing<M> {
    arity: u32,
    members: HashMap<MemberID, M>,
    ring: BTreeMap<u128, MemberID>,
}

impl<M> HashRing<M> {
    pub fn new() -> HashRing<M> {
        HashRing {
            arity: 16,
            members: HashMap::new(),
            ring: BTreeMap::new(),
        }
    }

    /// Add a member to the consistent hash ring. The "name" must be a stable
    /// identifier, while the "member" value can be anything deemed useful.
    pub fn add_member(&mut self, name: impl AsRef<str>, member: M) {
        let id = sha256_u128(name.as_ref());

        // We can't check the member data is the same, and adding a member with
        // the same name probably indicates a bug or configuration problem.
        assert!(
            self.members.insert(id, member).is_none(),
            "member {} already in map",
            name.as_ref()
        );

        for i in 0..self.arity {
            let at = sha256_u128(member_nth(&name, i));

            // Plugging some of the numbers into a calculator, the odds of
            // hitting this (assuming the hashes are uniformly pseudorandom) are
            // astronomically low, even if we've already inserted billions of
            // keys. The added complexity to handle this possibility is not
            // worth it. If somehow you are supremely unlucky and it happens to
            // you: congrats, you've been hit by an asteroid. Back up your data
            // ASAP and get in touch.
            assert!(
                self.ring.insert(at, id).is_none(),
                "name collision on {}:{i} THIS IS VERY BAD",
                name.as_ref()
            );
        }
    }

    /// Remove a member from the consistent hash ring.
    pub fn remove_member(&mut self, name: impl AsRef<str>) {
        let id = sha256_u128(name.as_ref());
        self.members.remove(&id);

        for i in 0..self.arity {
            let at = sha256_u128(member_nth(&name, i));
            self.ring.remove(&at);
        }
    }

    /// Return an iterator over members starting from the given key. Members are
    /// deduplicated, meaning for single-member hash rings this will always
    /// return a single-element iterator.
    pub fn lookup(&self, key: impl AsRef<[u8]>) -> impl Iterator<Item = &M> {
        let mut seen = HashSet::new();
        let at = sha256_u128(key);
        self.ring
            .range(at..)
            .chain(self.ring.range(..at))
            .map(|(_, v)| *v)
            .filter(move |id| seen.insert(*id))
            .map(|id| self.members.get(&id).unwrap())
    }
}

fn member_nth(name: impl AsRef<str>, n: u32) -> Vec<u8> {
    let mut bytes = n.to_be_bytes().to_vec();
    let mut res = name.as_ref().as_bytes().to_owned();
    res.append(&mut bytes);
    res
}

fn sha256_u128(key: impl AsRef<[u8]>) -> u128 {
    let hash = sha2::Sha256::digest(key);
    let bytes: [u8; 16] = hash[..16].try_into().unwrap();
    u128::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::HashRing;

    #[test]
    fn test_single_member() {
        let ring = {
            let mut ring = HashRing::new();
            ring.add_member("ringo", "ringo");
            ring
        };
        let _ = {
            let mut it = ring.lookup("drummer").copied();
            assert_eq!(it.next(), Some("ringo"));
            assert_eq!(it.next(), None);
        };
        let _ = {
            let mut it = ring.lookup("octopus").copied();
            assert_eq!(it.next(), Some("ringo"));
            assert_eq!(it.next(), None);
        };
    }

    #[test]
    fn test_ten_members() {
        let ring = {
            let mut ring = HashRing::new();
            for i in 0..10 {
                ring.add_member(format!("{i}"), i);
            }
            ring
        };
        assert_ten_members(&ring);
    }

    #[test]
    fn test_ten_members_after_removal() {
        let ring = {
            let mut ring = HashRing::new();
            ring.add_member("13", 13);
            for i in 0..10 {
                ring.add_member(format!("{i}"), i);
            }
            ring.remove_member("13");
            ring
        };
        assert_ten_members(&ring);
    }

    fn assert_ten_members(ring: &HashRing<usize>) {
        let _ = {
            let mut it = ring.lookup("one").take(3).copied();
            assert_eq!(it.next(), Some(6));
            assert_eq!(it.next(), Some(5));
            assert_eq!(it.next(), Some(8));
            assert_eq!(it.next(), None);
        };
        let _ = {
            let mut it = ring.lookup("two").take(3).copied();
            assert_eq!(it.next(), Some(0));
            assert_eq!(it.next(), Some(7));
            assert_eq!(it.next(), Some(1));
            assert_eq!(it.next(), None);
        };
        let _ = {
            let mut it = ring.lookup("three").take(3).copied();
            assert_eq!(it.next(), Some(2));
            assert_eq!(it.next(), Some(8));
            assert_eq!(it.next(), Some(9));
            assert_eq!(it.next(), None);
        };
    }

    #[test]
    #[should_panic(expected = "member ringo already in map")]
    fn test_duplicate_member() {
        let mut ring = HashRing::new();
        ring.add_member("ringo", "1");
        ring.add_member("ringo", "2");
    }
}
