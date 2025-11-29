//! Types with useful [`Crdt`] implementations.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::crdt::{Crdt, StoredCrdt};

/// Merge by picking the larger of two values.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Max<T>(pub T);

impl<T: Ord> Crdt for Max<T> {
    fn merge_from(&mut self, other: Self) {
        if let Ordering::Less = self.0.cmp(&other.0) {
            *self = other
        }
    }

    fn merge(self, other: Self) -> Self {
        Max(std::cmp::max(self.0, other.0))
    }
}

impl<T: Ord + Serialize + DeserializeOwned + 'static> StoredCrdt for Max<T> {}

/// Merge by picking the smaller of two values.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Min<T>(pub T);

impl<T: Ord> Crdt for Min<T> {
    fn merge_from(&mut self, other: Self) {
        if let Ordering::Greater = self.0.cmp(&other.0) {
            *self = other
        }
    }

    fn merge(self, other: Self) -> Self {
        Min(std::cmp::min(self.0, other.0))
    }
}

impl<T: Ord + Serialize + DeserializeOwned + 'static> StoredCrdt for Min<T> {}

/// Merge by picking the value with a larger version, or merging if they have
/// the same version.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct Version<V, T>(pub V, pub T);

impl<V: Ord, T: Crdt> Crdt for Version<V, T> {
    fn merge_from(&mut self, other: Self) {
        match self.0.cmp(&other.0) {
            Ordering::Less => *self = other,
            Ordering::Equal => self.1.merge_from(other.1),
            Ordering::Greater => (),
        }
    }

    fn merge(self, other: Self) -> Self {
        match self.0.cmp(&other.0) {
            Ordering::Less => other,
            Ordering::Equal => Version(self.0, self.1.merge(other.1)),
            Ordering::Greater => self,
        }
    }
}

impl<V: Ord + Serialize + DeserializeOwned + 'static, T: StoredCrdt> StoredCrdt for Version<V, T> {}
