#![doc = include_str!("README.md")]

use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    sync::{LazyLock, RwLock},
};

use amimono::config::{AppBuilder, JobBuilder};

pub mod crdt;

pub(crate) mod client;
pub(crate) mod controller;
pub(crate) mod router;
pub(crate) mod storage;
pub(crate) mod types;

pub use client::CrdtClient;
use serde::{Serialize, de::DeserializeOwned};

/// The main CRDT trait.
///
/// See the [module-level documentation][crate::crdt] for more details.
pub trait Crdt: Sized {
    /// Take the other value and merge it into this one. In order to maintain
    /// the CRDT invariants, this operation must be commutative, associative,
    /// and idempotent. See the [module-level documentation][crate::crdt] for
    /// more details.
    fn merge_from(&mut self, other: Self);

    /// Merge the two values into a new one. This operation must have the same
    /// semantics as [`merge_from`][Self::merge_from].
    ///
    /// The default implementation uses [`merge_from`][Self::merge_from], but
    /// some instances may be able to provide a more efficient implementation.
    fn merge(mut self, other: Self) -> Self {
        self.merge_from(other);
        self
    }
}

/// A trait for CRDTs that can be stored.
pub trait StoredCrdt: Crdt + Serialize + DeserializeOwned + 'static {
    /// Bind this type to a scope.
    ///
    /// This is a provided method that must be called before application startup
    /// for all `StoredCrdt` instances that will be used.
    fn bind(scope: &str) {
        bind_scope::<Self>(scope);
    }
}

impl Crdt for () {
    fn merge_from(&mut self, _other: Self) {}
    fn merge(self, _other: Self) {}
}

impl StoredCrdt for () {}

// TODO: tuple impl macros...

/// Merge two pairs by merging the left and right values.
impl<T0, T1> Crdt for (T0, T1)
where
    T0: Crdt,
    T1: Crdt,
{
    fn merge_from(&mut self, other: Self) {
        self.0.merge_from(other.0);
        self.1.merge_from(other.1);
    }
    fn merge(self, other: Self) -> Self {
        (self.0.merge(other.0), self.1.merge(other.1))
    }
}

impl<T0, T1> StoredCrdt for (T0, T1)
where
    T0: StoredCrdt,
    T1: StoredCrdt,
{
}

/// Merge two vectors by merging values at the same index. The vector will be
/// extended to the length of the longest input. This is a generalized version
/// of the behavior for tuples.
impl<T> Crdt for Vec<T>
where
    T: Crdt,
{
    fn merge_from(&mut self, mut other: Self) {
        if other.len() > self.len() {
            self.append(&mut other.split_off(self.len()));
        }
        for (i, that) in other.into_iter().enumerate() {
            self[i].merge_from(that);
        }
    }
    fn merge(mut self, mut other: Self) -> Self {
        if self.len() > other.len() {
            for (i, that) in other.into_iter().enumerate() {
                self[i].merge_from(that);
            }
            self
        } else {
            for (i, this) in self.into_iter().enumerate() {
                other[i].merge_from(this);
            }
            other
        }
    }
}

impl<T> StoredCrdt for Vec<T> where T: StoredCrdt {}

/// Merge two sets by computing their union.
impl<T> Crdt for HashSet<T>
where
    T: Eq + Hash,
{
    fn merge_from(&mut self, other: Self) {
        for item in other.into_iter() {
            self.insert(item);
        }
    }
}

impl<T> StoredCrdt for HashSet<T> where T: Eq + Hash + Serialize + DeserializeOwned + 'static {}

/// Merge two hash maps by combining their keys and merging values for keys that
/// appear in both maps.
impl<K: Eq + Hash, T: Crdt> Crdt for HashMap<K, T> {
    fn merge_from(&mut self, other: Self) {
        for (key, that) in other.into_iter() {
            if let Some(this) = self.get_mut(&key) {
                this.merge_from(that);
            } else {
                self.insert(key, that);
            }
        }
    }
}

impl<K, T> StoredCrdt for HashMap<K, T>
where
    K: Eq + Hash + Serialize + DeserializeOwned + 'static,
    T: StoredCrdt,
{
}

pub(crate) fn install_controller(job: &mut JobBuilder, prefix: &str) {
    job.add_component(controller::component(prefix));
}

pub(crate) fn install(app: &mut AppBuilder, prefix: &str) {
    app.add_job(
        JobBuilder::new()
            .with_label(format!("{prefix}-crdt"))
            .add_component(router::component(prefix))
            .add_component(storage::component(prefix)),
    );
}

trait StoredCrdtBinding: Send + Sync {
    fn inner(&self) -> TypeId;

    fn merge(&self, a: &[u8], b: &[u8]) -> Result<Vec<u8>, &'static str>;
}

struct StoredCrdtBindingImpl<T>(PhantomData<fn() -> T>);

impl<T: StoredCrdt> StoredCrdtBinding for StoredCrdtBindingImpl<T> {
    fn inner(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn merge(&self, a: &[u8], b: &[u8]) -> Result<Vec<u8>, &'static str> {
        let a_parsed: T = serde_json::from_slice(a).map_err(|_| "parse failed")?;
        let b_parsed: T = serde_json::from_slice(b).map_err(|_| "parse failed")?;
        let c = a_parsed.merge(b_parsed);
        serde_json::to_vec(&c).map_err(|_| "serialize failed")
    }
}

static SCOPES: LazyLock<RwLock<HashMap<String, Box<dyn StoredCrdtBinding>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn bind_scope<T: StoredCrdt>(scope: &str) {
    let binding = StoredCrdtBindingImpl::<T>(PhantomData);
    SCOPES
        .write()
        .expect("failed to get SCOPES lock")
        .insert(scope.to_owned(), Box::new(binding));
}

pub(crate) fn check_scope<T: StoredCrdt>(scope: &str) -> bool {
    let ty = SCOPES
        .read()
        .expect("failed to get SCOPES lock")
        .get(scope)
        .expect("scope not found")
        .inner();
    ty == TypeId::of::<T>()
}

pub(crate) fn merge_in_scope(scope: &str, a: &[u8], b: &[u8]) -> Result<Vec<u8>, &'static str> {
    SCOPES
        .read()
        .expect("failed to get SCOPES lock")
        .get(scope)
        .ok_or("scope not found")?
        .merge(a, b)
}
