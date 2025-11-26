A basic storage system built on CRDTs.

# What is a CRDT?

A CRDT is a data type that takes advantage of **idempotency**,
**associativity**, and **commutativity**, i.e. it is a type with a *merge*(*a*,
*b*) operation that has the following properties:

* Idempotency: *merge*(*a*, *a*) = *a*
* Associativity: *merge*(*merge*(*a*, *b*), *c*) = *merge*(*a*, merge(*b*, *c*))
* Commutativity: *merge*(*a*, *b*) = *merge*(*b*, *a*)

Those with some abstract algebra background will recognize this as a
semilattice. Types with those properties are implemented with the
[`Crdt`][Crdt] trait.

## Uses

CRDTs are useful for decentralizing data storage, because conflict resolution is
built-in: writes to different replicas can always be automatically resolved
using the *merge*() operation. For an analogy, think of every copy of the data
as being a separate forks of a git repository, where forks can always be merged
automatically and predictably.

The `crdt` module provides a place to store CRDTs. For example, consider an
external user with a phone and a laptop. While a CRDT provides a good foundation
for resolving offline edits that occur on these devices, they still need a way
to send their replicas to each other for editing.

## Example: `HashSet`

A simple example of a CRDT is sets under union, which in Rust we can implement
in terms of `HashSet`:

```rust,ignore
impl<T: Eq + Hash> Crdt for HashSet<T> {
    fn merge_from(&mut self, other: Self) {
        for item in other.into_iter() {
            self.insert(item);
        }
    }
}
```

In fact, this is provided as a [`Crdt`] implementation.

# Design assumptions

In addition to the overall design assumptions of Haze, described in the
crate-level documentation, the storage system provided by this module makes the
following assumptions:

- Values are only accessed one at a time by their key, i.e. there is no need for
list, scan, or query operations.

- Values are relatively small, not more than a few kilobytes.

- Access patterns are roughly evenly distributed across the key space.

# How it works

Replicas are organized into a consistent hashing ring, where each point in the
key space is assigned to 2 replicas, one primary and one secondary. Reads and
writes always go to the primary replica, and values are copied to the secondary
replica.

## Note on single-replica systems

A single-replica system functions as a simple database.