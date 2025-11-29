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

- Outages are acceptable during deployments or reconfigurations.

This last assumption is significant, but CRDTs work best when clients maintain
their own copy of the state that is periodically synchronized in full with the
backing store, rather than relying on the backing store to act as a
permanently-available source of truth. By allowing the backing store to fail,
and perhaps even lose data (e.g. when restoring from a backup), the architecture
and storage requirements become much more tractable, at the expense of requiring
some cooperation from data users.

## How it works

The key space is divided with a consistent hashing ring. Each node is
represented in the hash ring with multiple virtual nodes. The routing layer
uses the ring to map keys to replicas and forwards requests appropriately.

A controller component handles repartitioning. To introduce a new node, it takes
the following steps for each new virtual node:

- Inform the existing virtual node that it should begin replicating a portion of
the keyspace to the new node. The routing layer still treats the old node as the
primary for these keys, but the old node will gradually move its data to the new
node, forwarding reads and writes as appropriate. The old node will eventually
reach a state where all reads and writes are simply proxied to the new node.

- Gradually inform the routing layer about the new node. The routing layer will
begin to send reads and writes directly to the new node.