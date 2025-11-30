# Amimono Haze

Haze is a **highly experimental** Amimono-based implementation of a variety of
cloud computing primitives. They are similar to embedded databases, but for a
distributed application written as a modular monolith via Amimono. It currently
exists as a way to exercise Amimono's functionality and tooling, but making it
production-ready is a long-term goal.

It aspirationally consists of the following:

* `blob` &mdash; Content-addressed object storage.

* `crdt` &mdash; Simple CRDT storage.

* `dashboard` &mdash; An extendable dashboard mimicking a file tree.

* `wf` &mdash; Asynchronous workflow execution.