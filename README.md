# Please

Crate for identifying and expiring long-running database activities. The core
primitive provided by the crate is a PleaseHandle. These handles represent
long-running operations, and can be used as the basis for implementing
exclusive locking behaviour when a lock may be held for too long for
transaction-level locking to be acceptable.

[Documentation](https://docs.rs/please)
