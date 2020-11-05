# odbutils - A collection of utility functions which may be useful for building databases in OCaml.

### OWal : A write ahead log functor

This takes some input module providing a data structure and some way to update it. Those updates are then serialised and written to disk lazily. If `datasync` is called it will flush all updates to disk and call `fdatasync` on the disk. Once that promise is resolved, the data should be persisted to disk and be recovered in case of a system crash etc.
