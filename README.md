Rublo
=====

Very simple `tokio`-based asynchronous bloom filter server. [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a
probabilistic data structure which is used to test the membership of elements
in a large set, trading precision for space-efficiency and performance.

This is a simple implementation using a non-cryptographic hashing function
Murmur3 to generate the digests to set and check the presence of elements in
the each filter. A tokio based TCP server exposes the following text protocol:

- `create filter-name [capacity false-positive-probability]`
- `set filter-name key`
- `check filter-name key`
- `info filter-name`
- `clear filter-name`

Each command can be executed from any TCP client such as `netcat` or `telnet`.
Each filter is periodically dumped to disk for disaster recovery.
