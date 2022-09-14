# Parity DB fuzzing

It relies on [cargo fuzz](https://github.com/rust-fuzz/cargo-fuzz).
There is [a detailed tutorial available](https://rust-fuzz.github.io/book/cargo-fuzz.html).

Two fuzzers are currently available:

- `simple_model`: checks that the database without reference counting behaves like an in-memory collection. It covers both hash-map and b-tree.
- `recounted_model`: checks that the database without reference counting behaves like an in-memory collection. It covers both hash-map and b-tree.

Both fuzzers currently only checks a sequence of transactions and restarts.

To setup and run the simple model fuzzer run the root directory of Parity DB:
```shell
cargo install cargo-fuzz
cargo +nightly fuzz run simple_model
```
