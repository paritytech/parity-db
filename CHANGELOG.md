# Changelog

The format is based on [Keep a Changelog].

[Keep a Changelog]: http://keepachangelog.com/en/1.0.0/

## [Unreleased]

## [v0.4.14] - 2024-10-03

- Multi tree column option [`#232`](https://github.com/paritytech/parity-db/pull/232)

## [v0.4.13] - 2024-01-05

- Lock memory map when reading from file [`#234`](https://github.com/paritytech/parity-db/pull/234)
- Disable read-ahead on Windows [`#235`](https://github.com/paritytech/parity-db/pull/235)

## [v0.4.12] - 2023-10-12

- CI for windows and macos. Also fixes access denied error on windows [`#222`](https://github.com/paritytech/parity-db/pull/222)
- Force alignment for all chunk buffers [`#225`](https://github.com/paritytech/parity-db/pull/225)

## [v0.4.11] - 2023-09-13

- Make `madvise_random` compatible with non-Unix OS [`#221`](https://github.com/paritytech/parity-db/pull/221)
- Explicit `funlock` [`#218`](https://github.com/paritytech/parity-db/pull/218)

## [v0.4.10] - 2023-07-21

- Use mmap IO for value tables [`#214`](https://github.com/paritytech/parity-db/pull/214)

## [v0.4.9] - 2023-07-03

- Call madvise for existing index files [`#211`](https://github.com/paritytech/parity-db/pull/211)

## [v0.4.8] - 2023-05-15

- Support for removing a column [`#210`](https://github.com/paritytech/parity-db/pull/210)

## [v0.4.7] - 2023-05-02

- Fix find_entry for zero partial key [`#206`](https://github.com/paritytech/parity-db/pull/206)
- Limit log cleanup queue [`#204`](https://github.com/paritytech/parity-db/pull/204)
- Ensure log deletion order [`#202`](https://github.com/paritytech/parity-db/pull/202)
- Improve DB validation tool [`#200`](https://github.com/paritytech/parity-db/pull/200)
- Representative query timing [`#196`](https://github.com/paritytech/parity-db/pull/196)
- Force value-only iteration in the public API [`#192`](https://github.com/paritytech/parity-db/pull/192)
- Fuzzer: model more carefully what is written to disk [`70c60f0`](https://github.com/paritytech/parity-db/commit/70c60f02047d96138653502a0a40577687a4ba0c)
- Fixes fuzzer: the database now enacts commits on drop properly [`a28a58d`](https://github.com/paritytech/parity-db/commit/a28a58dc21ab8654641168e484be70227ea7d5e7)

## [v0.4.6] - 2023-03-20

- Check read bound for btree nodes. [`#194`](https://github.com/paritytech/parity-db/pull/194)
- Fix find_entry_sse [`#193`](https://github.com/paritytech/parity-db/pull/193)
- Preserve logs on write error.  [`#188`](https://github.com/paritytech/parity-db/pull/188)
- Don't kill logs on write error [`1fe76f4`](https://github.com/paritytech/parity-db/commit/1fe76f4f51a947f1a1dddfc66e4c2165fa8260b3)

## [v0.4.4] - 2023-03-03

- Siphash key with salt for uniform columns [`#186`](https://github.com/paritytech/parity-db/pull/186)
- Uniform keys option for stress test. [`#185`](https://github.com/paritytech/parity-db/pull/185)
- Applies a Clippy suggestion to avoid a mem::transmute call [`#184`](https://github.com/paritytech/parity-db/pull/184)
- CI: Test aarch64 using Qemu [`#182`](https://github.com/paritytech/parity-db/pull/182)
- Benchmarks for find_entry [`#180`](https://github.com/paritytech/parity-db/pull/180)
- Reduce alloc in copy_to_overlay [`#178`](https://github.com/paritytech/parity-db/pull/178)
- No sse2 target [`#177`](https://github.com/paritytech/parity-db/pull/177)
- Use SSE2 SIMD in IndexTable::find_entry [`#176`](https://github.com/paritytech/parity-db/pull/176)
- IndexTable::find_entry: add assertion bound checking [`#175`](https://github.com/paritytech/parity-db/pull/175)
- Upgrades clap and env_logger [`#173`](https://github.com/paritytech/parity-db/pull/173)
- Renames "build" CI job to "test" [`#174`](https://github.com/paritytech/parity-db/pull/174)
- Makes Clippy happy [`#172`](https://github.com/paritytech/parity-db/pull/172)
- btree copy_to_overlay [`86ffde6`](https://github.com/paritytech/parity-db/commit/86ffde6a0750e0fcefdde2a2b96df783f47641af)
- fix fmt and clippy [`6e7ffb6`](https://github.com/paritytech/parity-db/commit/6e7ffb6df20d3a9dc97d8b1633a4e0da42ceb14a)
- fix: reduce allocations in copy_to_overlay [`43331db`](https://github.com/paritytech/parity-db/commit/43331db9838094fc0c3dfe1f878c4bba0e902466)

## [v0.4.2] - 2023-01-23

- Fix index corruption on reindex [`#170`](https://github.com/paritytech/parity-db/pull/170)
- implement FromStr for CompressionType [`#167`](https://github.com/paritytech/parity-db/pull/167)
- Update README.md [`#166`](https://github.com/paritytech/parity-db/pull/166)
- Loom: validates iteration and adds deletions to transaction test [`#165`](https://github.com/paritytech/parity-db/pull/165)
- Fixes CI workflow file syntax [`#164`](https://github.com/paritytech/parity-db/pull/164)
- Tests writes and iteration with Loom [`#163`](https://github.com/paritytech/parity-db/pull/163)
- Implements Entry::new_uninit where it's fairly safe [`#160`](https://github.com/paritytech/parity-db/pull/160)
- Avoid a race-condition leading to crash in Log::clean_logs [`#161`](https://github.com/paritytech/parity-db/pull/161)
- Removes TableKey::index (private dead code) [`#155`](https://github.com/paritytech/parity-db/pull/155)
- Removes "pub" modifier on methods with private input types [`#156`](https://github.com/paritytech/parity-db/pull/156)
- Properly reset BTree iteration state on boundaries [`#154`](https://github.com/paritytech/parity-db/pull/154)
- Log: Adds a read queue [`#151`](https://github.com/paritytech/parity-db/pull/151)
- Makes fuzzer support more recovery edge cases [`#149`](https://github.com/paritytech/parity-db/pull/149)
- Clean logs using the usual process even in case of replay or db failure [`#148`](https://github.com/paritytech/parity-db/pull/148)
- Removes some trivial casts [`#147`](https://github.com/paritytech/parity-db/pull/147)
- Makes fuzzer call explicitly I/O operations [`#143`](https://github.com/paritytech/parity-db/pull/143)
- Properly reset BTree iteration state on boundaries [`#152`](https://github.com/paritytech/parity-db/issues/152)
- Clean logs using the usual process even in case of replay or db failure [`#145`](https://github.com/paritytech/parity-db/issues/145)
- Fuzz iteration as part of operations [`097f5c1`](https://github.com/paritytech/parity-db/commit/097f5c18ef486abd0da4dfe3372a02b74830085b)
- Adds very basic concurrency test with loom [`3038b2a`](https://github.com/paritytech/parity-db/commit/3038b2a36544c4ab3c7b67662b0415573be10509)
- Fuzzer: Removes duplicated code related to layers [`a917267`](https://github.com/paritytech/parity-db/commit/a917267ef6dc5189c7bfb9e707fdeba218541094)

## [v0.4.2] - 2022-10-12

- Support for adding new columns in existing db [`#144`](https://github.com/paritytech/parity-db/pull/144)
- I/O simulation: always fail after the first failure [`#141`](https://github.com/paritytech/parity-db/pull/141)
- Add `add_column` and test [`764e1ee`](https://github.com/paritytech/parity-db/commit/764e1ee5f8d52c2d00aa3c79af1402d30c38e958)
- fix clippy [`53d3aca`](https://github.com/paritytech/parity-db/commit/53d3acac2c5a16bfdea05e7ac539ddb0e7acbca3)
- More explicit error message [`36ffe8e`](https://github.com/paritytech/parity-db/commit/36ffe8ecf471f4f62671357ab2b72bf9a8517828)

## [v0.4.1] - 2022-10-06

- Don't panic on bad size header [`#140`](https://github.com/paritytech/parity-db/pull/140)
- Removes skip_check_lock option [`#136`](https://github.com/paritytech/parity-db/pull/136)
- Fixes a possible crash during log validation [`#137`](https://github.com/paritytech/parity-db/issues/137)
- Removes skip_check_lock [`08038f8`](https://github.com/paritytech/parity-db/commit/08038f862c9618181b847d0234da7eb5b9fba2cf)
- Error message [`f293dfc`](https://github.com/paritytech/parity-db/commit/f293dfc6609b886fa97dd089147b1a8bc9c8cc4e)

## [0.4.0] - 2022-09-28
- Compression threshold moved to modifiable starting option. [#103](https://github.com/paritytech/parity-db/pull/103)
- Iterator behave like rust std btree when changing direction [#125](https://github.com/paritytech/parity-db/pull/125)
