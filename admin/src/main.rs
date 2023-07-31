// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Command line admin client for parity-db.
//! Experimental, some functionality may not
//! guarantee db durability.

#[cfg_attr(not(target_env = "msvc"), global_allocator)]
#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
	fdlimit::raise_fd_limit();

	parity_db_admin::run().unwrap();
}
