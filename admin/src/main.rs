// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Command line admin client for parity-db.
//! Experimental, some functionality may not
//! guarantee db durability.

#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
	fdlimit::raise_fd_limit();

	parity_db_admin::run().unwrap();
}
