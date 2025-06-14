// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

#![cfg_attr(feature = "bench", feature(test))]

mod btree;
mod column;
mod compress;
mod db;
mod display;
mod error;
mod file;
mod hash;
mod index;
mod log;
mod migration;
mod multitree;
mod options;
mod parking_lot;
mod ref_count;
mod stats;
mod table;

pub use btree::BTreeIterator;
pub use column::{ColId, ValueIterState};
pub use compress::CompressionType;
pub use db::{check::CheckOptions, Db, Operation, TreeReader, Value};
#[cfg(feature = "instrumentation")]
pub use error::set_number_of_allowed_io_operations;
pub use error::{Error, Result};
pub use migration::{clear_column, migrate};
pub use multitree::{Children, NewNode, NodeAddress, NodeRef};
pub use options::{ColumnOptions, Options};
pub use stats::{ColumnStatSummary, StatSummary};

pub const KEY_SIZE: usize = 32;
pub type Key = [u8; KEY_SIZE];

#[cfg(not(any(
	target_arch = "x86_64",
	target_arch = "aarch64",
	target_arch = "loongarch64",
	target_arch = "riscv64"
)))]
compile_error!("parity-db only supports x86_64, aarch64, riscv64 and loongarch64 (unofficially)");

#[cfg(not(target_endian = "little"))]
compile_error!("parity-db only supports little-endian platforms");
