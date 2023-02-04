// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

#![cfg_attr(nightly, feature(portable_simd))]
#![cfg_attr(nightly, feature(test))]

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
mod options;
mod parking_lot;
mod stats;
mod table;

pub use btree::BTreeIterator;
pub use column::ColId;
pub use compress::CompressionType;
pub use db::{check::CheckOptions, Db, Operation, Value};
#[cfg(feature = "instrumentation")]
pub use error::set_number_of_allowed_io_operations;
pub use error::{Error, Result};
pub use migration::{clear_column, migrate};
pub use options::{ColumnOptions, Options};
pub use stats::{ColumnStatSummary, StatSummary};

pub const KEY_SIZE: usize = 32;
pub type Key = [u8; KEY_SIZE];
