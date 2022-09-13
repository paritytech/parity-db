// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

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
mod stats;
mod table;

pub use btree::BTreeIterator;
pub use compress::CompressionType;
pub use db::{check::CheckOptions, Db, Operation, Value};
pub use error::{Error, Result};
pub use migration::{clear_column, migrate};
pub use options::{ColumnOptions, Options};
pub use stats::{ColumnStatSummary, StatSummary};

pub const KEY_SIZE: usize = 32;
pub type Key = [u8; KEY_SIZE];
