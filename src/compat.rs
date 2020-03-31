// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

// kvdb-compatibility layer

use std::{io, io::ErrorKind};

use kvdb::{DBTransaction, DBValue, KeyValueDB};
use crate::db;

/// Database configuration
#[derive(Clone)]
pub struct DatabaseConfig {
	pub columns: u32,
}

pub type KeyValuePair = (Box<[u8]>, Box<[u8]>);

impl DatabaseConfig {
	/// Create new `DatabaseConfig` with default parameters and specified set of columns.
	/// Note that cache sizes must be explicitly set.
	///
	/// # Safety
	///
	/// The number of `columns` must not be zero.
	pub fn with_columns(columns: u32) -> Self {
		assert!(columns > 0, "the number of columns must not be zero");
		Self { columns }
	}
}

/// Key-Value database.
pub struct Database {
	db: db::Db,
}

fn to_io_err<E: std::fmt::Debug>(e: E) -> io::Error {
	io::Error::new(ErrorKind::Other, format!("{:?}", e))
}

impl parity_util_mem::MallocSizeOf for Database {
	fn size_of(&self, _ops: &mut parity_util_mem::MallocSizeOfOps) -> usize {
		0
	}
}


impl Database {
	/// Open database file. Creates if it does not exist.
	///
	/// # Safety
	///
	/// The number of `config.columns` must not be zero.
	pub fn open(config: &DatabaseConfig, path: &str) -> io::Result<Database> {
		assert!(config.columns > 0, "the number of columns must not be zero");

		let path: std::path::PathBuf = path.into();
		let db = db::Db::open(path.as_path(), config.columns as u8).map_err(to_io_err)?;
		Ok(Database {
			db,
		})
	}

	/// Helper to create new transaction for this database.
	pub fn transaction(&self) -> DBTransaction {
		DBTransaction::new()
	}

	/// Commit transaction to database.
	pub fn write(&self, tx: DBTransaction) -> io::Result<()> {
		self.db.commit_tx(tx).map_err(to_io_err)?;
		Ok(())
	}

	/// Get value by key.
	pub fn get(&self, col: u32, key: &[u8]) -> io::Result<Option<DBValue>> {
		self.db.get(col as u8, key).map(|r| r.map(|v| DBValue::from(v))).map_err(to_io_err)
	}

	/// Get value by partial key. Prefix size should match configured prefix size. Only searches flushed values.
	pub fn get_by_prefix(&self, _col: u32, _prefix: &[u8]) -> Option<Box<[u8]>> {
		unimplemented!();
	}

	pub fn num_columns(&self) -> u32 {
		self.db.num_columns() as u32
	}
}

impl KeyValueDB for Database {
	fn get(&self, col: u32, key: &[u8]) -> io::Result<Option<DBValue>> {
		Database::get(self, col, key)
	}

	fn get_by_prefix(&self, col: u32, prefix: &[u8]) -> Option<Box<[u8]>> {
		Database::get_by_prefix(self, col, prefix)
	}

	fn write_buffered(&self, _transaction: DBTransaction) {
		unimplemented!()
	}

	fn write(&self, transaction: DBTransaction) -> io::Result<()> {
		Database::write(self, transaction)
	}

	fn flush(&self) -> io::Result<()> {
		Ok(())
	}

	fn iter<'a>(&'a self, _col: u32) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
		unimplemented!();
	}

	fn iter_from_prefix<'a>(&'a self, _col: u32, _prefix: &'a [u8]) -> Box<dyn Iterator<Item = KeyValuePair> + 'a> {
		unimplemented!();
	}

	fn restore(&self, _new_db: &str) -> io::Result<()> {
		unimplemented!()
	}

	fn io_stats(&self, _kind: kvdb::IoStatsKind) -> kvdb::IoStats {
		kvdb::IoStats::empty()
	}
}


