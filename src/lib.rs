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

mod btree;
mod column;
mod compress;
mod db;
mod display;
mod error;
mod file;
mod index;
mod log;
mod migration;
mod options;
mod stats;
mod table;

pub use btree::BTreeIterator;
pub use compress::CompressionType;
pub use db::{check::CheckOptions, Db, Value};
pub use error::{Error, Result};
pub use migration::migrate;
pub use options::{ColumnOptions, Options};

#[derive(Default)]
pub struct IdentityKeyHash(u64);
type IdentityBuildHasher = std::hash::BuildHasherDefault<IdentityKeyHash>;

impl std::hash::Hasher for IdentityKeyHash {
	fn write(&mut self, bytes: &[u8]) {
		self.0 = u64::from_le_bytes((&bytes[0..8]).try_into().unwrap())
	}
	fn write_u8(&mut self, _: u8) {
		unreachable!()
	}
	fn write_u16(&mut self, _: u16) {
		unreachable!()
	}
	fn write_u32(&mut self, _: u32) {
		unreachable!()
	}
	fn write_u64(&mut self, _: u64) {
		unreachable!()
	}
	fn write_usize(&mut self, _: usize) {}
	fn write_i8(&mut self, _: i8) {
		unreachable!()
	}
	fn write_i16(&mut self, _: i16) {
		unreachable!()
	}
	fn write_i32(&mut self, _: i32) {
		unreachable!()
	}
	fn write_i64(&mut self, _: i64) {
		unreachable!()
	}
	fn write_isize(&mut self, _: isize) {
		unreachable!()
	}
	fn finish(&self) -> u64 {
		self.0
	}
}

pub const KEY_SIZE: usize = 32;
pub type Key = [u8; KEY_SIZE];

#[macro_export]
macro_rules! const_assert {
	(let $e:expr; ) => (
		const _: [(); { const ASSERT: bool = $e; ASSERT} as usize -1] = [];
	);
	(let $e:expr; $e1:expr $(, $ee:expr)*) => (
		const_assert!(let ($e) && ($e1); $($ee),*);
	);
	($e:expr $(, $ee:expr)*) => (
		const_assert!(let true && ($e); $($ee),*);
	);
}
