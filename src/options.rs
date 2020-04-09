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

/// Database configuration.
pub struct Options {
	/// Database path.
	pub path: std::path::PathBuf,
	/// Column settings
	pub columns: Vec<ColumnOptions>,
	/// fsync WAL to disk before commiting any changes. Provides extra consistency
	/// guarantees. Off by default.
	pub sync: bool,
}

pub struct ColumnOptions {
	/// Indicates that the column value is the preimage of the key.
	/// This implies that a given value always has the same key.
	/// Enables some optimizations.
	pub preimage: bool,
	/// Indicates that the keys are at least 32 bytes and
	/// the first 32 bytes have uniform distribution.
	/// Allows for skipping additional key hashing.
	pub uniform: bool,
	/// Value size tiers.
	pub sizes: [u16; 15],
}

impl Default for ColumnOptions {
	fn default() -> ColumnOptions {
		ColumnOptions {
			preimage: false,
			uniform: false,
			sizes: [96, 128, 192, 256, 320, 512, 768, 1024, 1536, 2048, 3072, 4096, 8192, 16384, 32768],
		}
	}
}

impl Options {
	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Options {
		Options {
			path: path.into(),
			sync: false,
			columns: (0 .. num_columns).map(|_| Default::default()).collect(),
		}
	}
}
