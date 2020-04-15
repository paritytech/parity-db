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

use std::io::Write;
use crate::error::{Error, Result};

const CURRENT_VERSION: u32 = 1;

/// Database configuration.
pub struct Options {
	/// Database path.
	pub path: std::path::PathBuf,
	/// Column settings
	pub columns: Vec<ColumnOptions>,
	/// fsync WAL to disk before commiting any changes. Provides extra consistency
	/// guarantees. Off by default.
	pub sync: bool,
	/// Collect database statistics. May have effect on performance.
	pub stats: bool,
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

impl ColumnOptions {
	fn as_string(&self) -> String {
		format!("preimage: {}, uniform: {}, sizes: [{}]",
			self.preimage,
			self.uniform,
			self.sizes.iter().fold(String::new(), |mut r, s| {
				if !r.is_empty() {
					r.push_str(", ");
				}
				r.push_str(&s.to_string()); r
			})
		)
	}
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
			stats: true,
			columns: (0 .. num_columns).map(|_| Default::default()).collect(),
		}
	}

	pub fn write_metadata(&self, path: &std::path::Path) -> Result<()> {
		let mut file = std::fs::File::create(path)?;
		writeln!(file, "version={}", CURRENT_VERSION)?;
		for i in 0 .. self.columns.len() {
			writeln!(file, "col{}={}", i, self.columns[i].as_string())?;
		}
		Ok(())
	}

	pub fn validate_metadata(&self) -> Result<()> {
		use std::io::BufRead;
		use std::str::FromStr;

		let mut path = self.path.clone();
		path.push("metadata");
		if !path.exists() {
			return self.write_metadata(&path)
		}
		let file = std::io::BufReader::new(std::fs::File::open(path)?);
		for l in file.lines() {
			let l = l?;
			let mut vals = l.split("=");
			let k = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			let v = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			if k == "version" {
				let version = u32::from_str(v).map_err(|_| Error::Corruption("Bad version string".into()))?;
				if version != CURRENT_VERSION {
					return Err(Error::Corruption(format!(
						"Unsupported database version {}. Expected {}", version, CURRENT_VERSION)));
				}
			} else if k.starts_with("col") {
				let col_index = u8::from_str(&k[3..]).map_err(|_| Error::Corruption("Bad metadata column index".into()))?;
				if col_index as usize > self.columns.len() {
					return Err(Error::InvalidInput(format!("Column config mismatch. Bad metadata column index: {}", col_index)));
				}
				let column_meta = self.columns[col_index as usize].as_string();
				if column_meta != v {
					return Err(Error::InvalidInput(format!(
						"Column config mismatch for column {}. Expected \"{}\", got \"{}\"",
						col_index, v, column_meta)));
				}
			}
		}
		Ok(())
	}
}

