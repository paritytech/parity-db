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
use crate::column::Salt;
use rand::Rng;

const CURRENT_VERSION: u32 = 3;
const LAST_SUPPORTED_VERSION: u32 = 2;

/// Database configuration.
#[derive(Clone, Debug)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
	/// Use referece counting for values.
	pub ref_counted: bool,
	/// Compression to use for this column.
	pub compression: crate::compress::CompressType,
	/// Minimal value size treshold to attempt compressing a value.
	pub compression_treshold: usize,
}

impl ColumnOptions {
	fn as_string(&self) -> String {
		format!("preimage: {}, uniform: {}, refc: {}, compression: {}, sizes: [{}]",
			self.preimage,
			self.uniform,
			self.ref_counted,
			self.compression as u8,
			self.sizes.iter().fold(String::new(), |mut r, s| {
				if !r.is_empty() {
					r.push_str(", ");
				}
				r.push_str(&s.to_string());
				r
			})
		)
	}

	fn from_string(encoded: &str) -> Option<(u8, ColumnOptions)> {
		let mut result = ColumnOptions::default();
		let split = encoded.find('=')?;
		let (col, encoded) = encoded.split_at(split);
		// "col = "
		let col: u8 = col[3..].parse().ok()?;
		let mut size_ix = 0;
		for pair in encoded[1..].split(',') {
			if size_ix > 0 {
				if size_ix == result.sizes.len() - 1 {
					result.sizes[size_ix] = pair[1..pair.len() - 1].parse().ok()?;
				} else {
					result.sizes[size_ix] = pair[1..].parse().ok()?;
				}
				size_ix += 1;
				continue;
			}
			let split = pair.find(':')?;
			match pair.split_at(split) {
				/*(a,b) => {
					result.preimage = true;
				},*/
				("preimage", ": true") => {
					result.preimage = true;
				},
				("preimage", ": false") => {
					result.preimage = false;
				},
				(" uniform", ": true") => {
					result.uniform = true;
				},
				(" uniform", ": false") => {
					result.uniform = false;
				},
				(" refc", ": true") => {
					result.ref_counted = true;
				},
				(" refc", ": false") => {
					result.ref_counted = false;
				},
				(" compression", comp) => {
					let compression: u8 = comp[2..].parse().ok()?;
					result.compression = compression.into();
				},
				(" sizes", sizes) => {
					result.sizes[size_ix] = sizes[3..].parse().ok()?;
					size_ix += 1;
				},
				_ => return None,
			}
		}
		Some((col, result))
	}
}

impl Default for ColumnOptions {
	fn default() -> ColumnOptions {
		ColumnOptions {
			preimage: false,
			uniform: false,
			ref_counted: false,
			compression: crate::compress::CompressType::NoCompression,
			compression_treshold: usize::max_value(),
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
			columns: (0..num_columns).map(|_| Default::default()).collect(),
		}
	}

	pub fn write_metadata(&self, path: &std::path::Path, salt: &Salt) -> Result<()> {
		self.write_metadata_old(path, Some(salt))
	}

	pub(crate) fn write_metadata_old(&self, path: &std::path::Path, salt: Option<&Salt>) -> Result<()> {
		let mut file = std::fs::File::create(path)?;
		writeln!(file, "version={}", CURRENT_VERSION)?;
		if let Some(salt) = salt {
			writeln!(file, "salt={}", hex::encode(salt))?;
		}
		for i in 0..self.columns.len() {
			writeln!(file, "col{}={}", i, self.columns[i].as_string())?;
		}
		Ok(())
	}

	pub fn read_metadata(path: &std::path::Path) -> Result<(Option<Salt>, Options)> {
		use std::io::BufRead;
		use std::str::FromStr;

		let mut path: std::path::PathBuf = path.into();
		let mut options = Options {
			path: path.clone(),
			columns: Vec::new(),
			sync: false,
			stats: true,
		};

		path.push("metadata");
		let file = std::io::BufReader::new(std::fs::File::open(path)?);
		let mut salt = None;
		for l in file.lines() {
			let l = l?;
			let mut vals = l.split("=");
			let k = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			let v = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			if k == "version" {
				let version = u32::from_str(v).map_err(|_| Error::Corruption("Bad version string".into()))?;
				if version < LAST_SUPPORTED_VERSION  {
					return Err(Error::InvalidConfiguration(format!(
						"Unsupported database version {}. Expected {}", version, CURRENT_VERSION)));
				}
			} else if k == "salt" {
					let salt_slice = hex::decode(v).map_err(|_| Error::Corruption("Bad salt string".into()))?;
					let mut s = Salt::default();
					s.copy_from_slice(&salt_slice);
					salt = Some(s);
			} else if k.starts_with("col") {
				if let Some((ix, col_option)) = ColumnOptions::from_string(k) {
					assert!(ix == options.columns.len() as u8);
					options.columns.push(col_option);
				} else {
					return Err(Error::InvalidConfiguration(format!(
						"Couldn't read column configuration: {:?}",
						k)));
				}
			}
		}
		Ok((salt, options))
	}


	pub fn load_and_validate_metadata(&self) -> Result<Option<Salt>> {
		use std::io::BufRead;
		use std::str::FromStr;

		let mut path = self.path.clone();
		path.push("metadata");
		if !path.exists() {
			let salt: Salt = rand::thread_rng().gen();
			self.write_metadata(&path, &salt)?;
			return Ok(Some(salt))
		}
		let file = std::io::BufReader::new(std::fs::File::open(path)?);
		let mut salt = None;
		for l in file.lines() {
			let l = l?;
			let mut vals = l.split("=");
			let k = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			let v = vals.next().ok_or(Error::Corruption("Bad metadata".into()))?;
			if k == "version" {
				let version = u32::from_str(v).map_err(|_| Error::Corruption("Bad version string".into()))?;
				if version < LAST_SUPPORTED_VERSION  {
					return Err(Error::InvalidConfiguration(format!(
						"Unsupported database version {}. Expected {}", version, CURRENT_VERSION)));
				}
			} else if k == "salt" {
					let salt_slice = hex::decode(v).map_err(|_| Error::Corruption("Bad salt string".into()))?;
					let mut s = Salt::default();
					s.copy_from_slice(&salt_slice);
					salt = Some(s);
			} else if k.starts_with("col") {
				let col_index = u8::from_str(&k[3..]).map_err(|_| Error::Corruption("Bad metadata column index".into()))?;
				if col_index as usize > self.columns.len() {
					return Err(Error::InvalidConfiguration(format!("Column config mismatch. Bad metadata column index: {}", col_index)));
				}
				let column_meta = self.columns[col_index as usize].as_string();
				if column_meta != v {
					return Err(Error::InvalidConfiguration(format!(
						"Column config mismatch for column {}. Expected \"{}\", got \"{}\"",
						col_index, v, column_meta)));
				}
			}
		}
		Ok(salt)
	}
}

#[test]
fn encode_decode_option() {
	let col = ColumnOptions::default();
	let encoded = format!("col{}={}", 3, col.as_string());
	let decoded = ColumnOptions::from_string(encoded.as_str()).unwrap();
	assert_eq!((3, col), decoded);
}
