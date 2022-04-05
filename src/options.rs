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

use crate::{
	column::Salt,
	compress::CompressionType,
	error::{Error, Result},
};
use rand::Rng;
use std::{collections::HashMap, io::Write, path::Path};

pub const CURRENT_VERSION: u32 = 7;
// TODO on last supported 5, remove MULTIHEAD_V4 and MULTIPART_V4
const LAST_SUPPORTED_VERSION: u32 = 4;

/// Database configuration.
#[derive(Clone, Debug)]
pub struct Options {
	/// Database path.
	pub path: std::path::PathBuf,
	/// Column settings
	pub columns: Vec<ColumnOptions>,
	/// fsync WAL to disk before committing any changes. Provides extra consistency
	/// guarantees. On by default.
	pub sync_wal: bool,
	/// fsync/msync data to disk before removing logs. Provides crash resistance guarantee.
	/// On by default.
	pub sync_data: bool,
	/// Collect database statistics. May have effect on performance.
	pub stats: bool,
	/// Override salt value. If `None` is specified salt is loaded from metadata
	/// or randomly generated when creating a new database.
	pub salt: Option<Salt>,
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
	/// Use reference counting for values.
	pub ref_counted: bool,
	/// Compression to use for this column.
	pub compression: CompressionType,
	/// Minimal value size threshold to attempt compressing a value.
	pub compression_threshold: u32,
	/// Column is using a btree indexing.
	pub btree_index: bool,
}

/// Database metadata.
#[derive(Clone, Debug)]
pub struct Metadata {
	/// Salt value.
	pub salt: Salt,
	/// Database version.
	pub version: u32,
	/// Column metadata.
	pub columns: Vec<ColumnOptions>,
}

impl ColumnOptions {
	fn as_string(&self) -> String {
		format!(
			"preimage: {}, uniform: {}, refc: {}, compression: {}, ordered: {}",
			self.preimage, self.uniform, self.ref_counted, self.compression as u8, self.btree_index,
		)
	}

	pub fn is_valid(&self) -> bool {
		if self.btree_index && self.preimage {
			log::error!(target: "parity-db", "Using `preimage` option on an ordered column is not supported");
			return false
		}
		true
	}

	fn from_string(s: &str) -> Option<Self> {
		let mut split = s.split("sizes: ");
		let vals = split.next()?;

		let vals: HashMap<&str, &str> = vals
			.split(", ")
			.filter_map(|s| {
				let mut pair = s.split(": ");
				Some((pair.next()?, pair.next()?))
			})
			.collect();

		let preimage = vals.get("preimage")?.parse().ok()?;
		let uniform = vals.get("uniform")?.parse().ok()?;
		let ref_counted = vals.get("refc")?.parse().ok()?;
		let compression: u8 = vals.get("compression").and_then(|c| c.parse().ok()).unwrap_or(0);
		let btree_index = vals.get("ordered").and_then(|c| c.parse().ok()).unwrap_or(false);

		Some(ColumnOptions {
			preimage,
			uniform,
			ref_counted,
			compression: compression.into(),
			compression_threshold: ColumnOptions::default().compression_threshold,
			btree_index,
		})
	}
}

impl Default for ColumnOptions {
	fn default() -> ColumnOptions {
		ColumnOptions {
			preimage: false,
			uniform: false,
			ref_counted: false,
			compression: CompressionType::NoCompression,
			compression_threshold: 4096,
			btree_index: false,
		}
	}
}

impl Options {
	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Options {
		Options {
			path: path.into(),
			sync_wal: true,
			sync_data: true,
			stats: true,
			salt: None,
			columns: (0..num_columns).map(|_| Default::default()).collect(),
		}
	}

	// TODO on next major version remove in favor of write_metadata_with_version
	pub fn write_metadata(&self, path: &std::path::Path, salt: &Salt) -> Result<()> {
		let mut path = path.to_path_buf();
		path.push("metadata");
		self.write_metadata_file(&path, salt)
	}

	// TODO on next major version remove in favor of write_metadata_with_version
	pub fn write_metadata_file(&self, path: &std::path::Path, salt: &Salt) -> Result<()> {
		let mut file = std::fs::File::create(path)?;
		writeln!(file, "version={}", CURRENT_VERSION)?;
		writeln!(file, "salt={}", hex::encode(salt))?;
		for i in 0..self.columns.len() {
			writeln!(file, "col{}={}", i, self.columns[i].as_string())?;
		}
		Ok(())
	}

	pub fn write_metadata_with_version(
		&self,
		path: &std::path::Path,
		salt: &Salt,
		version: Option<u32>,
	) -> Result<()> {
		let mut path = path.to_path_buf();
		path.push("metadata");
		self.write_metadata_file_with_version(&path, salt, version)
	}

	pub fn write_metadata_file_with_version(
		&self,
		path: &std::path::Path,
		salt: &Salt,
		version: Option<u32>,
	) -> Result<()> {
		let version = version.unwrap_or(CURRENT_VERSION);
		let mut file = std::fs::File::create(path)?;
		writeln!(file, "version={}", version)?;
		writeln!(file, "salt={}", hex::encode(salt))?;
		for i in 0..self.columns.len() {
			writeln!(file, "col{}={}", i, self.columns[i].as_string())?;
		}
		Ok(())
	}

	pub fn load_and_validate_metadata(&self, create: bool) -> Result<Metadata> {
		let meta = Self::load_metadata(&self.path)?;

		if let Some(meta) = meta {
			if meta.columns.len() != self.columns.len() {
				return Err(Error::InvalidConfiguration("Column config mismatch".into()))
			}

			for c in 0..meta.columns.len() {
				if meta.columns[c] != self.columns[c] {
					return Err(Error::InvalidConfiguration(format!(
						"Column config mismatch for column {}. Expected \"{}\", got \"{}\"",
						c,
						self.columns[c].as_string(),
						meta.columns[c].as_string()
					)))
				}
			}
			Ok(meta)
		} else if create {
			let s: Salt = self.salt.unwrap_or_else(|| rand::thread_rng().gen());
			self.write_metadata(&self.path, &s)?;
			Ok(Metadata { version: CURRENT_VERSION, columns: self.columns.clone(), salt: s })
		} else {
			Err(Error::DatabaseNotFound)
		}
	}

	pub fn load_metadata(path: &Path) -> Result<Option<Metadata>> {
		let mut path = path.to_path_buf();
		path.push("metadata");
		Self::load_metadata_file(&path)
	}

	pub fn load_metadata_file(path: &Path) -> Result<Option<Metadata>> {
		use std::{io::BufRead, str::FromStr};

		if !path.exists() {
			return Ok(None)
		}
		let file = std::io::BufReader::new(std::fs::File::open(path)?);
		let mut salt = None;
		let mut columns = Vec::new();
		let mut version = 0;
		for l in file.lines() {
			let l = l?;
			let mut vals = l.split('=');
			let k = vals.next().ok_or_else(|| Error::Corruption("Bad metadata".into()))?;
			let v = vals.next().ok_or_else(|| Error::Corruption("Bad metadata".into()))?;
			if k == "version" {
				version =
					u32::from_str(v).map_err(|_| Error::Corruption("Bad version string".into()))?;
			} else if k == "salt" {
				let salt_slice =
					hex::decode(v).map_err(|_| Error::Corruption("Bad salt string".into()))?;
				let mut s = Salt::default();
				s.copy_from_slice(&salt_slice);
				salt = Some(s);
			} else if k.starts_with("col") {
				let col = ColumnOptions::from_string(v)
					.ok_or_else(|| Error::Corruption("Bad column metadata".into()))?;
				columns.push(col);
			}
		}
		if version < LAST_SUPPORTED_VERSION {
			return Err(Error::InvalidConfiguration(format!(
				"Unsupported database version {}. Expected {}",
				version, CURRENT_VERSION
			)))
		}
		let salt = salt.ok_or_else(|| Error::InvalidConfiguration("Missing salt value".into()))?;
		Ok(Some(Metadata { version, columns, salt }))
	}

	pub fn is_valid(&self) -> bool {
		for option in self.columns.iter() {
			if !option.is_valid() {
				return false
			}
		}
		true
	}
}

impl Metadata {
	pub fn columns_to_migrate(&self) -> std::collections::BTreeSet<u8> {
		std::collections::BTreeSet::new()
	}
}
