// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::{ColId, Salt},
	compress::CompressionType,
	error::{try_io, Error, Result},
};
use rand::Rng;
use std::{collections::HashMap, path::Path};

pub const CURRENT_VERSION: u32 = 8;
// TODO on last supported 5, remove MULTIHEAD_V4 and MULTIPART_V4
// TODO on last supported 8, remove XOR with salt in column::hash
const LAST_SUPPORTED_VERSION: u32 = 4;

pub const DEFAULT_COMPRESSION_THRESHOLD: u32 = 4096;

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
	/// Minimal value size threshold to attempt compressing a value per column.
	///
	/// Optional. A sensible default is used if nothing is set for a given column.
	pub compression_threshold: HashMap<ColId, u32>,
	#[cfg(any(test, feature = "instrumentation"))]
	/// Always starts background threads.
	pub with_background_thread: bool,
	#[cfg(any(test, feature = "instrumentation"))]
	/// Always flushes data from the log to the on-disk data structures.
	pub always_flush: bool,
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
	///
	/// Reference counting do not enforce immediate removal
	/// and user should not check for missing value.
	pub ref_counted: bool,
	/// Compression to use for this column.
	pub compression: CompressionType,
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
		if self.ref_counted && !self.preimage {
			log::error!(target: "parity-db", "Using `ref_counted` option without `preimage` enabled is not supported");
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
			btree_index: false,
		}
	}
}

impl Options {
	pub fn with_columns(path: &Path, num_columns: u8) -> Options {
		Options {
			path: path.into(),
			sync_wal: true,
			sync_data: true,
			stats: true,
			salt: None,
			columns: (0..num_columns).map(|_| Default::default()).collect(),
			compression_threshold: HashMap::new(),
			#[cfg(any(test, feature = "instrumentation"))]
			with_background_thread: true,
			#[cfg(any(test, feature = "instrumentation"))]
			always_flush: false,
		}
	}

	// TODO on next major version remove in favor of write_metadata_with_version
	pub fn write_metadata(&self, path: &Path, salt: &Salt) -> Result<()> {
		self.write_metadata_with_version(path, salt, None)
	}

	// TODO on next major version remove in favor of write_metadata_with_version
	pub fn write_metadata_file(&self, path: &Path, salt: &Salt) -> Result<()> {
		self.write_metadata_file_with_version(path, salt, None)
	}

	pub fn write_metadata_with_version(
		&self,
		path: &Path,
		salt: &Salt,
		version: Option<u32>,
	) -> Result<()> {
		let mut path = path.to_path_buf();
		path.push("metadata");
		self.write_metadata_file_with_version(&path, salt, version)
	}

	pub fn write_metadata_file_with_version(
		&self,
		path: &Path,
		salt: &Salt,
		version: Option<u32>,
	) -> Result<()> {
		let mut metadata = vec![
			format!("version={}", version.unwrap_or(CURRENT_VERSION)),
			format!("salt={}", hex::encode(salt)),
		];
		for i in 0..self.columns.len() {
			metadata.push(format!("col{}={}", i, self.columns[i].as_string()));
		}
		try_io!(std::fs::write(path, metadata.join("\n")));
		Ok(())
	}

	pub fn load_and_validate_metadata(&self, create: bool) -> Result<Metadata> {
		let meta = Self::load_metadata(&self.path)?;

		if let Some(meta) = meta {
			if meta.columns.len() != self.columns.len() {
				return Err(Error::InvalidConfiguration(format!(
					"Column config mismatch. Expected {} columns, got {}",
					self.columns.len(),
					meta.columns.len()
				)))
			}

			for c in 0..meta.columns.len() {
				if meta.columns[c] != self.columns[c] {
					return Err(Error::IncompatibleColumnConfig {
						id: c as ColId,
						reason: format!(
							"Column config mismatch. Expected \"{}\", got \"{}\"",
							self.columns[c].as_string(),
							meta.columns[c].as_string(),
						),
					})
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
		let file = std::io::BufReader::new(try_io!(std::fs::File::open(path)));
		let mut salt = None;
		let mut columns = Vec::new();
		let mut version = 0;
		for l in file.lines() {
			let l = try_io!(l);
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
				"Unsupported database version {version}. Expected {CURRENT_VERSION}"
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
