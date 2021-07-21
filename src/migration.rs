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

/// Database migration.

use std::path::Path;
use crate::{options::Options, db::Db, Error, Result, column::ColId};

const COMMIT_SIZE: usize = 10240;

pub fn migrate(from: &Path, mut to: Options) -> Result<()> {
	let mut path: std::path::PathBuf = from.into();
	path.push("metadata");
	let source_meta = Options::load_metadata(&path)?
		.ok_or_else(|| Error::Migration("Error loading source metadata".into()))?;

	if source_meta.columns.len() != to.columns.len() {
		return Err(Error::Migration("Source and dest columns mismatch".into()));
	}

	let mut source_options = Options::with_columns(from, source_meta.columns.len() as u8);
	source_options.columns = source_meta.columns;

	// Make sure we are using the same salt value.
	to.salt = source_meta.salt;

	let source = Db::open(&source_options)?;
	let dest = Db::open_or_create(&to)?;

	let mut ncommits: u64 = 0;
	let mut commit = Vec::with_capacity(COMMIT_SIZE);
	let mut last_time = std::time::Instant::now();
	for c in 0 .. source_options.columns.len() as ColId {
		log::info!("Migrating col {}", c);
		source.iter_column_while(c, |index, key, rc, mut value| {
			//TODO: more efficient ref migration
			for _ in 0 .. rc {
				let value = std::mem::take(&mut value);
				commit.push((c, key.clone(), Some(value)));
				if commit.len() == COMMIT_SIZE {
					ncommits += 1;
					if let Err(e) = dest.commit_raw(std::mem::take(&mut commit)) {
						log::warn!("Migration error: {:?}", e);
						return false;
					}
					commit.reserve(COMMIT_SIZE);

					if last_time.elapsed() > std::time::Duration::from_secs(3) {
						last_time = std::time::Instant::now();
						log::info!("Migrating {} #{}, commit {}", c, index, ncommits);
					}
				}
			}
			true
		})?;
	}
	dest.commit_raw(commit)?;
	Ok(())
}

#[cfg(test)]
mod test {
	use crate::{Db, Options, migration::migrate};

	struct TempDir(std::path::PathBuf);

	impl TempDir {
		fn new(name: &'static str) -> TempDir {
			env_logger::try_init().ok();
			let mut path = std::env::temp_dir();
			path.push("parity-db-test");
			path.push("migration");
			path.push(name);

			if path.exists() {
				std::fs::remove_dir_all(&path).unwrap();
			}
			std::fs::create_dir_all(&path).unwrap();
			TempDir(path)
		}

		fn path(&self, sub: &str) -> std::path::PathBuf {
			let mut path = self.0.clone();
			path.push(sub);
			path
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			if self.0.exists() {
				std::fs::remove_dir_all(&self.0).unwrap();
			}
		}
	}

	#[test]
	fn migrate_simple() {
		let dir = TempDir::new("migrate_simple");
		let source_dir = dir.path("source");
		let dest_dir = dir.path("dest");
		{
			let source = Db::with_columns(&source_dir, 1).unwrap();
			source.commit([(0, b"1".to_vec(), Some(b"value".to_vec()))]).unwrap();
		}

		let dest_opts = Options::with_columns(&dest_dir, 1);

		migrate(&source_dir, dest_opts).unwrap();
		let dest = Db::with_columns(&dest_dir, 1).unwrap();
		assert_eq!(dest.get(0, b"1").unwrap(), Some("value".as_bytes().to_vec()));
	}
}

