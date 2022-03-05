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
	column::{ColId, IterState},
	db::{CommitChangeSet, Db, IndexedChangeSet},
	options::Options,
	Error, Result,
};
/// Database migration.
use std::path::Path;

const COMMIT_SIZE: usize = 10240;
const OVERWRITE_TMP_PATH: &str = "to_revert_overwrite";

pub fn migrate(from: &Path, mut to: Options, overwrite: bool, force_migrate: &[u8]) -> Result<()> {
	let source_meta = Options::load_metadata(from)?
		.ok_or_else(|| Error::Migration("Error loading source metadata".into()))?;

	let mut to_migrate = source_meta.columns_to_migrate();
	for force in force_migrate.iter() {
		to_migrate.insert(*force);
	}
	if source_meta.columns.len() != to.columns.len() {
		return Err(Error::Migration("Source and dest columns mismatch".into()))
	}

	// Make sure we are using the same salt value.
	to.salt = Some(source_meta.salt);

	if (to.salt.is_none()) && overwrite {
		return Err(Error::Migration("Changing salt need to update metadata at once.".into()))
	}

	let mut source_options = Options::with_columns(from, source_meta.columns.len() as u8);
	source_options.salt = Some(source_meta.salt);
	source_options.columns = source_meta.columns;

	let mut source = Db::open(&source_options)?;
	let mut dest = Db::open_or_create(&to)?;

	let mut ncommits: u64 = 0;
	let mut commit = CommitChangeSet::default();
	let mut nb_commit = 0;
	let mut last_time = std::time::Instant::now();
	for c in 0..source_options.columns.len() as ColId {
		if source_options.columns[c as usize] != to.columns[c as usize] {
			to_migrate.insert(c);
		}
	}
	for c in 0..source_options.columns.len() as ColId {
		if !to_migrate.contains(&c) {
			if !overwrite {
				std::mem::drop(dest);
				copy_column(c, from, &to.path)?;
				dest = Db::open_or_create(&to)?;
			}
			continue
		}
		log::info!("Migrating col {}", c);
		source.iter_column_while(c, |IterState { chunk_index: index, key, rc, mut value }| {
			//TODO: more efficient ref migration
			for _ in 0..rc {
				let value = std::mem::take(&mut value);
				commit
					.indexed
					.entry(c)
					.or_insert_with(|| IndexedChangeSet::new(c))
					.changes
					.push((key, Some(value)));
				nb_commit += 1;
				if nb_commit == COMMIT_SIZE {
					ncommits += 1;
					if let Err(e) = dest.commit_raw(std::mem::take(&mut commit)) {
						log::warn!("Migration error: {:?}", e);
						return false
					}
					nb_commit = 0;

					if last_time.elapsed() > std::time::Duration::from_secs(3) {
						last_time = std::time::Instant::now();
						log::info!("Migrating {} #{}, commit {}", c, index, ncommits);
					}
				}
			}
			true
		})?;
		if overwrite {
			dest.commit_raw(commit)?;
			commit = Default::default();
			nb_commit = 0;
			std::mem::drop(dest);
			dest = Db::open_or_create(&to)?; // This is needed to flush logs.
			log::info!("Collection migrated {}, imported", c);

			std::mem::drop(dest);
			std::mem::drop(source);
			let mut tmp_dir = from.to_path_buf();
			tmp_dir.push(OVERWRITE_TMP_PATH);
			let remove_tmp_dir = || -> Result<()> {
				if std::fs::metadata(&tmp_dir).is_ok() {
					std::fs::remove_dir_all(&tmp_dir).map_err(|e| {
						Error::Migration(format!("Error removing overwrite tmp dir: {:?}", e))
					})?;
				}
				Ok(())
			};
			remove_tmp_dir()?;
			std::fs::create_dir_all(&tmp_dir).map_err(|e| {
				Error::Migration(format!("Error creating overwrite tmp dir: {:?}", e))
			})?;

			move_column(c, from, &tmp_dir)?;
			move_column(c, &to.path, from)?;
			source_options.columns[c as usize] = to.columns[c as usize].clone();
			source_options
				.write_metadata(from, &to.salt.expect("Migrate requires salt"))
				.map_err(|e| {
					Error::Migration(format!(
						"Error {:?}\nFail updating metadata of column {:?} \
							in source, please restore manually before restarting.",
						e, c
					))
				})?;
			remove_tmp_dir()?;
			source = Db::open(&source_options)?;
			dest = Db::open_or_create(&to)?;

			log::info!("Collection migrated {}, migrated", c);
		}
	}
	dest.commit_raw(commit)?;
	Ok(())
}

fn move_column(c: ColId, from: &Path, to: &Path) -> Result<()> {
	deplace_column(c, from, to, false)
}

fn copy_column(c: ColId, from: &Path, to: &Path) -> Result<()> {
	deplace_column(c, from, to, true)
}

fn deplace_column(c: ColId, from: &Path, to: &Path, copy: bool) -> Result<()> {
	for entry in std::fs::read_dir(from)? {
		let entry = entry?;
		if let Some(file) = entry.path().file_name().and_then(|f| f.to_str()) {
			if crate::index::TableId::is_file_name(c, file) ||
				crate::table::TableId::is_file_name(c, file)
			{
				let mut from = from.to_path_buf();
				from.push(file);
				let mut to = to.to_path_buf();
				to.push(file);
				if copy {
					std::fs::copy(from, to)?;
				} else {
					std::fs::rename(from, to)?;
				}
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod test {
	use crate::{migration::migrate, Db, Options};

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

		migrate(&source_dir, dest_opts, false, &[0]).unwrap();
		let dest = Db::with_columns(&dest_dir, 1).unwrap();
		assert_eq!(dest.get(0, b"1").unwrap(), Some("value".as_bytes().to_vec()));
	}
}
