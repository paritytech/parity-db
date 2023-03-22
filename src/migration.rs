// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::{ColId, IterState},
	db::{CommitChangeSet, Db, IndexedChangeSet, Operation},
	error::try_io,
	options::Options,
	Error, Result,
};
/// Database migration.
use std::path::{Path, PathBuf};

const COMMIT_SIZE: usize = 10240;
const OVERWRITE_TMP_PATH: &str = "to_revert_overwrite";

/// Attempt to migrate a database to a new configuration with different column settings.
/// `from` Source database path
/// `to` New database configuration.
/// `overwrite` Ignore path set in `to` and attempt to overwrite data in place. This may be faster
/// but if migration fails data may be lost
/// `force_migrate` Force column re-population even if its setting did not change.
///
/// Note that migration between hash to btree columns is not possible.
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

	for c in to_migrate.iter() {
		if source_options.columns[*c as usize].btree_index || to.columns[*c as usize].btree_index {
			return Err(Error::Migration(
				"Migrate only implemented for hash indexed column to hash indexed column".into(),
			))
		}
	}

	for c in 0..source_options.columns.len() as ColId {
		if !to_migrate.contains(&c) {
			if !overwrite {
				drop(dest);
				copy_column(c, from, &to.path)?;
				dest = Db::open_or_create(&to)?;
			}
			continue
		}
		log::info!("Migrating col {}", c);
		source.iter_column_index_while(
			c,
			|IterState { chunk_index: index, key, rc, mut value }| {
				//TODO: more efficient ref migration
				for _ in 0..rc {
					let value = std::mem::take(&mut value);
					commit
						.indexed
						.entry(c)
						.or_insert_with(|| IndexedChangeSet::new(c))
						.changes
						.push(Operation::Set(key, value.into()));
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
			},
		)?;
		if overwrite {
			dest.commit_raw(commit)?;
			commit = Default::default();
			nb_commit = 0;
			drop(dest);
			dest = Db::open_or_create(&to)?; // This is needed to flush logs.
			log::info!("Collection migrated {}, imported", c);

			drop(dest);
			drop(source);
			let mut tmp_dir = from.to_path_buf();
			tmp_dir.push(OVERWRITE_TMP_PATH);
			let remove_tmp_dir = || -> Result<()> {
				if std::fs::metadata(&tmp_dir).is_ok() {
					std::fs::remove_dir_all(&tmp_dir).map_err(|e| {
						Error::Migration(format!("Error removing overwrite tmp dir: {e:?}"))
					})?;
				}
				Ok(())
			};
			remove_tmp_dir()?;
			std::fs::create_dir_all(&tmp_dir).map_err(|e| {
				Error::Migration(format!("Error creating overwrite tmp dir: {e:?}"))
			})?;

			move_column(c, from, &tmp_dir)?;
			move_column(c, &to.path, from)?;
			source_options.columns[c as usize] = to.columns[c as usize].clone();
			source_options
				.write_metadata(from, &to.salt.expect("Migrate requires salt"))
				.map_err(|e| {
					Error::Migration(format!(
						"Error {e:?}\nFail updating metadata of column {c:?} \
							in source, please restore manually before restarting."
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

/// Clear specified column. All data is removed and stats are reset.
/// Database must be closed before calling this.
pub fn clear_column(path: &Path, column: ColId) -> Result<()> {
	let meta = Options::load_metadata(path)?
		.ok_or_else(|| Error::Migration("Error loading source metadata".into()))?;

	if (column as usize) >= meta.columns.len() {
		return Err(Error::Migration("Invalid column index".into()))
	}

	// Validate the database by opening. This also makes sure all the logs are enacted,
	// so that after deleting a column there are no leftover commits that may write to it.
	let mut options = Options::with_columns(path, meta.columns.len() as u8);
	options.columns = meta.columns;
	options.salt = Some(meta.salt);
	let _db = Db::open(&options)?;
	drop(_db);

	// It is not specified how read_dir behaves when deleting and iterating in the same loop
	// We collect a list of paths to be deleted first.
	let mut to_delete = Vec::new();
	for entry in try_io!(std::fs::read_dir(path)) {
		let entry = try_io!(entry);
		if let Some(file) = entry.path().file_name().and_then(|f| f.to_str()) {
			if crate::index::TableId::is_file_name(column, file) ||
				crate::table::TableId::is_file_name(column, file)
			{
				to_delete.push(PathBuf::from(file));
			}
		}
	}

	for file in to_delete {
		let mut path = path.to_path_buf();
		path.push(file);
		try_io!(std::fs::remove_file(path));
	}
	Ok(())
}

fn move_column(c: ColId, from: &Path, to: &Path) -> Result<()> {
	deplace_column(c, from, to, false)
}

fn copy_column(c: ColId, from: &Path, to: &Path) -> Result<()> {
	deplace_column(c, from, to, true)
}

fn deplace_column(c: ColId, from: &Path, to: &Path, copy: bool) -> Result<()> {
	for entry in try_io!(std::fs::read_dir(from)) {
		let entry = try_io!(entry);
		if let Some(file) = entry.path().file_name().and_then(|f| f.to_str()) {
			if crate::index::TableId::is_file_name(c, file) ||
				crate::table::TableId::is_file_name(c, file)
			{
				let mut from = from.to_path_buf();
				from.push(file);
				let mut to = to.to_path_buf();
				to.push(file);
				if copy {
					try_io!(std::fs::copy(from, to));
				} else {
					try_io!(std::fs::rename(from, to));
				}
			}
		}
	}
	Ok(())
}

#[cfg(test)]
mod test {
	use crate::{migration, Db, Options};
	use tempfile::tempdir;

	#[test]
	fn migrate_simple() {
		let dir = tempdir().unwrap();
		let source_dir = dir.path().join("source");
		let dest_dir = dir.path().join("dest");
		{
			let source = Db::with_columns(&source_dir, 1).unwrap();
			source.commit([(0, b"1".to_vec(), Some(b"value".to_vec()))]).unwrap();
		}

		let dest_opts = Options::with_columns(&dest_dir, 1);
		migration::migrate(&source_dir, dest_opts, false, &[0]).unwrap();
		let dest = Db::with_columns(&dest_dir, 1).unwrap();
		assert_eq!(dest.get(0, b"1").unwrap(), Some("value".as_bytes().to_vec()));
	}

	#[test]
	fn clear_column() {
		let source_dir = tempdir().unwrap();
		let mut options = Options::with_columns(source_dir.path(), 3);
		options.columns = vec![Default::default(); 3];
		options.columns[1].btree_index = true;
		{
			let db = Db::open_or_create(&options).unwrap();

			db.commit(vec![
				(0, b"0".to_vec(), Some(b"value0".to_vec())),
				(1, b"1".to_vec(), Some(b"value1".to_vec())),
				(2, b"2".to_vec(), Some(b"value2".to_vec())),
			])
			.unwrap();
		}

		migration::clear_column(source_dir.path(), 1).unwrap();
		let db = Db::open(&options).unwrap();
		assert_eq!(db.get(0, b"0").unwrap(), Some("value0".as_bytes().to_vec()));
		assert_eq!(db.get(1, b"1").unwrap(), None);
		assert_eq!(db.get(2, b"2").unwrap(), Some("value2".as_bytes().to_vec()));
	}
}
