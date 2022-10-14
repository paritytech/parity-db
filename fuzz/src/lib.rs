// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{cmp::Ordering, fmt::Debug, path::Path};
use tempfile::tempdir;

#[derive(Arbitrary, Debug, Clone, Copy)]
pub enum CompressionType {
	NoCompression,
	Snappy,
	Lz4,
}

impl From<CompressionType> for parity_db::CompressionType {
	fn from(t: CompressionType) -> Self {
		match t {
			CompressionType::NoCompression => Self::NoCompression,
			CompressionType::Snappy => Self::Snappy,
			CompressionType::Lz4 => Self::Lz4,
		}
	}
}

#[derive(Arbitrary, Debug)]
pub struct Config {
	pub btree_index: bool,
	pub compression: CompressionType,
	pub number_of_allowed_io_operations: u8,
}

#[derive(Arbitrary, Debug)]
pub enum Action<O: Debug> {
	Transaction(Vec<O>),
	ProcessReindex,
	ProcessCommits,
	FlushAndEnactLogs,
	CleanLogs,
	Restart,
}

pub trait DbSimulator {
	type Operation: Debug;
	type Model: Default + Debug;

	fn build_options(config: &Config, path: &Path) -> parity_db::Options;

	fn apply_operations_on_model<'a>(
		operations: impl IntoIterator<Item = &'a Self::Operation>,
		model: &mut Self::Model,
	) where
		Self::Operation: 'a;

	fn write_first_layer_to_disk(model: &mut Self::Model);

	fn attempt_to_reset_model_to_disk_state(
		model: &Self::Model,
		state: &[(u8, u8)],
	) -> Option<Self::Model>;

	fn map_operation(operation: &Self::Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>>;

	fn model_required_content(model: &Self::Model) -> Vec<(Vec<u8>, Vec<u8>)>;

	fn model_optional_content(model: &Self::Model) -> Vec<(Vec<u8>, Vec<u8>)>;

	fn model_removed_content(model: &Self::Model) -> Vec<Vec<u8>>;

	fn simulate(config: Config, actions: Vec<Action<Self::Operation>>) {
		let dir = tempdir().unwrap();
		let options = Self::build_options(&config, dir.path());

		// We don't check for now failures inside of initialization.
		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		let mut db = parity_db::Db::open_or_create(&options).unwrap();
		let mut model = Self::Model::default();
		parity_db::set_number_of_allowed_io_operations(
			config.number_of_allowed_io_operations.into(),
		);
		for action in &actions {
			// We apply the action on both the database and the model
			log::debug!("Applying on the database: {:?}", action);
			match action {
				Action::Transaction(operations) => {
					Self::apply_operations_on_model(operations, &mut model);
					db.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))
						.unwrap();
				},
				Action::ProcessReindex =>
					db = Self::try_or_restart(|db| db.process_reindex(), db, &mut model, &options),
				Action::ProcessCommits => {
					Self::write_first_layer_to_disk(&mut model);
					db = Self::try_or_restart(|db| db.process_commits(), db, &mut model, &options)
				},
				Action::FlushAndEnactLogs => {
					// We repeat flush and then call enact_log to avoid deadlocks due to
					// Log::flush_one side effects
					for _ in 0..2 {
						db = Self::try_or_restart(|db| db.flush_logs(), db, &mut model, &options)
					}
					db = Self::try_or_restart(|db| db.enact_logs(), db, &mut model, &options)
				},
				Action::CleanLogs =>
					db = Self::try_or_restart(|db| db.clean_logs(), db, &mut model, &options),
				Action::Restart => {
					db = {
						drop(db);
						retry_operation(|| parity_db::Db::open(&options))
					};
					Self::reset_model_from_database(&db, &mut model);
				},
			}
			retry_operation(|| {
				Self::check_db_and_model_are_equals(&db, &model, config.btree_index)
			})
			.unwrap();
		}
	}

	fn try_or_restart(
		op: impl FnOnce(&parity_db::Db) -> parity_db::Result<()>,
		mut db: parity_db::Db,
		model: &mut Self::Model,
		options: &parity_db::Options,
	) -> parity_db::Db {
		match op(&db) {
			Ok(()) => db,
			Err(e) if e.to_string().contains("Instrumented failure") => {
				log::debug!("Restarting after an instrumented failure");
				drop(db);
				parity_db::set_number_of_allowed_io_operations(usize::MAX);
				db = parity_db::Db::open(options).unwrap();
				Self::reset_model_from_database(&db, model);
				db
			},
			Err(e) => panic!("database error: {}", e),
		}
	}

	fn reset_model_from_database(db: &parity_db::Db, model: &mut Self::Model) {
		*model = retry_operation(|| {
			let mut disk_state = Vec::new();
			for i in u8::MIN..=u8::MAX {
				if let Some(v) = db.get(0, &[i])? {
					disk_state.push((i, v[0]));
				}
			}

			if let Some(model) = Self::attempt_to_reset_model_to_disk_state(model, &disk_state) {
				Ok(model)
			} else {
				Err(parity_db::Error::Corruption(format!("Not able to recover the database to one of the valid state. The current database state is: {:?}", disk_state)))
			}
		})
	}

	fn check_db_and_model_are_equals(
		db: &parity_db::Db,
		model: &Self::Model,
		is_db_b_tree: bool,
	) -> parity_db::Result<Result<(), String>> {
		for (k, v) in Self::model_required_content(model) {
			if db.get(0, &k)?.as_ref() != Some(&v) {
				return Ok(Err(format!("The value {:?} for key {:?} is not in the database", v, k)))
			}
		}
		for k in Self::model_removed_content(model) {
			if db.get(0, &k)?.is_some() {
				return Ok(Err(format!("The key {:?} should not be in the database", k)))
			}
		}

		if is_db_b_tree {
			let mut model_content = Self::model_optional_content(model);

			// We check the BTree forward iterator
			let mut db_iter = db.iter(0)?;
			db_iter.seek_to_first()?;
			let mut db_content = Vec::new();
			while let Some(e) = db_iter.next()? {
				db_content.push(e);
			}
			if !is_slice_included_in_sorted(&db_content, &model_content, |a, b| a.cmp(b)) {
				return Ok(Err(format!(
					"The forward iterator for the db gives {:?} and not {:?}",
					db_content, model_content
				)))
			}

			// We check the BTree backward iterator
			model_content.reverse();
			let mut db_iter = db.iter(0)?;
			db_iter.seek_to_last()?;
			let mut db_content = Vec::new();
			while let Some(e) = db_iter.prev()? {
				db_content.push(e);
			}
			if !is_slice_included_in_sorted(&db_content, &model_content, |a, b| b.cmp(a)) {
				return Ok(Err(format!(
					"The backward iterator for the db gives {:?} and not {:?}",
					db_content, model_content
				)))
			}
		}
		Ok(Ok(()))
	}
}

fn retry_operation<'a, T>(op: impl Fn() -> parity_db::Result<T> + 'a) -> T {
	(op()).unwrap_or_else(|e| {
		log::debug!("Database error: {}, restarting without I/O limitations.", e);

		// We ignore the error and try to open the DB again
		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		op().unwrap()
	})
}

fn is_slice_included_in_sorted<T>(
	small: &[T],
	large: &[T],
	cmp: impl Fn(&T, &T) -> Ordering,
) -> bool {
	let mut large = large.iter();
	for se in small {
		loop {
			let le = if let Some(le) = large.next() { le } else { return false };
			match cmp(se, le) {
				Ordering::Less => return false,
				Ordering::Greater => continue,
				Ordering::Equal => break,
			}
		}
	}
	true
}
