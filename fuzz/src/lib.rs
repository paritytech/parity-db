// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{cmp::Ordering, collections::HashMap, fmt::Debug};
use tempfile::tempdir;

pub const NUMBER_OF_POSSIBLE_KEYS: usize = 256;

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
	FlushLog,
	EnactLog,
	CleanLogs,
	Restart,
}

#[derive(Clone, Debug)]
pub struct Layer<V> {
	// The stored value per possible key (depends if we have ref counting or not)
	pub values: [Option<V>; NUMBER_OF_POSSIBLE_KEYS],
	pub written: bool,
}

pub trait DbSimulator {
	type ValueType: Debug + Copy;
	type Operation: Debug;

	fn build_column_options(config: &Config) -> parity_db::ColumnOptions;

	fn apply_operations_on_values<'a>(
		operations: impl IntoIterator<Item = &'a Self::Operation>,
		values: &mut [Option<Self::ValueType>; NUMBER_OF_POSSIBLE_KEYS],
	) where
		Self::Operation: 'a;

	fn is_layer_state_compatible_with_disk_state(
		layer_values: &[Option<Self::ValueType>; NUMBER_OF_POSSIBLE_KEYS],
		state: &[(u8, u8)],
	) -> bool;

	fn build_best_layer_for_recovery(layers: &[&Layer<Self::ValueType>]) -> Layer<Self::ValueType>;

	fn map_operation(operation: &Self::Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>>;

	fn layer_required_content(
		values: &[Option<Self::ValueType>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)>;

	fn layer_optional_content(
		values: &[Option<Self::ValueType>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)>;

	fn layer_removed_content(
		values: &[Option<Self::ValueType>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<Vec<u8>>;

	fn simulate(config: Config, actions: Vec<Action<Self::Operation>>) {
		let dir = tempdir().unwrap();
		let options = parity_db::Options {
			path: dir.path().to_owned(),
			columns: vec![Self::build_column_options(&config)],
			sync_wal: true,
			sync_data: true,
			stats: false,
			salt: Some([0; 32]),
			compression_threshold: HashMap::new(),
			always_flush: true,
			with_background_thread: false,
		};

		// We don't check for now failures inside of initialization.
		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		let mut db = parity_db::Db::open_or_create(&options).unwrap();
		let mut layers = Vec::new();
		// In case of bad writes, when restarting the DB we might end up to with a state of the
		// previous opening but with a state of a older one.
		let mut old_layers = Vec::new();
		parity_db::set_number_of_allowed_io_operations(
			config.number_of_allowed_io_operations.into(),
		);
		for action in &actions {
			// We apply the action on both the database and the model
			log::debug!("Applying on the database: {:?}", action);
			match action {
				Action::Transaction(operations) => {
					let mut values = layers
						.last()
						.map_or([None; NUMBER_OF_POSSIBLE_KEYS], |l: &Layer<Self::ValueType>| {
							l.values
						});
					Self::apply_operations_on_values(operations, &mut values);
					layers.push(Layer { values, written: false });
					db.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))
						.unwrap();
				},
				Action::ProcessReindex =>
					db = Self::try_or_restart(
						|db| db.process_reindex(),
						db,
						&mut layers,
						&old_layers,
						&options,
					),
				Action::ProcessCommits => {
					for layer in &mut layers {
						if !layer.written {
							layer.written = true;
							break
						}
					}
					db = Self::try_or_restart(
						|db| db.process_commits(),
						db,
						&mut layers,
						&old_layers,
						&options,
					)
				},
				Action::FlushLog =>
					db = Self::try_or_restart(
						|db| db.flush_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					),
				Action::EnactLog =>
					db = Self::try_or_restart(
						|db| db.enact_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					),
				Action::CleanLogs =>
					db = Self::try_or_restart(
						|db| db.clean_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					),
				Action::Restart => {
					old_layers.push(layers.clone());
					db = {
						drop(db);
						retry_operation(|| parity_db::Db::open(&options))
					};
					Self::reset_model_from_database(&db, &mut layers, &old_layers);
				},
			}
			retry_operation(|| {
				Self::check_db_and_model_are_equals(&db, &layers, config.btree_index)
			})
			.unwrap();
		}
	}

	fn try_or_restart(
		op: impl FnOnce(&parity_db::Db) -> parity_db::Result<()>,
		mut db: parity_db::Db,
		layers: &mut Vec<Layer<Self::ValueType>>,
		old_layers: &[Vec<Layer<Self::ValueType>>],
		options: &parity_db::Options,
	) -> parity_db::Db {
		match op(&db) {
			Ok(()) => db,
			Err(e) if e.to_string().contains("Instrumented failure") => {
				log::debug!("Restarting after an instrumented failure");
				drop(db);
				parity_db::set_number_of_allowed_io_operations(usize::MAX);
				db = parity_db::Db::open(options).unwrap();
				Self::reset_model_from_database(&db, layers, old_layers);
				db
			},
			Err(e) => panic!("database error: {}", e),
		}
	}

	fn reset_model_from_database(
		db: &parity_db::Db,
		layers: &mut Vec<Layer<Self::ValueType>>,
		old_layers: &[Vec<Layer<Self::ValueType>>],
	) {
		*layers = retry_operation(|| {
			let mut disk_state = Vec::new();
			for i in u8::MIN..=u8::MAX {
				if let Some(v) = db.get(0, &[i])? {
					disk_state.push((i, v[0]));
				}
			}

			if let Some(layers) = Self::attempt_to_reset_model_to_disk_state(layers, &disk_state) {
				return Ok(layers)
			}
			for layers in old_layers {
				if let Some(layers) =
					Self::attempt_to_reset_model_to_disk_state(layers, &disk_state)
				{
					return Ok(layers)
				}
			}
			Err(parity_db::Error::Corruption(format!("Not able to recover the database to one of the valid state. The current database state is: {:?}", disk_state)))
		})
	}

	fn attempt_to_reset_model_to_disk_state(
		layers: &[Layer<Self::ValueType>],
		state: &[(u8, u8)],
	) -> Option<Vec<Layer<Self::ValueType>>> {
		let mut candidates = Vec::new();
		for layer in layers.iter().rev() {
			if !layer.written {
				continue
			}

			if Self::is_layer_state_compatible_with_disk_state(&layer.values, state) {
				// We found a correct last layer
				candidates.push(layer);
			}
		}
		if candidates.is_empty() {
			if state.is_empty() {
				Some(Vec::new())
			} else {
				None
			}
		} else {
			Some(vec![Self::build_best_layer_for_recovery(&candidates)])
		}
	}

	fn check_db_and_model_are_equals(
		db: &parity_db::Db,
		layers: &[Layer<Self::ValueType>],
		is_db_b_tree: bool,
	) -> parity_db::Result<Result<(), String>> {
		let values = layers.last().map_or([None; NUMBER_OF_POSSIBLE_KEYS], |l| l.values);
		for (k, v) in Self::layer_required_content(&values) {
			if db.get(0, &k)?.as_ref() != Some(&v) {
				return Ok(Err(format!("The value {:?} for key {:?} is not in the database", v, k)))
			}
		}
		for k in Self::layer_removed_content(&values) {
			if db.get(0, &k)?.is_some() {
				return Ok(Err(format!("The key {:?} should not be in the database", k)))
			}
		}

		if is_db_b_tree {
			let mut model_content = Self::layer_optional_content(&values);

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
