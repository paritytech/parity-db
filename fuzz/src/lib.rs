// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{collections::HashMap, fmt::Debug};
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
	FlushAndEnactLogs,
	CleanLogs,
	Restart,
	IterPrev,
	IterNext,
}

#[derive(Clone, Debug)]
pub struct Layer<V> {
	// The stored value per possible key (depends if we have ref counting or not)
	pub values: [Option<V>; NUMBER_OF_POSSIBLE_KEYS],
	pub written: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum IterPosition {
	Start,
	Value(u8),
	End,
}

pub struct DbWithIter {
	iter: Option<parity_db::BTreeIterator<'static>>,
	iter_current_key: Option<IterPosition>,
	db: parity_db::Db,
}

impl DbWithIter {
	fn open(options: &parity_db::Options) -> parity_db::Result<Self> {
		let db = parity_db::Db::open_or_create(options)?;
		// Ok because we never move it outside of the current struct where the database is required
		// to outlive the iterator
		let iter = if options.columns[0].btree_index {
			Some(unsafe { std::mem::transmute(db.iter(0)?) })
		} else {
			None
		};
		Ok(Self { db, iter, iter_current_key: None })
	}
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
		let mut db = DbWithIter::open(&options).unwrap();
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
					let mut values = model_values(&layers);
					Self::apply_operations_on_values(operations, &mut values);
					layers.push(Layer { values, written: false });
					db.db
						.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))
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
				Action::FlushAndEnactLogs => {
					// We repeat flush and then call enact_log to avoid deadlocks due to
					// Log::flush_one side effects
					for _ in 0..2 {
						db = Self::try_or_restart(
							|db| db.flush_logs(),
							db,
							&mut layers,
							&old_layers,
							&options,
						)
					}
					db = Self::try_or_restart(
						|db| db.enact_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					)
				},
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
						retry_operation(|| DbWithIter::open(&options))
					};
					Self::reset_model_from_database(&db.db, &mut layers, &old_layers);
				},
				Action::IterPrev =>
					if let Some(iter) = &mut db.iter {
						let mut old_key = if let Some(old_key) = db.iter_current_key.take() {
							old_key
						} else {
							retry_operation(|| iter.seek_to_last());
							IterPosition::End
						};
						let new_key_value =
							iter.prev().unwrap_or_else(|e| {
								log::debug!("Database error: {}, restarting iter.prev without I/O limitations.", e);

								// We ignore the error and reset the iterator
								parity_db::set_number_of_allowed_io_operations(usize::MAX);
								iter.seek_to_last().unwrap();
								old_key = IterPosition::End;
								iter.prev().unwrap()
							});
						let mut content = Self::layer_required_content(&model_values(&layers));
						content.sort();
						match old_key {
							IterPosition::Start => {
								log::info!(
									"Prev lookup on iterator in start state, expecting None"
								);
								assert_eq!(new_key_value, None);
								db.iter_current_key = Some(IterPosition::Start);
							},
							IterPosition::Value(old_key) => {
								let expected = Self::valid_previous_values(&[old_key], &layers);
								log::info!(
									"Prev lookup on iterator on old key {}, expecting one of {:?}",
									old_key,
									expected
								);
								assert!(expected.contains(&new_key_value), "Prev lookup on iterator on old key {}, expecting one of {:?}, found {:?}",
										old_key,
										expected, new_key_value);
								db.iter_current_key =
									Some(new_key_value.map_or(IterPosition::Start, |(k, _)| {
										IterPosition::Value(k[0])
									}));
							},
							IterPosition::End => {
								let expected =
									Self::valid_previous_values(&[u8::MAX, u8::MAX], &layers);
								log::info!(
									"Prev lookup on iterator on end state, expecting one of {:?}",
									expected
								);
								assert!(expected.contains(&new_key_value), "Prev lookup on iterator on end state, expecting one of {:?}, found {:?}",
										expected, new_key_value);
								db.iter_current_key =
									Some(new_key_value.map_or(IterPosition::Start, |(k, _)| {
										IterPosition::Value(k[0])
									}));
							},
						}
					},
				Action::IterNext =>
					if let Some(iter) = &mut db.iter {
						let mut old_key = if let Some(old_key) = db.iter_current_key.take() {
							old_key
						} else {
							retry_operation(|| iter.seek_to_first());
							IterPosition::Start
						};
						let new_key_value =
							iter.next().unwrap_or_else(|e| {
								log::debug!("Database error: {}, restarting iter.next without I/O limitations.", e);

								// We ignore the error and reset the iterator
								parity_db::set_number_of_allowed_io_operations(usize::MAX);
								iter.seek_to_first().unwrap();
								old_key = IterPosition::Start;
								iter.next().unwrap()
							});
						match old_key {
							IterPosition::Start => {
								let expected = Self::valid_next_values(&[], &layers);
								log::info!(
									"Next lookup on iterator on start state, expecting any of {:?}",
									expected
								);
								assert!(expected.contains(&new_key_value), "Next lookup on iterator on start state, expecting any of {:?}, found {:?}", expected, new_key_value);
								db.iter_current_key =
									Some(new_key_value.map_or(IterPosition::End, |(k, _)| {
										IterPosition::Value(k[0])
									}));
							},
							IterPosition::Value(old_key) => {
								let expected = Self::valid_next_values(&[old_key], &layers);
								log::info!(
									"Next lookup on iterator on old key {}, expecting one of {:?}",
									old_key,
									expected
								);
								assert!(expected.contains(&new_key_value), "Next lookup on iterator on old key {}, expecting one of {:?}, found {:?}", old_key, expected, new_key_value);
								db.iter_current_key =
									Some(new_key_value.map_or(IterPosition::End, |(k, _)| {
										IterPosition::Value(k[0])
									}));
							},
							IterPosition::End => {
								log::info!("Next lookup on iterator in end state, expecting None");
								assert_eq!(new_key_value, None);
								db.iter_current_key = Some(IterPosition::End);
							},
						}
					},
			}
			retry_operation(|| Self::check_db_and_model_are_equals(&db.db, &layers)).unwrap();
		}
	}

	fn try_or_restart(
		op: impl FnOnce(&parity_db::Db) -> parity_db::Result<()>,
		mut db: DbWithIter,
		layers: &mut Vec<Layer<Self::ValueType>>,
		old_layers: &[Vec<Layer<Self::ValueType>>],
		options: &parity_db::Options,
	) -> DbWithIter {
		match op(&db.db) {
			Ok(()) => db,
			Err(e) if e.to_string().contains("Instrumented failure") => {
				log::debug!("Restarting after an instrumented failure");
				drop(db);
				parity_db::set_number_of_allowed_io_operations(usize::MAX);
				db = DbWithIter::open(options).unwrap();
				Self::reset_model_from_database(&db.db, layers, old_layers);
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
	) -> parity_db::Result<Result<(), String>> {
		let values = model_values(layers);
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
		Ok(Ok(()))
	}

	fn valid_next_values(
		current_key: &[u8],
		layers: &[Layer<Self::ValueType>],
	) -> Vec<Option<(Vec<u8>, Vec<u8>)>> {
		let values = model_values(layers);

		// We pick first the next required value
		let mut required_content = Self::layer_required_content(&values);
		required_content.sort();
		let next_required_key = required_content
			.iter()
			.filter_map(|(k, _)| if current_key < k.as_slice() { Some(k) } else { None })
			.next();

		let mut possible_content = Self::layer_optional_content(&values);
		possible_content.sort();
		let mut result = possible_content
			.into_iter()
			.filter(|(k, _)| {
				current_key < k.as_slice() &&
					next_required_key.map_or(true, |next_required_key| k <= next_required_key)
			})
			.map(Some)
			.collect::<Vec<_>>();
		if next_required_key.is_none() {
			result.push(None);
		}
		result
	}

	fn valid_previous_values(
		current_key: &[u8],
		layers: &[Layer<Self::ValueType>],
	) -> Vec<Option<(Vec<u8>, Vec<u8>)>> {
		let values = model_values(layers);

		// We pick first the next required value
		let mut required_content = Self::layer_required_content(&values);
		required_content.sort();
		let previous_required_key = required_content
			.iter()
			.rev()
			.filter_map(|(k, _)| if k.as_slice() < current_key { Some(k) } else { None })
			.next();

		let mut possible_content = Self::layer_optional_content(&values);
		possible_content.sort();
		let mut result = possible_content
			.into_iter()
			.filter(|(k, _)| {
				k.as_slice() < current_key &&
					previous_required_key
						.map_or(true, |previous_required_key| previous_required_key <= k)
			})
			.map(Some)
			.collect::<Vec<_>>();
		if previous_required_key.is_none() {
			result.push(None);
		}
		result
	}
}

fn model_values<T: Copy>(layers: &[Layer<T>]) -> [Option<T>; NUMBER_OF_POSSIBLE_KEYS] {
	layers.last().map_or([None; NUMBER_OF_POSSIBLE_KEYS], |l| l.values)
}

fn retry_operation<'a, T>(mut op: impl FnMut() -> parity_db::Result<T> + 'a) -> T {
	(op()).unwrap_or_else(|e| {
		log::debug!("Database error: {}, let's keep going without I/O limitations.", e);

		// We ignore the error and try to redo it
		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		op().unwrap()
	})
}
