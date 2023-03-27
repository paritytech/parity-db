// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{
	cmp::{Ordering, PartialOrd},
	collections::HashMap,
	fmt::{self, Debug, Formatter},
};
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
	IterPrev,
	IterNext,
}

#[derive(Clone)]
pub struct Layer<V> {
	// The stored value per possible key (depends if we have ref counting or not)
	pub values: [Option<V>; NUMBER_OF_POSSIBLE_KEYS],
	pub written: WrittenState,
}

impl<V: Debug> Debug for Layer<V> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let mut output = f.debug_map();
		output.entry(&"written", &self.written);
		for (k, v) in self.values.iter().enumerate() {
			if let Some(v) = v {
				output.entry(&k, v);
			}
		}
		output.finish()
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrittenState {
	/// The state has been persisted to disk.
	Yes,
	/// The state has been inserted into the log. It might not be flushed yet do disk.
	Processed,
	/// The state is still in the in-memory cache.
	No,
}

#[derive(Clone, Copy, Debug)]
pub enum IterPosition {
	Start,
	Value(u8),
	End,
}

impl PartialEq<[u8]> for IterPosition {
	fn eq(&self, other: &[u8]) -> bool {
		match self {
			Self::Start => false,
			Self::Value(v) => [*v] == other,
			Self::End => false,
		}
	}
}

impl PartialOrd<[u8]> for IterPosition {
	fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
		match self {
			Self::Start => Some(Ordering::Less),
			Self::Value(v) => [*v].as_slice().partial_cmp(other),
			Self::End => Some(Ordering::Greater),
		}
	}
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
					db.db
						.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))
						.unwrap();
					layers.push(Layer { values, written: WrittenState::No });
				},
				Action::ProcessReindex => {
					let (new_db, _) = Self::try_or_restart(
						|db| db.process_reindex(),
						db,
						&mut layers,
						&old_layers,
						&options,
					);
					db = new_db;
				},
				Action::ProcessCommits => {
					for layer in &mut layers {
						if layer.written == WrittenState::No {
							layer.written = WrittenState::Processed;
							break
						}
					}
					let (new_db, _) = Self::try_or_restart(
						|db| db.process_commits(),
						db,
						&mut layers,
						&old_layers,
						&options,
					);
					db = new_db;
				},
				Action::FlushLog => {
					let (new_db, result) = Self::try_or_restart(
						|db| db.flush_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					);
					db = new_db;
					if result.is_ok() {
						for layer in &mut layers {
							if layer.written == WrittenState::Processed {
								layer.written = WrittenState::Yes;
							}
						}
					}
				},
				Action::EnactLog => {
					let (new_db, _) = Self::try_or_restart(
						|db| db.enact_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					);
					db = new_db;
				},
				Action::CleanLogs => {
					let (new_db, _) = Self::try_or_restart(
						|db| db.clean_logs(),
						db,
						&mut layers,
						&old_layers,
						&options,
					);
					db = new_db;
				},
				Action::Restart => {
					// drop might flush commits
					for layer in &mut layers {
						layer.written = WrittenState::Processed;
					}
					old_layers.push(layers.clone());
					db = {
						drop(db);
						retry_operation(|| DbWithIter::open(&options))
					};
					Self::reset_model_from_database(&db.db, &mut layers, &old_layers);
				},
				Action::IterPrev => {
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
						let expected = Self::valid_iter_value(old_key, &layers, Ordering::Greater);
						log::info!(
							"Prev lookup on iterator with old position {:?}, expecting one of {:?}",
							old_key,
							expected
						);
						assert!(expected.contains(&new_key_value), "{}", "Prev lookup on iterator with old position {old_key:?}, expecting one of {expected:?}, found {new_key_value:?}");
						db.iter_current_key = Some(
							new_key_value
								.map_or(IterPosition::Start, |(k, _)| IterPosition::Value(k[0])),
						);
					}
				},
				Action::IterNext => {
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
						let expected = Self::valid_iter_value(old_key, &layers, Ordering::Less);
						log::info!(
							"Next lookup on iterator with old position {:?}, expecting one of {:?}",
							old_key,
							expected
						);
						assert!(expected.contains(&new_key_value), "{}", "Next lookup on iterator with old position {old_key:?}, expecting one of {expected:?}, found {new_key_value:?}");
						db.iter_current_key = Some(
							new_key_value
								.map_or(IterPosition::End, |(k, _)| IterPosition::Value(k[0])),
						);
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
	) -> (DbWithIter, Result<(), parity_db::Error>) {
		match op(&db.db) {
			Ok(()) => (db, Ok(())),
			Err(e) if e.to_string().contains("Instrumented failure") => {
				log::debug!("Restarting after an instrumented failure");
				drop(db);
				parity_db::set_number_of_allowed_io_operations(usize::MAX);
				db = DbWithIter::open(options).unwrap();
				Self::reset_model_from_database(&db.db, layers, old_layers);
				(db, Err(e))
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

			if let Some(layers) = Self::attempt_to_reset_model_to_disk_state(layers, &disk_state)? {
				return Ok(layers)
			}
			for layers in old_layers {
				if let Some(layers) =
					Self::attempt_to_reset_model_to_disk_state(layers, &disk_state)?
				{
					return Ok(layers)
				}
			}
			Err(parity_db::Error::Corruption(format!("Not able to recover the database to one of the valid state. The current database state is: {disk_state:?} and the state stack is is {layers:?}")))
		})
	}

	fn attempt_to_reset_model_to_disk_state(
		layers: &[Layer<Self::ValueType>],
		state: &[(u8, u8)],
	) -> Result<Option<Vec<Layer<Self::ValueType>>>, parity_db::Error> {
		let mut candidates = Vec::new();
		for layer in layers.iter().rev() {
			match layer.written {
				WrittenState::No => (),
				WrittenState::Processed => {
					// might already be flushed to disk but maybe we are in a previous state
					if Self::is_layer_state_compatible_with_disk_state(&layer.values, state) {
						candidates.push(layer)
					}
				},
				WrittenState::Yes => {
					// must be the state
					if Self::is_layer_state_compatible_with_disk_state(&layer.values, state) {
						candidates.push(layer);
						break
					}
					if candidates.is_empty() {
						return Err(parity_db::Error::Corruption(format!("Not able to recover the database to one of the valid state. The current database state is: {state:?} and the state stack is is {layers:?}")));
					}
				},
			}
		}
		Ok(if candidates.is_empty() {
			if state.is_empty() {
				Some(Vec::new())
			} else {
				None
			}
		} else {
			Some(vec![Self::build_best_layer_for_recovery(&candidates)])
		})
	}

	fn check_db_and_model_are_equals(
		db: &parity_db::Db,
		layers: &[Layer<Self::ValueType>],
	) -> parity_db::Result<Result<(), String>> {
		let values = model_values(layers);
		for (k, v) in Self::layer_required_content(&values) {
			if db.get(0, &k)?.as_ref() != Some(&v) {
				return Ok(Err(format!("The value {v:?} for key {k:?} is not in the database")))
			}
		}
		for k in Self::layer_removed_content(&values) {
			if db.get(0, &k)?.is_some() {
				return Ok(Err(format!("The key {k:?} should not be in the database")))
			}
		}
		Ok(Ok(()))
	}

	fn valid_iter_value(
		current_position: IterPosition,
		layers: &[Layer<Self::ValueType>],
		direction: Ordering,
	) -> Vec<Option<(Vec<u8>, Vec<u8>)>> {
		let values = model_values(layers);

		// We pick first the next required value
		let mut required_content = Self::layer_required_content(&values);
		required_content.sort();
		let next_required_key = required_content
			.iter()
			.filter_map(|(k, _)| {
				if current_position.partial_cmp(k.as_slice()) == Some(direction) {
					Some(k)
				} else {
					None
				}
			})
			.next();

		let mut possible_content = Self::layer_optional_content(&values);
		possible_content.sort();
		let mut result = possible_content
			.into_iter()
			.filter(|(k, _)| {
				current_position.partial_cmp(k.as_slice()) == Some(direction) &&
					next_required_key.map_or(true, |next_required_key| {
						k == next_required_key || k.cmp(next_required_key) == direction
					})
			})
			.map(Some)
			.collect::<Vec<_>>();
		if next_required_key.is_none() {
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
