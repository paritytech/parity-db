// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{fmt::Debug, path::Path};
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
pub enum Action<O> {
	Transaction(Vec<O>),
	Restart,
}

pub trait DbSimulator {
	type Operation;
	type Model: Default;

	fn build_options(config: &Config, path: &Path) -> parity_db::Options;

	fn apply_operation_on_model(operation: &Self::Operation, model: &mut Self::Model);

	fn map_operation(operation: &Self::Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>>;

	fn model_content(model: &Self::Model) -> Vec<(Vec<u8>, Vec<u8>)>;

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
			match action {
				Action::Transaction(operations) => {
					if let Err(e) =
						db.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))
					{
						drop(db);
						let (d, m) = Self::fail_or_restart_after_db_error(
							e,
							&options,
							&actions,
							config.btree_index,
						);
						model = m;
						db = d;
					} else {
						for o in operations {
							Self::apply_operation_on_model(o, &mut model);
						}
						retry_operation(|| {
							check_db_and_model_are_equals(
								&db,
								Self::model_content(&model),
								config.btree_index,
							)
						})
						.unwrap();
					}
				},
				Action::Restart => {
					db = {
						drop(db);
						parity_db::Db::open_or_create(&options)
					}
					.and_then(|db| {
						match check_db_and_model_are_equals(
							&db,
							Self::model_content(&model),
							config.btree_index,
						)? {
							Ok(()) => Ok(db),
							Err(_) =>
								Err(parity_db::Error::Corruption("Instrumented failure".into())),
						}
					})
					.unwrap_or_else(|e| {
						let (db, m) = Self::fail_or_restart_after_db_error(
							e,
							&options,
							&actions,
							config.btree_index,
						);
						model = m;
						db
					});
				},
			}
		}
	}

	fn fail_or_restart_after_db_error(
		error: parity_db::Error,
		options: &parity_db::Options,
		actions: &[Action<Self::Operation>],
		is_db_b_tree: bool,
	) -> (parity_db::Db, Self::Model) {
		if !error.to_string().contains("Instrumented failure") {
			panic!("{}", error);
		}

		// Instrumented failure, we restart the database and check if it is back to a valid state

		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		let db = parity_db::Db::open_or_create(options).unwrap();

		// We look for the matching model
		let mut model = Self::Model::default();
		for action in actions {
			if check_db_and_model_are_equals(&db, Self::model_content(&model), is_db_b_tree)
				.unwrap()
				.is_ok()
			{
				return (db, model) //We found it!
			}

			if let Action::Transaction(operations) = action {
				for o in operations {
					Self::apply_operation_on_model(o, &mut model);
				}
			}
		}
		if let Err(e) =
			check_db_and_model_are_equals(&db, Self::model_content(&model), is_db_b_tree).unwrap()
		{
			panic!("Not able to recover to a proper state when coming back from an instrumented failure: {}", e)
		}
		(db, model)
	}
}

fn retry_operation<'a, T>(op: impl Fn() -> parity_db::Result<T> + 'a) -> T {
	(op()).unwrap_or_else(|_| {
		// We ignore the error and try to open the DB again
		parity_db::set_number_of_allowed_io_operations(usize::MAX);
		op().unwrap()
	})
}

fn check_db_and_model_are_equals(
	db: &parity_db::Db,
	mut model_content: Vec<(Vec<u8>, Vec<u8>)>,
	is_db_b_tree: bool,
) -> parity_db::Result<Result<(), String>> {
	for (k, v) in &model_content {
		if db.get(0, k)?.as_ref() != Some(v) {
			return Ok(Err(format!("The value {:?} for key {:?} is not in the database", k, v)))
		}
	}

	if is_db_b_tree {
		// We check the BTree forward iterator
		let mut db_iter = db.iter(0)?;
		db_iter.seek_to_first()?;
		let mut db_content = Vec::new();
		while let Some(e) = db_iter.next()? {
			db_content.push(e);
		}
		if db_content != model_content {
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
		if db_content != model_content {
			return Ok(Err(format!(
				"The backward iterator for the db gives {:?} and not {:?}",
				db_content, model_content
			)))
		}
	}
	Ok(Ok(()))
}
