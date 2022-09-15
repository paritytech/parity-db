// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use std::{fmt::Debug, iter::once, path::Path};
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

	fn simulate(config: Config, actions: Vec<Action<Self::Operation>>) -> parity_db::Result<()> {
		let dir = tempdir().unwrap();
		let options = Self::build_options(&config, dir.path());
		parity_db::set_number_of_allowed_io_operations(
			config.number_of_allowed_io_operations.into(),
		);
		if let Err(e) = Self::apply_simulation(&options, &actions, config.btree_index) {
			if e.to_string() != "IO Error: Instrumented failure" {
				// Real error
				panic!("DB error: {}", e);
			}

			// We check that the database is in a correct state
			parity_db::set_number_of_allowed_io_operations(usize::MAX);
			let db = parity_db::Db::open_or_create(&options).unwrap();
			let mut model = Self::Model::default();
			for action in once(Action::Transaction(vec![])).chain(actions) {
				// We apply the action on both the database and the model
				if let Action::Transaction(operations) = action {
					for o in &operations {
						Self::apply_operation_on_model(o, &mut model);
					}
					if check_db_and_model_are_equals(
						&db,
						Self::model_content(&model),
						config.btree_index,
					)
					.unwrap()
					.is_ok()
					{
						return Ok(()) //Ok
					}
				}
			}
			// We print an error message
			if let Err(e) =
				check_db_and_model_are_equals(&db, Self::model_content(&model), config.btree_index)
					.unwrap()
			{
				panic!("Not able to recover to a proper state when coming back from an instrumented failure: {}", e);
			}
		}
		Ok(())
	}

	fn apply_simulation(
		options: &parity_db::Options,
		actions: &[Action<Self::Operation>],
		is_btree: bool,
	) -> parity_db::Result<()> {
		let mut db = parity_db::Db::open_or_create(options)?;
		let mut model = Self::Model::default();
		for action in actions {
			// We apply the action on both the database and the model
			match action {
				Action::Transaction(operations) => {
					db.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))?;
					for o in operations {
						Self::apply_operation_on_model(o, &mut model);
					}
				},
				Action::Restart =>
					db = {
						drop(db);
						parity_db::Db::open_or_create(options)?
					},
			}

			check_db_and_model_are_equals(&db, Self::model_content(&model), is_btree)?.unwrap();
		}
		Ok(())
	}
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
