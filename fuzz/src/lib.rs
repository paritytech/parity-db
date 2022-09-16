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
		let mut db = parity_db::Db::open_or_create(&options)?;
		let mut model = Self::Model::default();
		for action in actions {
			// We apply the action on both the database and the model
			match action {
				Action::Transaction(operations) => {
					db.commit_changes(operations.iter().map(|o| (0, Self::map_operation(o))))?;
					for o in &operations {
						Self::apply_operation_on_model(o, &mut model);
					}
				},
				Action::Restart =>
					db = {
						drop(db);
						parity_db::Db::open_or_create(&options)?
					},
			}

			assert_db_and_model_are_equals(&db, Self::model_content(&model), config.btree_index)?;
		}
		Ok(())
	}
}

fn assert_db_and_model_are_equals(
	db: &parity_db::Db,
	mut model_content: Vec<(Vec<u8>, Vec<u8>)>,
	is_db_b_tree: bool,
) -> parity_db::Result<()> {
	for (k, v) in &model_content {
		assert_eq!(db.get(0, k)?.as_ref(), Some(v));
	}

	if is_db_b_tree {
		// We check the BTree forward iterator
		let mut db_iter = db.iter(0)?;
		db_iter.seek_to_first()?;
		let mut db_content = Vec::new();
		while let Some(e) = db_iter.next()? {
			db_content.push(e);
		}
		assert_eq!(db_content, model_content);

		// We check the BTree backward iterator
		model_content.reverse();
		let mut db_iter = db.iter(0)?;
		db_iter.seek_to_last()?;
		let mut db_content = Vec::new();
		while let Some(e) = db_iter.prev()? {
			db_content.push(e);
		}
		assert_eq!(db_content, model_content);
	}
	Ok(())
}
