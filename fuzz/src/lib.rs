// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use arbitrary::Arbitrary;
use parity_db;

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

pub fn assert_db_and_model_are_equals(
	db: &parity_db::Db,
	mut model_content: Vec<(u8, u8)>,
	is_db_b_tree: bool,
) {
	for (k, v) in &model_content {
		assert_eq!(db.get(0, &[*k]).unwrap(), Some(vec![*v]));
	}

	if is_db_b_tree {
		// We check the BTree forward iterator
		let mut db_iter = db.iter(0).unwrap();
		db_iter.seek_to_first().unwrap();
		let mut db_content = Vec::new();
		while let Some((k, v)) = db_iter.next().unwrap() {
			db_content.push((k[0], v[0]));
		}
		assert_eq!(db_content, model_content);

		// We check the BTree backward iterator
		model_content.reverse();
		let mut db_iter = db.iter(0).unwrap();
		db_iter.seek_to_last().unwrap();
		let mut db_content = Vec::new();
		while let Some((k, v)) = db_iter.prev().unwrap() {
			db_content.push((k[0], v[0]));
		}
		assert_eq!(db_content, model_content);
	}
}
