// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check without reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use std::collections::btree_map::BTreeMap;
use tempfile::tempdir;

#[derive(Arbitrary, Debug)]
enum Action {
	Transaction(Vec<(u8, Option<u8>)>),
	Restart,
}

#[derive(Arbitrary, Debug, Clone, Copy)]
enum CompressionType {
	NoCompression,
	Snappy,
	Lz4,
}

#[derive(Arbitrary, Debug)]
struct Config {
	btree_index: bool,
	compression: CompressionType,
}

fuzz_target!(|entry: (Config, Vec<Action>)| {
	let (config, actions) = entry;
	let dir = tempdir().unwrap();
	let options = parity_db::Options {
		path: dir.path().to_owned(),
		columns: vec![parity_db::ColumnOptions {
			compression: match config.compression {
				CompressionType::NoCompression => parity_db::CompressionType::NoCompression,
				CompressionType::Snappy => parity_db::CompressionType::Snappy,
				CompressionType::Lz4 => parity_db::CompressionType::Lz4,
			},
			btree_index: config.btree_index,
			..parity_db::ColumnOptions::default()
		}],
		sync_wal: true,
		sync_data: true,
		stats: false,
		salt: None,
	};
	let mut db = parity_db::Db::open_or_create(&options).unwrap();
	let mut model = BTreeMap::<u8, u8>::default();
	for action in actions {
		// We apply the action on both the database and the model
		match action {
			Action::Transaction(operations) => {
				db.commit(
					operations.iter().copied().map(|(k, v)| (0u8, vec![k], v.map(|v| vec![v]))),
				)
				.unwrap();
				for (k, v) in operations {
					if let Some(v) = v {
						model.insert(k, v);
					} else {
						model.remove(&k);
					}
				}
			},
			Action::Restart =>
				db = {
					drop(db);
					parity_db::Db::open_or_create(&options).unwrap()
				},
		}

		// We check the state
		for (k, v) in &model {
			assert_eq!(db.get(0, &[*k]).unwrap(), Some(vec![*v]));
		}

		if config.btree_index {
			// We check the BTree forward iterator
			let mut model_content = model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
			let mut db_iter = db.iter(0).unwrap();
			db_iter.seek_to_first().unwrap();
			let mut db_content = Vec::new();
			while let Some((k, v)) = db_iter.next().unwrap() {
				db_content.push((k[0], v[0]));
			}
			assert_eq!(db_content, model_content);

			// We check the BTree backward iterator
			model_content.reverse();
			db_iter.seek_to_last().unwrap();
			let mut db_content = Vec::new();
			while let Some((k, v)) = db_iter.prev().unwrap() {
				db_content.push((k[0], v[0]));
			}
			assert_eq!(db_content, model_content);
		}
	}
});
