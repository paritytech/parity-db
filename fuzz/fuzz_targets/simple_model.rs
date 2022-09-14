// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check without reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::collections::{btree_map::BTreeMap, HashMap};
use tempfile::tempdir;

#[derive(Arbitrary, Debug)]
enum Action {
	Transaction(Vec<(u8, Option<u8>)>),
	Restart,
}

fuzz_target!(|entry: (Config, Vec<Action>)| {
	let (config, actions) = entry;
	let dir = tempdir().unwrap();
	let options = parity_db::Options {
		path: dir.path().to_owned(),
		columns: vec![parity_db::ColumnOptions {
			compression: config.compression.into(),
			btree_index: config.btree_index,
			..parity_db::ColumnOptions::default()
		}],
		sync_wal: true,
		sync_data: true,
		stats: false,
		salt: None,
		compression_threshold: HashMap::new(),
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

		assert_db_and_model_are_equals(
			&db,
			model.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
			config.btree_index,
		);
	}
});
