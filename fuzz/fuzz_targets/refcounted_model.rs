// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check with reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::collections::{
	btree_map::{BTreeMap, Entry},
	HashMap,
};
use tempfile::tempdir;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Operation {
	Set(u8, u8),
	Dereference(u8),
	Reference(u8),
}

#[derive(Arbitrary, Debug)]
enum Action {
	Transaction(Vec<Operation>),
	Restart,
}

fuzz_target!(|entry: (Config, Vec<Action>)| {
	let (config, actions) = entry;
	let dir = tempdir().unwrap();
	let options = parity_db::Options {
		path: dir.path().to_owned(),
		columns: vec![parity_db::ColumnOptions {
			ref_counted: true,
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
	let mut model = BTreeMap::<u8, (usize, u8)>::default();
	for action in actions {
		// We apply the action on both the database and the model
		match action {
			Action::Transaction(operations) => {
				db.commit_changes(operations.iter().copied().map(|op| {
					(
						0u8,
						match op {
							Operation::Set(k, v) => parity_db::Operation::Set(vec![k], vec![v]),
							Operation::Dereference(k) => parity_db::Operation::Dereference(vec![k]),
							Operation::Reference(k) => parity_db::Operation::Reference(vec![k]),
						},
					)
				}))
				.unwrap();
				for op in operations {
					match op {
						Operation::Set(k, v) => {
							model.insert(k, (1, v));
						},
						Operation::Dereference(k) => {
							if let Entry::Occupied(mut e) = model.entry(k) {
								let (mut counter, _) = e.get_mut();
								counter -= 1;
								if counter == 0 {
									e.remove_entry();
								}
							}
						},
						Operation::Reference(k) =>
							if let Some((counter, _)) = model.get_mut(&k) {
								*counter += 1;
							},
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
			model.iter().map(|(k, (_, v))| (*k, *v)).collect::<Vec<_>>(),
			config.btree_index,
		);
	}
});
