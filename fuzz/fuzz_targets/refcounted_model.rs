// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check with reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::{
	collections::{btree_map::Entry, BTreeMap, HashMap},
	path::Path,
};

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Operation {
	Set(u8),
	Dereference(u8),
	Reference(u8),
}

struct Simulator;

impl DbSimulator for Simulator {
	type Operation = Operation;
	type Model = BTreeMap<u8, u8>;

	fn build_options(config: &Config, path: &Path) -> parity_db::Options {
		parity_db::Options {
			path: path.to_owned(),
			columns: vec![parity_db::ColumnOptions {
				ref_counted: true,
				preimage: true,
				compression: config.compression.into(),
				btree_index: config.btree_index,
				..parity_db::ColumnOptions::default()
			}],
			sync_wal: true,
			sync_data: true,
			stats: false,
			salt: None,
			compression_threshold: HashMap::new(),
		}
	}

	fn apply_operation_on_model(operation: &Operation, model: &mut Self::Model) {
		match *operation {
			Operation::Set(k) => {
				*model.entry(k).or_default() += 1;
			},
			Operation::Dereference(k) =>
				if let Entry::Occupied(mut e) = model.entry(k) {
					let counter = e.get_mut();
					*counter -= 1;
					if *counter == 0 {
						e.remove_entry();
					}
				},
			Operation::Reference(k) =>
				if let Entry::Occupied(mut e) = model.entry(k) {
					*e.get_mut() += 1;
				},
		}
	}

	fn map_operation(operation: &Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		match *operation {
			Operation::Set(k) => parity_db::Operation::Set(vec![k], vec![k]),
			Operation::Dereference(k) => parity_db::Operation::Dereference(vec![k]),
			Operation::Reference(k) => parity_db::Operation::Reference(vec![k]),
		}
	}

	fn model_content(model: &BTreeMap<u8, u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
		model.iter().map(|(k, _)| (vec![*k], vec![*k])).collect::<Vec<_>>()
	}
}

fuzz_target!(|entry: (Config, Vec<Action<Operation>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions).unwrap();
});
