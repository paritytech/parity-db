// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check without reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::{
	collections::{BTreeMap, HashMap},
	path::Path,
};

struct Simulator;

impl DbSimulator for Simulator {
	type Operation = (u8, Option<u8>);
	type Model = BTreeMap<u8, u8>;

	fn build_options(config: &Config, path: &Path) -> parity_db::Options {
		parity_db::Options {
			path: path.to_owned(),
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
		}
	}

	fn apply_operation_on_model(operation: &(u8, Option<u8>), model: &mut Self::Model) {
		let (k, v) = operation;
		if let Some(v) = *v {
			model.insert(*k, v);
		} else {
			model.remove(k);
		}
	}

	fn map_operation(operation: &(u8, Option<u8>)) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		let (k, v) = operation;
		if let Some(v) = *v {
			parity_db::Operation::Set(vec![*k], vec![v])
		} else {
			parity_db::Operation::Dereference(vec![*k])
		}
	}

	fn model_content(model: &BTreeMap<u8, u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
		model.iter().map(|(k, v)| (vec![*k], vec![*v])).collect::<Vec<_>>()
	}
}

fuzz_target!(|entry: (Config, Vec<Action<(u8, Option<u8>)>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions).unwrap();
});
