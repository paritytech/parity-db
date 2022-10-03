// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check with reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::{collections::HashMap, path::Path};

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Operation {
	Set(u8),
	Dereference(u8),
	Reference(u8),
}

#[derive(Clone, Debug)]
struct Layer {
	counts: [Option<i64>; 256],
	is_maybe_saved: bool,
}

type Model = Vec<Layer>;

struct Simulator;

impl DbSimulator for Simulator {
	type Operation = Operation;
	type Model = Model;

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
			always_flush: true,
			with_background_thread: false,
		}
	}

	fn apply_operations_on_model<'a>(
		operations: impl IntoIterator<Item = &'a Operation>,
		model: &mut Model,
	) {
		let mut counts = [None; 256];
		for operation in operations {
			match *operation {
				Operation::Set(k) => {
					let count = count_for_key(k.into(), model).unwrap_or(0) +
						counts[usize::from(k)].unwrap_or(0);
					if count == 0 {
						*counts[usize::from(k)].get_or_insert(0) += 1
					}
				},
				Operation::Dereference(k) => {
					let count = count_for_key(k.into(), model).unwrap_or(0) +
						counts[usize::from(k)].unwrap_or(0);
					if count > 0 {
						*counts[usize::from(k)].get_or_insert(0) -= 1;
					}
				},
				Operation::Reference(k) => {
					let count = count_for_key(k.into(), model).unwrap_or(0) +
						counts[usize::from(k)].unwrap_or(0);
					if count > 0 {
						*counts[usize::from(k)].get_or_insert(0) += 1;
					}
				},
			}
		}

		model.push(Layer { counts, is_maybe_saved: false });
	}

	fn write_first_layer_to_disk(model: &mut Model) {
		for layer in model {
			if !layer.is_maybe_saved {
				layer.is_maybe_saved = true;
				break
			}
		}
	}

	fn attempt_to_reset_model_to_disk_state(model: &Model, state: &[(u8, u8)]) -> Option<Model> {
		let mut model = model.clone();
		let mut expected = [false; 256];
		for (k, _) in state {
			expected[usize::from(*k)] = true;
		}

		while !model.is_empty() {
			if !model.last().unwrap().is_maybe_saved {
				model.pop();
				continue
			}

			// Is it equal to current state?
			let is_equal = expected.iter().enumerate().all(|(k, is_present)| {
				let c = count_for_key(k, &model);
				if *is_present {
					c.is_some()
				} else {
					c.unwrap_or(0) == 0
				}
			});
			if is_equal {
				// We found it!
				return Some(model)
			}
			model.pop();
		}
		Some(model)
	}

	fn map_operation(operation: &Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		match *operation {
			Operation::Set(k) => parity_db::Operation::Set(vec![k], vec![k]),
			Operation::Dereference(k) => parity_db::Operation::Dereference(vec![k]),
			Operation::Reference(k) => parity_db::Operation::Reference(vec![k]),
		}
	}

	fn model_required_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		(0..=255)
			.filter_map(|k| {
				let key = usize::from(k);
				let mut min_count = 0;
				for layer in model {
					if let Some(c) = layer.counts[key] {
						if !layer.is_maybe_saved && c < 0 {
							min_count += c;
						}
					}
				}
				(min_count > 0).then(|| (vec![k], vec![k]))
			})
			.collect()
	}

	fn model_optional_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		(0..=255)
			.filter_map(|k| {
				let key = usize::from(k);
				model
					.iter()
					.any(|layer| layer.counts[key].is_some())
					.then(|| (vec![k], vec![k]))
			})
			.collect()
	}

	fn model_removed_content(_model: &Model) -> Vec<Vec<u8>> {
		Vec::new()
	}
}

fn count_for_key(key: usize, layers: &[Layer]) -> Option<i64> {
	let mut count = None;
	for layer in layers {
		if let Some(c) = layer.counts[key] {
			*count.get_or_insert(0) += c;
		}
	}
	count
}

fuzz_target!(|entry: (Config, Vec<Action<Operation>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions);
});
