// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check with reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::{cmp::min, collections::HashMap, path::Path};

const NUMBER_OF_POSSIBLE_KEYS: usize = 256;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Operation {
	Set(u8),
	Dereference(u8),
	Reference(u8),
}

#[derive(Clone, Debug)]
struct Layer {
	counts: [Option<usize>; NUMBER_OF_POSSIBLE_KEYS], /* The number of references per key (or
	                                                   * None
	                                                   * if the key never existed in the
	                                                   * database) */
	written: bool,
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
		let mut counts = model.last().map_or([None; NUMBER_OF_POSSIBLE_KEYS], |l| l.counts);
		for operation in operations {
			match *operation {
				Operation::Set(k) => *counts[usize::from(k)].get_or_insert(0) += 1,
				Operation::Dereference(k) =>
					if counts[usize::from(k)].unwrap_or(0) > 0 {
						*counts[usize::from(k)].get_or_insert(0) -= 1;
					},
				Operation::Reference(k) =>
					if counts[usize::from(k)].unwrap_or(0) > 0 {
						*counts[usize::from(k)].get_or_insert(0) += 1;
					},
			}
		}

		model.push(Layer { counts, written: false });
	}

	fn write_first_layer_to_disk(model: &mut Model) {
		for layer in model {
			if !layer.written {
				layer.written = true;
				break
			}
		}
	}

	fn attempt_to_reset_model_to_disk_state(model: &Model, state: &[(u8, u8)]) -> Option<Model> {
		let expected = {
			let mut is_present = [false; NUMBER_OF_POSSIBLE_KEYS];
			for (k, _) in state {
				is_present[usize::from(*k)] = true;
			}
			is_present
		};

		let mut candidates = Vec::new();
		for layer in model.iter().rev() {
			if !layer.written {
				continue
			}

			// Is it equal to current state?
			let is_equal = expected.iter().enumerate().all(|(k, is_present)| {
				if *is_present {
					layer.counts[k].is_some()
				} else {
					layer.counts[k].unwrap_or(0) == 0
				}
			});
			if is_equal {
				// We found a correct last layer
				candidates.push(layer);
			}
		}
		if candidates.is_empty() {
			return if state.is_empty() { Some(Vec::new()) } else { None }
		}

		// if we are multiple candidates, we are unsure. We pick the lower count per candidate
		let mut new_state_safe_counts = [None; NUMBER_OF_POSSIBLE_KEYS];
		for layer in candidates {
			for i in u8::MIN..=u8::MAX {
				if let Some(c) = layer.counts[usize::from(i)] {
					new_state_safe_counts[usize::from(i)] =
						Some(min(c, new_state_safe_counts[usize::from(i)].unwrap_or(usize::MAX)));
				}
			}
		}
		Some(vec![Layer { counts: new_state_safe_counts, written: true }])
	}

	fn map_operation(operation: &Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		match *operation {
			Operation::Set(k) => parity_db::Operation::Set(vec![k], vec![k]),
			Operation::Dereference(k) => parity_db::Operation::Dereference(vec![k]),
			Operation::Reference(k) => parity_db::Operation::Reference(vec![k]),
		}
	}

	fn model_required_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		if let Some(last) = model.last() {
			last.counts
				.iter()
				.enumerate()
				.filter_map(|(k, count)| {
					if count.unwrap_or(0) > 0 {
						Some((vec![k as u8], vec![k as u8]))
					} else {
						None
					}
				})
				.collect()
		} else {
			Vec::new()
		}
	}

	fn model_optional_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		if let Some(last) = model.last() {
			last.counts
				.iter()
				.enumerate()
				.filter_map(|(k, count)| {
					if count.is_some() {
						Some((vec![k as u8], vec![k as u8]))
					} else {
						None
					}
				})
				.collect()
		} else {
			Vec::new()
		}
	}

	fn model_removed_content(_model: &Model) -> Vec<Vec<u8>> {
		Vec::new()
	}
}

fuzz_target!(|entry: (Config, Vec<Action<Operation>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions);
});
