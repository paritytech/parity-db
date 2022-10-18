// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check with reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
use std::cmp::min;

#[derive(Arbitrary, Debug, Clone, Copy)]
enum Operation {
	Set(u8),
	Dereference(u8),
	Reference(u8),
}

struct Simulator;

impl DbSimulator for Simulator {
	type ValueType = usize;
	type Operation = Operation;

	fn build_column_options(config: &Config) -> parity_db::ColumnOptions {
		parity_db::ColumnOptions {
			ref_counted: true,
			preimage: true,
			compression: config.compression.into(),
			btree_index: config.btree_index,
			..parity_db::ColumnOptions::default()
		}
	}

	fn apply_operations_on_values<'a>(
		operations: impl IntoIterator<Item = &'a Self::Operation>,
		values: &mut [Option<usize>; NUMBER_OF_POSSIBLE_KEYS],
	) {
		for operation in operations {
			match *operation {
				Operation::Set(k) => *values[usize::from(k)].get_or_insert(0) += 1,
				Operation::Dereference(k) =>
					if values[usize::from(k)].unwrap_or(0) > 0 {
						*values[usize::from(k)].get_or_insert(0) -= 1;
					},
				Operation::Reference(k) =>
					if values[usize::from(k)].unwrap_or(0) > 0 {
						*values[usize::from(k)].get_or_insert(0) += 1;
					},
			}
		}
	}

	fn attempt_to_reset_model_to_disk_state(layers: &[Layer<usize>], state: &[(u8, u8)]) -> Option<Vec<Layer<usize>>> {
		let expected = {
			let mut is_present = [false; NUMBER_OF_POSSIBLE_KEYS];
			for (k, _) in state {
				is_present[usize::from(*k)] = true;
			}
			is_present
		};

		let mut candidates = Vec::new();
		for layer in layers.iter().rev() {
			if !layer.written {
				continue
			}

			// Is it equal to current state?
			let is_equal = expected.iter().enumerate().all(|(k, is_present)| {
				if *is_present {
					layer.values[k].is_some()
				} else {
					layer.values[k].unwrap_or(0) == 0
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
				if let Some(c) = layer.values[usize::from(i)] {
					new_state_safe_counts[usize::from(i)] =
						Some(min(c, new_state_safe_counts[usize::from(i)].unwrap_or(usize::MAX)));
				}
			}
		}
		Some(vec![Layer { values: new_state_safe_counts, written: true }])
	}

	fn map_operation(operation: &Operation) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		match *operation {
			Operation::Set(k) => parity_db::Operation::Set(vec![k], vec![k]),
			Operation::Dereference(k) => parity_db::Operation::Dereference(vec![k]),
			Operation::Reference(k) => parity_db::Operation::Reference(vec![k]),
		}
	}

	fn layer_required_content(
		values: &[Option<usize>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		values
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
	}

	fn layer_optional_content(values: &[Option<usize>; NUMBER_OF_POSSIBLE_KEYS]) -> Vec<(Vec<u8>, Vec<u8>)> {
		values
			.iter()
			.enumerate()
			.filter_map(
				|(k, count)| {
					if count.is_some() {
						Some((vec![k as u8], vec![k as u8]))
					} else {
						None
					}
				},
			)
			.collect()
	}

	fn layer_removed_content(values: &[Option<usize>; NUMBER_OF_POSSIBLE_KEYS]) -> Vec<Vec<u8>> {
		values
			.iter()
			.enumerate()
			.filter_map(|(k, count)| if count.is_some() { None } else { Some(vec![k as u8]) })
			.collect()
	}
}

fuzz_target!(|entry: (Config, Vec<Action<Operation>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions);
});
