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

	fn is_layer_state_compatible_with_disk_state(
		layer_values: &[Option<usize>; NUMBER_OF_POSSIBLE_KEYS],
		state: &[(u8, u8)],
	) -> bool {
		if !state.iter().all(|(k, v)| k == v) {
			return false // keys and values should be equal
		}
		layer_values.iter().enumerate().all(|(i, c)| {
			let key = i as u8;
			match c {
				None => state.iter().all(|(k, _)| *k != key),
				Some(0) => true,
				Some(_) => state.iter().any(|(k, _)| *k == key),
			}
		})
	}

	fn build_best_layer_for_recovery(layers: &[&Layer<usize>]) -> Layer<usize> {
		// if we are multiple candidates, we are unsure. We pick the lower count per candidate
		let mut new_state_safe_counts = [None; NUMBER_OF_POSSIBLE_KEYS];
		for layer in layers {
			for i in u8::MIN..=u8::MAX {
				if let Some(c) = layer.values[usize::from(i)] {
					new_state_safe_counts[usize::from(i)] =
						Some(min(c, new_state_safe_counts[usize::from(i)].unwrap_or(usize::MAX)));
				}
			}
		}
		Layer { values: new_state_safe_counts, written: WrittenState::Yes }
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

	fn layer_optional_content(
		values: &[Option<usize>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)> {
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
