// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check without reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;
struct Simulator;

impl DbSimulator for Simulator {
	type ValueType = u8;
	type Operation = (u8, Option<u8>);

	fn build_column_options(config: &Config) -> parity_db::ColumnOptions {
		parity_db::ColumnOptions {
			compression: config.compression.into(),
			btree_index: config.btree_index,
			..parity_db::ColumnOptions::default()
		}
	}

	fn apply_operations_on_values<'a>(
		operations: impl IntoIterator<Item = &'a Self::Operation>,
		values: &mut [Option<u8>; NUMBER_OF_POSSIBLE_KEYS],
	) {
		for (k, v) in operations {
			values[usize::from(*k)] = *v;
		}
	}

	fn is_layer_state_compatible_with_disk_state(
		layer_values: &[Option<u8>; NUMBER_OF_POSSIBLE_KEYS],
		state: &[(u8, u8)],
	) -> bool {
		layer_values.iter().enumerate().all(|(i, value)| {
			let key = i as u8;
			if let Some(value) = value {
				state.iter().any(|(k, v)| *k == key && v == value)
			} else {
				state.iter().all(|(k, _)| *k != key)
			}
		})
	}

	fn build_best_layer_for_recovery(layers: &[&Layer<u8>]) -> Layer<u8> {
		let mut layer = layers[0].clone();
		layer.written = WrittenState::Yes;
		layer
	}

	fn map_operation(operation: &(u8, Option<u8>)) -> parity_db::Operation<Vec<u8>, Vec<u8>> {
		let (k, v) = operation;
		if let Some(v) = *v {
			parity_db::Operation::Set(vec![*k], vec![v])
		} else {
			parity_db::Operation::Dereference(vec![*k])
		}
	}

	fn layer_required_content(
		values: &[Option<u8>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		values
			.iter()
			.enumerate()
			.filter_map(|(i, v)| v.map(|v| (vec![i as u8], vec![v])))
			.collect()
	}

	fn layer_optional_content(
		values: &[Option<u8>; NUMBER_OF_POSSIBLE_KEYS],
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		Self::layer_required_content(values)
	}

	fn layer_removed_content(values: &[Option<u8>; NUMBER_OF_POSSIBLE_KEYS]) -> Vec<Vec<u8>> {
		values
			.iter()
			.enumerate()
			.filter_map(|(i, v)| if v.is_none() { Some(vec![i as u8]) } else { None })
			.collect()
	}
}

type Actions = Vec<Action<(u8, Option<u8>)>>;
fuzz_target!(|entry: (Config, Actions)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions);
});
