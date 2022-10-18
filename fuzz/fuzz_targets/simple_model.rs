// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Model check without reference counting:
//! Checks that a sequence of operations and restarts behaves the same as an in-memory collection.

#![no_main]
use libfuzzer_sys::fuzz_target;
use parity_db_fuzz::*;

const NUMBER_OF_POSSIBLE_KEYS: usize = 256;

#[derive(Clone, Debug)]
struct Layer {
	// For each key the value if it is inserted or None if it is removed
	values: [Option<u8>; NUMBER_OF_POSSIBLE_KEYS],
	written: bool,
}

type Model = Vec<Layer>;

struct Simulator;

impl DbSimulator for Simulator {
	type Operation = (u8, Option<u8>);
	type Model = Model;

	fn build_column_options(config: &Config) -> parity_db::ColumnOptions {
		parity_db::ColumnOptions {
			compression: config.compression.into(),
			btree_index: config.btree_index,
			..parity_db::ColumnOptions::default()
		}
	}

	fn apply_operations_on_model<'a>(
		operations: impl IntoIterator<Item = &'a (u8, Option<u8>)>,
		model: &mut Model,
	) {
		let mut values = model.last().map_or([None; NUMBER_OF_POSSIBLE_KEYS], |l| l.values);
		for (k, v) in operations {
			values[usize::from(*k)] = *v;
		}
		model.push(Layer { values, written: false });
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
			let mut values = [None; NUMBER_OF_POSSIBLE_KEYS];
			for (k, v) in state {
				values[usize::from(*k)] = Some(*v);
			}
			values
		};

		for layer in model.iter().rev() {
			if !layer.written {
				continue
			}

			// Is it equal to current state?
			if layer.values == expected {
				// We found a correct last layer
				return Some(vec![layer.clone()])
			}
		}
		if state.is_empty() {
			Some(Vec::new())
		} else {
			None
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

	fn model_required_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		if let Some(last) = model.last() {
			last.values
				.iter()
				.enumerate()
				.filter_map(|(i, v)| v.map(|v| (vec![i as u8], vec![v])))
				.collect()
		} else {
			Vec::new()
		}
	}

	fn model_optional_content(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
		Self::model_required_content(model)
	}

	fn model_removed_content(model: &Model) -> Vec<Vec<u8>> {
		if let Some(last) = model.last() {
			last.values
				.iter()
				.enumerate()
				.filter_map(|(i, v)| if v.is_none() { Some(vec![i as u8]) } else { None })
				.collect()
		} else {
			(u8::MIN..=u8::MAX).map(|k| vec![k]).collect()
		}
	}
}

fuzz_target!(|entry: (Config, Vec<Action<(u8, Option<u8>)>>)| {
	let (config, actions) = entry;
	Simulator::simulate(config, actions);
});
