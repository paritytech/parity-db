// Copyright 2021 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.

//! BTree implementation struct and methods.
//! Allows multiple order variants or nodes in memory storage.


use super::*;
use crate::table::{ValueTable, key::TableKey};
use std::cmp::Ordering;
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::Result;
use crate::index::Address;
use crate::btree::btree::RemovedChildren;

impl Node {
	pub fn set_child_node(&mut self, at: usize, child: Box<Self>, index: Option<u64>) {
		self.set_child(at, Self::new_child(child, index))
	}

	pub fn last_separator_index(&mut self) -> Option<usize> {
		let i = self.number_separator();
		if i == 0 {
			None
		} else {
			Some(i - 1)
		}
	}

	pub fn insert(
		&mut self,
		depth: u32,
		key: &[u8],
		value: &[u8],
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed_node: &mut RemovedChildren,
	) -> Result<Option<(Separator, Child)>> {
		let has_child = depth != 0;

		let (at, i) = self.position(key)?;
		// insert
		if !at {
			if has_child {
				return Ok(if let Some(child) = self.fetch_child(i, column, log)? {
					match child.insert(depth - 1, key, value, record_id, column, log, origin, removed_node)? {
						Some((sep, right)) => {
							// insert from child
							self.insert_node(depth, i, sep, right, removed_node)?
						},
						r => r,
					}
				} else {
					None
				});
			}

			if self.has_separator(ORDER - 1) {
				// full
				let middle = ORDER / 2;
				let insert = i;
				let insert_separator = Self::create_separator(key, value, column, log, None, origin)?;

				if insert == middle {
					let (right, right_ix) = self.split(middle, true, None, None, has_child, removed_node);
					let right = Self::new_child(right, right_ix);
					return Ok(Some((insert_separator, right)));
				} else if insert < middle {
					let (right, right_ix) = self.split(middle, false, None, None, has_child, removed_node);
					let sep = self.remove_separator(middle - 1);
					self.shift_from(insert, has_child, false);
					self.set_separator(insert, insert_separator);
					let right = Self::new_child(right, right_ix);
					return Ok(Some((sep, right)));
				} else {
					let (right, right_ix) = self.split(middle + 1, false, Some((insert, insert_separator)), None, has_child, removed_node);
					let sep = self.remove_separator(middle);
					let right = Self::new_child(right, right_ix);
					return Ok(Some((sep, right)));
				}
			}

			self.shift_from(i, has_child, false);
			self.set_separator(i, Self::create_separator(key, value, column, log, None, origin)?);
		} else {
			let existing = self.separator_value_index(i);
			self.set_separator(i, Self::create_separator(key, value, column, log, existing, origin)?);
		}
		Ok(None)
	}

	pub fn insert_node(
		&mut self,
		depth: u32,
		at: usize,
		separator: Separator,
		right_child: Child,
		removed_node: &mut RemovedChildren,
	) -> Result<Option<(Separator, Child)>> {
		debug_assert!(depth != 0);
		let has_child = true;
		let child = right_child;
		if self.has_separator(ORDER - 1) {
			// full
			let middle = ORDER / 2;
			let insert = at;
			if insert == middle {
				let (mut right, right_ix) = self.split(middle, true, None, None, has_child, removed_node);
				right.set_child(0, child);
				let right = Self::new_child(right, right_ix);
				return Ok(Some((separator, right)));
			} else if insert < middle {
				let (right, right_ix) = self.split(middle, false, None, None, has_child, removed_node);
				let sep = self.remove_separator(middle - 1);
				self.shift_from(insert, has_child, false);
				self.set_separator(insert, separator);
				self.set_child(insert + 1, child);
				let right = Self::new_child(right, right_ix);
				return Ok(Some((sep, right)));
			} else {
				let (right, right_ix) = self.split(middle + 1, false, Some((insert, separator)), Some((insert, child)), has_child, removed_node);
				let sep = self.remove_separator(middle);
				let right = Self::new_child(right, right_ix);
				return Ok(Some((sep, right)));
			}
		} else {
			self.shift_from(at, has_child, false);
			self.set_separator(at, separator);
			self.set_child(at + 1, child);
			Ok(None)
		}
	}

	pub fn remove(
		&mut self,
		depth: u32,
		key: &[u8],
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed: &mut RemovedChildren,
	) -> Result<bool> {
		let has_child = depth != 0;
		let (at, i) = self.position(key)?;
		if at {
			let existing = self.separator_value_index(i);
			if let Some(existing) = existing {
				column.with_tables_and_self(|t, s| s.write_existing_value_plan(
					&TableKey::NoHash,
					t,
					Address::from_u64(existing),
					None,
					log,
					origin,
				))?;
			}
			let _ = self.remove_separator(i);
			if depth != 0 {
				// replace by bigger value in left child.
				if let Some(child) = self.fetch_child(i, column, log)? {
					let (need_balance, sep) = child.remove_last(depth - 1, record_id, column, log, origin, removed)?;
					if let Some(sep) = sep {
						self.set_separator(i, sep);
					}
					if need_balance {
						self.rebalance(depth, i, column, log, removed)?;
					}
				}
			} else {
				self.remove_from(i, false, true);
			}
		} else {
			if !has_child {
				return Ok(false);
			}
			if let Some(child) = self.fetch_child(i, column, log)? {
				let need_rebalance = child.remove(depth - 1, key, record_id, column, log, origin, removed)?;
				if need_rebalance {
					self.rebalance(depth, i, column, log, removed)?;
					return Ok(self.need_rebalance());
				} else {
					return Ok(false);
				}
			} else {
				return Ok(false);
			}
		}

		Ok(self.need_rebalance())
	}

	pub fn rebalance(
		&mut self,
		depth: u32,
		at: usize,
		column: &Column,
		log: &impl LogQuery,
		removed_nodes: &mut RemovedChildren,
	) -> Result<()> {
		let has_child = depth - 1 != 0;
		let middle = ORDER / 2;
		let mut balance_from_left = false;
		if at > 0 {
			if let Some(node) = self.fetch_child(at - 1, column, log)? {
				if node.has_separator(middle) {
					balance_from_left = true;
				}
			}
		}
		if balance_from_left {
			let mut child = None;
			let left = self.fetch_child(at - 1, column, log)?.unwrap();
			let last_sibling = left.last_separator_index().unwrap();
			if has_child {
				child = Some(left.remove_child(last_sibling + 1));
			}
			let separator2 = left.remove_separator(last_sibling);
			let separator = self.remove_separator(at - 1);
			self.set_separator(at - 1, separator2);
			let right = self.fetch_child(at, column, log)?.unwrap();
			right.shift_from(0, has_child, true);
			if let Some(child) = child {
				right.set_child(0, child);
			}
			right.set_separator(0, separator);
			return Ok(());
		}

		let number_child = self.number_separator() + 1;
		let mut balance_from_right = false;
		if at + 1 < number_child {
			if let Some(node) = self.fetch_child(at + 1, column, log)? {
				if node.has_separator(middle) {
					balance_from_right = true;
				}
			}
		}

		if balance_from_right {
			let mut child = None;
			let right = self.fetch_child(at + 1, column, log)?.unwrap();
			if has_child {
				child = Some(right.remove_child(0));
			}
			let separator2 = right.remove_separator(0);
			right.remove_from(0, has_child, true);
			let separator = self.remove_separator(at);
			self.set_separator(at, separator2);
			let left = self.fetch_child(at, column, log)?.unwrap();
			let last_left = left.number_separator();
			left.set_separator(last_left, separator);
			if let Some(child) = child {
				left.set_child(last_left + 1, child);
			}
			return Ok(());
		}

		let (at_left, at, at_right) = if at + 1 == number_child {
			(at - 1, at - 1, at)
		} else {
			(at, at, at + 1)
		};

		let separator = self.remove_separator(at);
		let left = self.fetch_child(at_left, column, log)?.unwrap();
		let mut i = left.number_separator();
		left.set_separator(i, separator);
		i += 1;
		let right = self.fetch_child(at_right, column, log)?.unwrap();
		let right_len = right.number_separator();
		let mut right_i = 0;
		while right_i < right_len {
			let mut child = None;
			let right = self.fetch_child(at_right, column, log)?.unwrap();
			if has_child {
				child = Some(right.remove_child(right_i));
			}
			let separator = right.remove_separator(right_i);
			let left = self.fetch_child(at_left, column, log)?.unwrap();
			left.set_separator(i, separator);
			if let Some(child) = child {
				left.set_child(i, child);
			}
			i += 1;
			right_i += 1;
		}
		if has_child {
			let right = self.fetch_child(at_right, column, log)?.unwrap();
			let child = right.remove_child(right_i);
			let left = self.fetch_child(at_left, column, log)?.unwrap();
			left.set_child(i, child);
		}

		let removed = self.remove_child(at_right);
		let (index, node) = Self::inner_child(removed);
		removed_nodes.push(index, node);
		let has_child = true; // rebalance on parent.
		self.remove_from(at, has_child, false);

		Ok(())
	}

	pub fn need_rebalance(&mut self) -> bool {
		let mut rebalance = false;
		let middle = ORDER / 2;
		if !self.has_separator(middle - 1) {
			rebalance = true;
		}
		rebalance
	}

	pub fn remove_last(
		&mut self,
		depth: u32,
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed: &mut RemovedChildren,
	) -> Result<(bool, Option<Separator>)> {
		let last = if let Some(last) = self.last_separator_index() {
			last
		} else {
			return Ok((false, None));
		};
		let i = last;
		if depth == 0 {
			let result = self.remove_separator(i);

			Ok((self.need_rebalance(), Some(result)))
		} else {
			if let Some(child) = self.fetch_child(i + 1, column, log)? {
				let result = child.remove_last(depth - 1, record_id, column, log, origin, removed)?;
				if result.0 {
					self.rebalance(depth, i + 1, column, log, removed)?;
					Ok((self.need_rebalance(), result.1))
				} else {
					Ok(result)
				}
			} else {
				Ok((false, None))
			}
		}
	}

	pub fn get(&mut self, key: &[u8], btree: &BTreeTable, values: &Vec<ValueTable>, log: &impl LogQuery, comp: &Compress) -> Result<Option<Address>> {
		let (at, i) = self.position(key)?;
		if at {
			Ok(self.separator_get_info(i))
		} else {
			if let Some(child) = self.fetch_child_from_btree(i, btree, log, values, comp)? {
				return child.get(key, btree, values, log, comp);
			}

			Ok(None)
		}
	}
	
	pub fn seek(from: &mut Box<Self>, key: &[u8], btree: &BTreeTable, values: &Vec<ValueTable>, log: &impl LogQuery, depth: u32, stack: &mut Vec<(usize, *mut Box<Self>)>, comp: &Compress) -> Result<bool> {
		let (at, i) = from.position(key)?;
		stack.push((i, from));
		if at {
			Ok(true)
		} else {
			if depth != 0 {
				if let Some(child) = from.fetch_child_box_from_btree(i, btree, log, values, comp)? {
					return Self::seek(child, key, btree, values, log, depth - 1, stack, comp);
				}
			}

			Ok(false)
		}
	}

	#[cfg(test)]
	pub fn is_balanced(&mut self, column: &Column, log: &impl LogQuery, parent_size: usize) -> Result<bool> {
		let size = self.number_separator();
		if parent_size != 0 && size < ORDER / 2 {
			return Ok(false);
		}

		let mut i = 0;
		while i < ORDER {
			let child = self.fetch_child(i, column, log)?;
			i += 1;
			if child.is_none() {
				continue;
			}
			if !child.unwrap().is_balanced(column, log, size)? {
				return Ok(false);
			}
		}

		Ok(true)
	}
}

/// Nodes with data loaded in memory.
/// Nodes get only serialized when flushed in the global overlay
/// (there we need one entry per record id).
pub struct Node {
	separators: [node::Separator; ORDER],
	children: [node::Child; ORDER_CHILD],
	changed: bool,
}

pub struct Separator {
	modified: bool,
	separator: Option<SeparatorInner>,
}

impl Default for Separator {
	fn default() -> Self {
		Separator {
			modified: false,
			separator: None,
		}
	}
}

pub struct SeparatorInner {
	pub key: Vec<u8>, // TODO could use range and only allocate on change. 
	pub value: u64,
}

#[derive(Clone, Default)]
pub struct ChildState {
	pub fetched: bool,
	pub modified: bool,
	pub moved: bool,
}

pub struct Child {
	state: ChildState,
	node: Option<Box<Node>>, // lazy fetch.
	entry_index: Option<u64>, // no index is new.
}
impl Default for Child {
	fn default() -> Self {
		let state = ChildState::default();
		Child { state, node: None, entry_index: None }
	}
}

impl Child {
	fn new(node: Box<Node>, entry_index: Option<u64>) -> Self {
		let mut state = ChildState::default();
		state.fetched = true;
		state.modified = true;
		state.moved = true;
		Child {
			state,
			node: Some(node),
			entry_index,
		}
	}
}

impl Node {
	pub fn new() -> Self {
		Node {
			separators: Default::default(),
			children: Default::default(),
			changed: true,
		}
	}

	pub fn clear(&mut self) {
		self.separators = Default::default();
		self.children = Default::default();
		self.changed = true;
	}

	pub fn from_encoded(enc: Vec<u8>) -> Self {
		let mut entry = Entry::from_encoded(enc);
		let mut node = Node {
			separators: Default::default(),
			children: Default::default(),
			changed: false,
		};
		let mut i_children = 0;
		let mut i_separator = 0;
		loop {
			if let Some(child_index) = entry.read_child_index() {
				node.children.as_mut()[i_children].entry_index = Some(child_index);
				
			}
			i_children += 1;
			if i_children == ORDER_CHILD {
				break;
			}
			if let Some(sep) = entry.read_separator() {
				node.separators.as_mut()[i_separator].separator = Some(sep);
				i_separator += 1
			} else {
				break;
			}
		}
		node
	}

	pub fn remove_separator(&mut self, at: usize) -> Separator {
		self.changed = true;
		let mut separator = std::mem::replace(self.get_separator(at), Separator {
			modified: true,
			separator: None,
		});
		separator.modified = true;
		separator
	}

	pub fn remove_child(&mut self, at: usize) -> Child {
		self.changed = true;
		let mut state = ChildState::default();
		state.fetched = true;
		state.moved = true;
		let mut child = std::mem::replace(self.get_child_index(at), Child {
			state,
			node: None,
			entry_index: None,
		});
		child.state.moved = true;
		child
	}

	pub fn try_forget_child(&mut self, at: usize) {
		let child = &mut self.children.as_mut()[at];
		if child.state.fetched && !child.state.modified {
			let mut state = child.state.clone();
			state.fetched = false;
			child.state = state;
			child.node = None;
		}
	}

	pub fn fetch_child_box(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		let child = self.get_fetched_child_index(at, col, log)?;
		Ok(child.node.as_mut())
	}

	pub fn fetch_child(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index(at, col, log)?;
		Ok(child.node.as_mut().map(|n| n.as_mut()))
	}

	pub fn fetch_child_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index_from_btree(at, btree, log, values, comp)?;
		Ok(child.node.as_mut().map(|n| n.as_mut()))
	}

	pub fn fetch_child_box_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<Option<&mut Box<Self>>> {
		let child = self.get_fetched_child_index_from_btree(at, btree, log, values, comp)?;
		Ok(child.node.as_mut())
	}

	pub fn has_separator(&mut self, at: usize) -> bool {
		let at = self.get_separator(at);
		at.separator.is_some()
	}

	pub fn separator_key(&mut self, at: usize) -> Option<Vec<u8>> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| {
			s.key.clone()
		})
	}

	pub fn separator_value_index(&mut self, at: usize) -> Option<u64> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| s.value)
	}

	pub fn separator_get_info(&mut self, at: usize) -> Option<Address> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| Address::from_u64(s.value))
	}
	
	pub fn set_separator(&mut self, at: usize, mut sep: Separator) {
		sep.modified = true;
		self.changed = true;
		self.separators.as_mut()[at] = sep;
	}

	pub fn new_child(mut child: Box<Self>, index: Option<u64>) -> Child {
		child.changed = true;
		Child::new(child, index)
	}

	pub fn inner_child(child: Child) -> (Option<u64>, Option<Box<Self>>) {
		(child.entry_index, child.node)
	}

	pub fn set_child(&mut self, at: usize, mut child: Child) {
		child.state.moved = true;
		self.changed = true;
		self.children.as_mut()[at] = child;
	}

	pub fn create_separator(key: &[u8], value: &[u8], column: &Column, log: &mut LogWriter, existing: Option<u64>, origin: ValueTableOrigin) -> Result<Separator> {
		let key = key.to_vec();
		let value = if let Some(address) = existing {
			column.with_tables_and_self(|t, s| s.write_existing_value_plan(
				&TableKey::NoHash,
				t,
				Address::from_u64(address),
				Some(value),
				log,
				origin,
			))?.1.map(|a| a.as_u64()).unwrap_or(address)
		} else {
			column.with_tables_and_self(|t, s| s.write_new_value_plan(
					&TableKey::NoHash,
					t,
					value,
					log,
					origin,
			))?.as_u64()
		};
		Ok(Separator {
			modified: true,
			separator: Some(SeparatorInner {
				key,
				value,
			}),
		})
	}

	pub fn split(
		&mut self,
		at: usize,
		skip_left_child: bool,
		mut insert_right: Option<(usize, Separator)>,
		mut insert_right_child: Option<(usize, Child)>,
		has_child: bool,
		removed_node: &mut RemovedChildren,
	) -> (Box<Self>, Option<u64>) {
		let (right_ix, mut right) = match removed_node.available() {
			(ix, Some(mut node)) => {
				node.clear();
				(ix, node)
			},
			(ix, None) => {
				(ix, Box::new(Self::new()))
			},
		};
		let mut offset = 0;
		let right_start = at;
		for i in right_start .. ORDER {
			let sep = self.remove_separator(i);
			if insert_right.as_ref().map(|ins| ins.0 == i).unwrap_or(false) {
				if let Some((_, sep)) = insert_right.take() {
					right.separators.as_mut()[i - right_start] = sep;
					offset = 1;
				}
			}
			right.separators.as_mut()[i + offset - right_start] = sep;
		}
		if let Some((insert, sep)) = insert_right.take() {
			debug_assert!(insert == ORDER);
			right.separators.as_mut()[insert - right_start] = sep;
		}
		let mut offset = 0;
		if has_child {
			let skip_offset = if skip_left_child { 1 } else { 0 };
			for i in right_start + skip_offset .. ORDER_CHILD {
				let child = self.remove_child(i);
				if insert_right_child.as_ref().map(|ins| ins.0 == i + 1).unwrap_or(false) {
					offset = 1;
					if let Some((_, mut child)) = insert_right_child.take() {
						child.state.moved = true;
						right.children.as_mut()[i + offset - right_start] = child;
					}
				}
				right.children.as_mut()[i + offset - right_start] = child;
			}
			if let Some((insert, mut child)) = insert_right_child.take() {
				debug_assert!(insert == ORDER);
				child.state.moved = true;
				right.children.as_mut()[insert + 1 - right_start] = child;
			}
		}
		(right, right_ix)
	}

	pub fn shift_from(&mut self, from: usize, has_child: bool, with_left_child: bool) {
		self.changed = true;
		let mut i = from;
		let mut current = self.remove_separator(i);
		while current.separator.is_some() {
			current.modified = true;
			i += 1;
			if i == ORDER {
				break;
			}
			let mut next = self.get_separator(i);
			current = std::mem::replace(&mut next, current);
		}
		if has_child {
			let mut i = if with_left_child {
				from
			} else {
				from + 1
			};
			let mut current = self.remove_child(i);
			while current.entry_index.is_some() || current.node.is_some() {
				current.state.moved = true;
				i += 1;
				if i == ORDER_CHILD {
					break;
				}
				let mut next = self.get_child_index(i);
				current = std::mem::replace(&mut next, current);
			}
		}
	}

	pub fn remove_from(&mut self, from: usize, has_child: bool, with_left_child: bool) {
		let mut i = from;
		self.changed = true;
		while i < ORDER - 1 {
			self.separators.as_mut()[i] = self.remove_separator(i + 1);
			self.separators.as_mut()[i].modified = true;
			if self.separators.as_mut()[i].separator.is_none() {
				break;
			}
			i += 1;
		}
		if has_child {
			let mut i = if with_left_child {
				from
			} else {
				from + 1
			};
			while i < ORDER_CHILD - 1 {
				self.children.as_mut()[i] = self.remove_child(i + 1);
				if self.children.as_mut()[i].entry_index.is_none() && self.children.as_mut()[i].node.is_none() {
					break;
				}
				i += 1;
			}
		}
	}

	pub fn number_separator(&mut self) -> usize {
		let mut i = 0;
		while self.get_separator(i).separator.is_some() {
			i += 1;
			if i == ORDER {
				break;
			}
		}
		i
	}

	// Return true if match and matched position.
	// Return index of first element bigger than key otherwhise.
	pub fn position(&mut self, key: &[u8]) -> Result<(bool, usize)> {
		let mut i = 0;
		while let Some(separator) = self.get_separator(i).separator.as_mut() {
			match key[..].cmp(&separator.key[..]) {
				Ordering::Greater => (),
				Ordering::Less => {
					return Ok((false, i));
				},
				Ordering::Equal => {
					return Ok((true, i));
				},
			}
			i += 1;
			if i == ORDER {
				break;
			}
		}
		Ok((false, i))
	}

	// TODO default traitify
	pub fn write_plan(&mut self, column: &Column, writer: &mut LogWriter, node_id: Option<u64>, table_id: BTreeTableId, btree: &mut BTreeIndex, record_id: u64, origin: ValueTableOrigin) -> Result<Option<u64>> {
		// TODO currently modified is not set properly so we iterate all, could have a child modified
		// (but with current use it is only interesting for delete on non existing value)

		for child in self.children.as_mut().iter_mut() {
			// TODO child modified is not handled properly (in case of recursive it needs parent and 
			// other, for now, if fetched, then it is modified).
			// let child_modified = child.modified;
			let child_modified = true;
			if child_modified {
				if let Some(node) = child.node.as_mut() {
					if let Some(index) = node.write_plan(column, writer, child.entry_index, table_id, btree, record_id, origin)? {
						child.entry_index = Some(index);
						self.changed = true;
					} else {
						if child.state.moved {
							self.changed = true;
						}
					}
				} else {
					if child.state.moved || child.state.modified {
						self.changed = true;
					}
				}
			}
		}

		for separator in self.separators.as_mut().iter_mut() {
			if separator.modified {
				self.changed = true;
			}
		}

		if !self.changed {
			return Ok(None);
		}



		let mut entry = Entry::empty();
		let mut i_children = 0;
		let mut i_separator = 0;
		loop {
			if let Some(index) = self.children.as_mut()[i_children].entry_index {
				entry.write_child_index(index);
				i_children += 1
			} else {
				entry.write_child_index(0);
				i_children += 1
			}
			if i_children == ORDER_CHILD {
				break;
			}
			if let Some(sep) = &self.separators.as_mut()[i_separator].separator {
				entry.write_separator(&sep.key, sep.value);
				i_separator += 1
			} else {
				break;
			}
		}
	
		let mut result = None;
		if let Some(existing) = node_id {
			let k = TableKey::NoHash;
			if let (_, Some(new_index)) = column.with_tables_and_self(|tables, s| s.write_existing_value_plan(
				&k,
				tables,
				Address::from_u64(existing),
				Some(entry.encoded.as_ref()),
				writer,
				origin,
			))? {
				result = Some(new_index.as_u64())
			}
		} else {
			let k = TableKey::NoHash;
			result = Some(column.with_tables_and_self(|t, s| s.write_new_value_plan(
				&k,
				t,
				entry.encoded.as_ref(),
				writer,
				origin,
			))?.as_u64());
		}

		Ok(result)
	}

	fn get_separator(&mut self, at: usize) -> &mut Separator {
		&mut self.separators.as_mut()[at]
	}

	fn get_child_index(&mut self, at: usize) -> &mut Child {
		&mut self.children.as_mut()[at]
	}

	fn get_fetched_child_index(&mut self, i: usize, column: &Column, log: &impl LogQuery) -> Result<&mut Child> {
		let mut child = self.get_child_index(i);
		if !child.state.fetched {
			if let Some(ix) = child.entry_index {
				child.state.fetched = true;
				let entry = column.with_value_tables_and_btree(|btree, values, comp| btree.get_index(ix, log, values, comp))?;
				child.node = Some(Box::new(Self::from_encoded(entry)));
			}
		}
		Ok(child)
	}

	// TODO merge with get_fetched_child_index
	fn get_fetched_child_index_from_btree(&mut self, i: usize, btree: &BTreeTable, log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<&mut Child> {
		let mut child = self.get_child_index(i);
		if !child.state.fetched {
			if let Some(ix) = child.entry_index {
				child.state.fetched = true;
				let entry = btree.get_index(ix, log, values, comp)?;
				child.node = Some(Box::new(Self::from_encoded(entry)));
			}
		}
		Ok(child)
	}
}
