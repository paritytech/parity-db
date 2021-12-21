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
use crate::table::ValueTable;
use std::cmp::Ordering;
use crate::table::key::{NoHash, VarKey};
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::Result;
use crate::index::Address;
use crate::btree::btree::RemovedChildren;


pub trait NodeT: Sized {
	type Config: Config;
	type Separator;
	type Child;

	fn new() -> Self;
	fn clear(&mut self);
	fn new_child(child: Box<Self>, index: Option<u64>) -> Self::Child;
	fn inner_child(child: Self::Child) -> (Option<u64>, Option<Box<Self>>);
	fn from_encoded(enc: <Self::Config as Config>::Encoded) -> Self;
	fn remove_separator(&mut self, at: usize) -> Self::Separator;
	fn has_separator(&mut self, at: usize) -> bool;
	fn separator_partial_key(&mut self, at: usize) -> Option<Vec<u8>>;
	fn separator_value_index(&mut self, at: usize) -> Option<u64>;
	fn separator_get_info(&mut self, at: usize) -> Option<(Address, bool)>;
	fn set_separator(&mut self, at: usize, sep: Self::Separator); // TODO a replace separator?
	fn create_separator(key: &[u8], value: &[u8], column: &Column, log: &mut LogWriter, existing: Option<u64>, origin: ValueTableOrigin) -> Result<Self::Separator>;
	fn remove_child(&mut self, at: usize) -> Self::Child;
	fn fetch_child(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Self>>;
	fn fetch_child_box(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>>;
	fn fetch_child_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Self>>;
	fn fetch_child_box_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>>;
	fn try_forget_child(&mut self, at: usize);
	fn set_child_node(&mut self, at: usize, child: Box<Self>, index: Option<u64>) {
		self.set_child(at, Self::new_child(child, index))
	}
	fn set_child(&mut self, at: usize, child: Self::Child);
	fn split(
		&mut self,
		at: usize,
		skip_left_child: bool,
		insert_right: Option<(usize, Self::Separator)>,
		insert_right_child: Option<(usize, Self::Child)>,
		has_child: bool,
		removed_node: &mut RemovedChildren<Self>,
	) -> (Box<Self>, Option<u64>);
	fn shift_from(&mut self, from: usize, has_child: bool, with_left_child: bool);
	fn remove_from(&mut self, from: usize, has_child: bool, with_left_child: bool);
	fn position(&mut self, key: &[u8], values: &Vec<ValueTable>, log: &impl LogQuery) -> Result<(bool, usize)>;
	fn number_separator(&mut self) -> usize;
	fn last_separator_index(&mut self) -> Option<usize> {
		let i = self.number_separator();
		if i == 0 {
			None
		} else {
			Some(i - 1)
		}
	}
	fn write_plan(&mut self, column: &Column, writer: &mut LogWriter, node_id: Option<u64>, table_id: BTreeTableId, btree: &mut BTreeIndex, record_id: u64) -> Result<Option<u64>>;

	fn insert(
		&mut self,
		depth: u32,
		key: &[u8],
		value: &[u8],
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed_node: &mut RemovedChildren<Self>,
	) -> Result<Option<(Self::Separator, Self::Child)>> {
		let has_child = depth != 0;

		let (at, i) = column.with_value_tables(|t| self.position(key, t, log))?;
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

			if self.has_separator(Self::Config::ORDER - 1) {
				// full
				let middle = Self::Config::ORDER / 2;
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

	fn insert_node(
		&mut self,
		depth: u32,
		at: usize,
		separator: Self::Separator,
		right_child: Self::Child,
		removed_node: &mut RemovedChildren<Self>,
	) -> Result<Option<(Self::Separator, Self::Child)>> {
		debug_assert!(depth != 0);
		let has_child = true;
		let child = right_child;
		if self.has_separator(Self::Config::ORDER - 1) {
			// full
			let middle = Self::Config::ORDER / 2;
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

	fn remove(
		&mut self,
		depth: u32,
		key: &[u8],
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed: &mut RemovedChildren<Self>,
	) -> Result<bool> {
		let has_child = depth != 0;
		let (at, i) = column.with_value_tables(|t| self.position(key, t, log))?;
		if at {
			let existing = self.separator_value_index(i);
			if let Some(existing) = existing {
				column.with_tables_and_self(|t, s| s.write_existing_value_plan(
					&NoHash,
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

	fn rebalance(
		&mut self,
		depth: u32,
		at: usize,
		column: &Column,
		log: &impl LogQuery,
		removed_nodes: &mut RemovedChildren<Self>,
	) -> Result<()> {
		let has_child = depth - 1 != 0;
		let middle = Self::Config::ORDER / 2;
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

	fn need_rebalance(&mut self) -> bool {
		let mut rebalance = false;
		let middle = Self::Config::ORDER / 2;
		if !self.has_separator(middle - 1) {
			rebalance = true;
		}
		rebalance
	}

	fn remove_last(
		&mut self,
		depth: u32,
		record_id: u64,
		column: &Column,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
		removed: &mut RemovedChildren<Self>,
	) -> Result<(bool, Option<Self::Separator>)> {
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

	fn get(&mut self, key: &[u8], btree: &BTreeTable, values: &Vec<ValueTable>, log: &impl LogQuery) -> Result<Option<(Address, bool)>> {
		let (at, i) = self.position(key, values, log)?;
		if at {
			Ok(self.separator_get_info(i))
		} else {
			if let Some(child) = self.fetch_child_from_btree(i, btree, log)? {
				return child.get(key, btree, values, log);
			}

			Ok(None)
		}
	}
	
	fn seek(from: &mut Box<Self>, key: &[u8], btree: &BTreeTable, values: &Vec<ValueTable>, log: &impl LogQuery, depth: u32, stack: &mut Vec<(usize, *mut Box<Self>)>) -> Result<bool> {
		let (at, i) = from.position(key, values, log)?;
		stack.push((i, from));
		if at {
			Ok(true)
		} else {
			if depth != 0 {
				if let Some(child) = from.fetch_child_box_from_btree(i, btree, log)? {
					return Self::seek(child, key, btree, values, log, depth - 1, stack);
				}
			}

			Ok(false)
		}
	}

	#[cfg(test)]
	fn is_balanced(&mut self, column: &Column, log: &impl LogQuery, parent_size: usize) -> Result<bool> {
		let size = self.number_separator();
		if parent_size != 0 && size < Self::Config::ORDER / 2 {
			return Ok(false);
		}

		let mut i = 0;
		while i < Self::Config::ORDER {
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
pub struct Node<C: Config> {
	separators: C::Separators,
	children: C::Children,
	entry: Entry<C>,
	changed: bool,
}

pub struct Separator<C: Config> {
	fetched: bool, // lazy read.
	modified: bool,
	separator: Option<SeparatorInner<C>>,
}
impl<C: Config> Default for Separator<C> {
	fn default() -> Self {
		Separator {
			fetched: false,
			modified: false,
			separator: None,
		}
	}
}

pub struct SeparatorInner<C: Config> {
	pub key: C::KeyBuf,
	pub splitted_key: Option<Vec<u8>>, // lazy read.
	pub value: u64,
}

impl<C: Config> SeparatorInner<C> {
	fn is_splitted(&self) -> bool {
		self.key.as_ref()[0] == u8::MAX
	}
	fn buf_key_size(&self) -> usize {
		let s = self.key.as_ref()[0];
		if s == u8::MAX {
			C::KEYSIZE - 1
		} else {
			s as usize
		}
	}
}

#[derive(Clone, Default)]
pub struct ChildState {
	pub read_index: bool,
	pub fetched: bool,
	pub modified: bool,
	pub moved: bool,
}

pub struct Child<C: Config> {
	state: ChildState,
	node: Option<Box<Node<C>>>, // lazy fetch.
	entry_index: Option<u64>, // no index is new.
}
impl<C: Config> Default for Child<C> {
	fn default() -> Self {
		let state = ChildState::default();
		Child { state, node: None, entry_index: None }
	}
}

impl<C: Config> Child<C> {
	fn new(node: Box<Node<C>>, entry_index: Option<u64>) -> Self {
		let mut state = ChildState::default();
		state.read_index = true;
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

impl<C: Config> NodeT for Node<C> {
	type Config = C;
	type Separator = Separator<C>;
	type Child = Child<C>;

	fn new() -> Self {
		Node {
			separators: Default::default(),
			children: Default::default(),
			entry: Entry::empty(),
			changed: true,
		}
	}

	fn clear(&mut self) {
		self.separators = Default::default();
		self.children = Default::default();
		self.entry = Entry::empty();
		self.changed = true;
	}

	fn from_encoded(enc: <Self::Config as Config>::Encoded) -> Self {
		Node {
			separators: Default::default(),
			children: Default::default(),
			entry: Entry::from_encoded(enc),
			changed: false,
		}
	}

	fn remove_separator(&mut self, at: usize) -> Self::Separator {
		self.changed = true;
		let mut separator = std::mem::replace(self.get_separator(at), Separator {
			fetched: true,
			modified: true,
			separator: None,
		});
		separator.modified = true;
		separator
	}

	fn remove_child(&mut self, at: usize) -> Self::Child {
		self.changed = true;
		let mut state = ChildState::default();
		state.fetched = true;
		state.moved = true;
		state.read_index = true;
		let mut child = std::mem::replace(self.get_child_index(at), Child {
			state,
			node: None,
			entry_index: None,
		});
		child.state.moved = true;
		child
	}

	fn try_forget_child(&mut self, at: usize) {
		let child = &mut self.children.as_mut()[at];
		if child.state.fetched && !child.state.modified {
			let mut state = child.state.clone();
			state.fetched = false;
			child.state = state;
			child.node = None;
		}
	}

	fn fetch_child_box(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		let child = self.get_fetched_child_index(at, col, log)?;
		Ok(child.node.as_mut())
	}

	fn fetch_child(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index(at, col, log)?;
		Ok(child.node.as_mut().map(|n| n.as_mut()))
	}

	fn fetch_child_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index_from_btree(at, btree, log)?;
		Ok(child.node.as_mut().map(|n| n.as_mut()))
	}

	fn fetch_child_box_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		let child = self.get_fetched_child_index_from_btree(at, btree, log)?;
		Ok(child.node.as_mut())
	}

	fn has_separator(&mut self, at: usize) -> bool {
		let at = self.get_separator(at);
		at.separator.is_some()
	}

	fn separator_partial_key(&mut self, at: usize) -> Option<Vec<u8>> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| {
			let size = s.buf_key_size();
			s.key.as_ref()[1..1 + size].to_vec()
		})
	}

	fn separator_value_index(&mut self, at: usize) -> Option<u64> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| s.value)
	}

	fn separator_get_info(&mut self, at: usize) -> Option<(Address, bool)> {
		let at = self.get_separator(at);
		at.separator.as_ref().map(|s| (Address::from_u64(s.value), s.is_splitted()))
	}
	
	fn set_separator(&mut self, at: usize, mut sep: Separator<C>) {
		sep.modified = true;
		self.changed = true;
		self.separators.as_mut()[at] = sep;
	}

	fn new_child(mut child: Box<Self>, index: Option<u64>) -> Self::Child {
		child.changed = true;
		Child::new(child, index)
	}

	fn inner_child(child: Self::Child) -> (Option<u64>, Option<Box<Self>>) {
		(child.entry_index, child.node)
	}

	fn set_child(&mut self, at: usize, mut child: Child<C>) {
		child.state.moved = true;
		self.changed = true;
		self.children.as_mut()[at] = child;
	}

	fn create_separator(key: &[u8], value: &[u8], column: &Column, log: &mut LogWriter, existing: Option<u64>, origin: ValueTableOrigin) -> Result<Self::Separator> {
		let mut key_buf = C::default_keybuf();
		let splitted_key = if key.len() > C::KEYSIZE - 1 {
			key_buf.as_mut()[0] = u8::MAX;
			key_buf.as_mut()[1..].copy_from_slice(&key[.. C::KEYSIZE - 1]);
			Some(key[C::KEYSIZE - 1 ..].to_vec())
		} else {
			key_buf.as_mut()[0] = key.len() as u8;
			key_buf.as_mut()[1..1 + key.len()].copy_from_slice(key);
			None
		};
		let value = if let Some(splitted_key) = splitted_key.as_ref() {
			if let Some(address) = existing {
				column.with_tables_and_self(|t, s| s.write_existing_value_plan(
					&VarKey(splitted_key.into()),
					t,
					Address::from_u64(address),
					Some(value),
					log,
					origin,
				))?.1.map(|a| a.as_u64()).unwrap_or(address)
			} else {
				column.with_tables_and_self(|t, s| s.write_new_value_plan(
						&VarKey(splitted_key.into()),
						t,
						value,
						log,
						origin,
				))?.as_u64()
			}
		} else {
			if let Some(address) = existing {
				column.with_tables_and_self(|t, s| s.write_existing_value_plan(
					&NoHash,
					t,
					Address::from_u64(address),
					Some(value),
					log,
					origin,
				))?.1.map(|a| a.as_u64()).unwrap_or(address)
			} else {
				column.with_tables_and_self(|t, s| s.write_new_value_plan(
						&NoHash,
						t,
						value,
						log,
						origin,
				))?.as_u64()
			}
		};
		Ok(Separator {
			fetched: true,
			modified: true,
			separator: Some(SeparatorInner {
				key: key_buf,
				splitted_key,
				value,
			}),
		})
	}

	fn split(
		&mut self,
		at: usize,
		skip_left_child: bool,
		mut insert_right: Option<(usize, Separator<C>)>,
		mut insert_right_child: Option<(usize, Child<C>)>,
		has_child: bool,
		removed_node: &mut RemovedChildren<Self>,
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
		for i in right_start .. C::ORDER {
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
			debug_assert!(insert == C::ORDER);
			right.separators.as_mut()[insert - right_start] = sep;
		}
		let mut offset = 0;
		if has_child {
			let skip_offset = if skip_left_child { 1 } else { 0 };
			for i in right_start + skip_offset .. C::ORDER_CHILD {
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
				debug_assert!(insert == C::ORDER);
				child.state.moved = true;
				right.children.as_mut()[insert + 1 - right_start] = child;
			}
		}
		(right, right_ix)
	}

	fn shift_from(&mut self, from: usize, has_child: bool, with_left_child: bool) {
		self.changed = true;
		let mut i = from;
		let mut current = self.remove_separator(i);
		while current.separator.is_some() {
			current.modified = true;
			i += 1;
			if i == C::ORDER {
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
				if i == C::ORDER_CHILD {
					break;
				}
				let mut next = self.get_child_index(i);
				current = std::mem::replace(&mut next, current);
			}
		}
	}

	fn remove_from(&mut self, from: usize, has_child: bool, with_left_child: bool) {
		let mut i = from;
		self.changed = true;
		while i < C::ORDER - 1 {
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
			while i < C::ORDER_CHILD - 1 {
				self.children.as_mut()[i] = self.remove_child(i + 1);
				if self.children.as_mut()[i].entry_index.is_none() && self.children.as_mut()[i].node.is_none() {
					break;
				}
				i += 1;
			}
		}
	}

	fn number_separator(&mut self) -> usize {
		let mut i = 0;
		while self.get_separator(i).separator.is_some() {
			i += 1;
			if i == C::ORDER {
				break;
			}
		}
		i
	}

	// Return true if match and matched position.
	// Return index of first element bigger than key otherwhise.
	fn position(&mut self, key: &[u8], values: &Vec<ValueTable>, log: &impl LogQuery) -> Result<(bool, usize)> {
		let mut i = 0;
		while let Some(separator) = self.get_separator(i).separator.as_mut() {
			let key_size = separator.buf_key_size();
			let upper = if separator.is_splitted() {
				std::cmp::min(key_size, key.len())
			} else {
				key.len()
			};
			match key[..upper].cmp(&separator.key.as_ref()[1..1 + key_size]) {
				Ordering::Greater => (),
				Ordering::Less => {
					return Ok((false, i));
				},
				Ordering::Equal => {
					if separator.is_splitted() {
						if separator.splitted_key.is_none() {
							let address = Address::from_u64(separator.value);
							let tier = address.size_tier();
							let index = address.offset();
							separator.splitted_key = values[tier as usize].var_key_at(index, log)?;
						}
						match key[key_size..].cmp(separator.splitted_key.as_ref().unwrap()) {
							Ordering::Greater => (),
							Ordering::Less => {
								return Ok((false, i));
							},
							Ordering::Equal => {
								return Ok((true, i));
							},
						}
					} else {
						return Ok((true, i));
					}
				},
			}
			i += 1;
			if i == C::ORDER {
				break;
			}
		}
		Ok((false, i))
	}

	// TODO default traitify
	fn write_plan(&mut self, column: &Column, writer: &mut LogWriter, node_id: Option<u64>, table_id: BTreeTableId, btree: &mut BTreeIndex, record_id: u64) -> Result<Option<u64>> {
		// TODO currently modified is not set properly so we iterate all, could have a child modified
		// (but with current use it is only interesting for delete on non existing value)

		for (at, child) in self.children.as_mut().iter_mut().enumerate() {
			// TODO child modified is not handled properly (in case of recursive it needs parent and 
			// other, for now, if fetched, then it is modified).
			// let child_modified = child.modified;
			let child_modified = true;
			if child_modified {
				if let Some(node) = child.node.as_mut() {
					if let Some(index) = node.write_plan(column, writer, child.entry_index, table_id, btree, record_id)? {
						child.entry_index = Some(index);
						self.changed = true;
						self.entry.write_child_index(at, index);
					} else {
						if child.state.moved {
							if let Some(index) = child.entry_index {
								self.changed = true;
								self.entry.write_child_index(at, index);
							}
						}
					}
				} else {
					if child.state.moved || child.state.modified {
						if let Some(index) = child.entry_index {
							self.changed = true;
							self.entry.write_child_index(at, index);
						} else {
							self.changed = true;
							self.entry.remove_child_index(at);
						}
					}
				}
			}
		}

		for (at, separator) in self.separators.as_mut().iter_mut().enumerate() {
			if separator.modified {
				self.changed = true;
				if let Some(sep) = separator.separator.as_mut() {
					self.entry.write_separator(at, &sep.key, sep.value);
				} else {
					self.entry.remove_separator(at);
				}
			}
		}

		let mut result = None;
		if self.changed {
			let index = if let Some(existing) = node_id {
				existing
			} else {
				let new_index = if btree.last_removed == HEADER_POSITION {
					let result = btree.filled;
					btree.filled += 1;
					result
				} else {
					let result = btree.last_removed;
					btree.last_removed = column.with_btree(|btree| {
						// TODO no need to read the whole encoded buffer.
						let encoded = btree.get_index::<Self, _>(result, writer)?;
						let mut buf = [0; 8];
						buf.copy_from_slice(&encoded.as_ref()[..8]);
						Ok(u64::from_le_bytes(buf))
					})?;
					result
				};
				result = Some(new_index);
				new_index
			};
			writer.insert_btree(table_id, index, self.entry.encoded.as_ref().to_vec(), record_id, btree);
		}

		// TODO could also update self back, but useless as long as only transient use.
		// Global cache support could be interesting to test though.

		Ok(result)
	}

}

impl<C: Config> Node<C> {
	fn get_separator(&mut self, at: usize) -> &mut Separator<C> {
		if self.separators.as_ref()[at].fetched == false {
			let separator = self.entry.read_separator(at);
			self.separators.as_mut()[at].separator = separator;
			self.separators.as_mut()[at].fetched = true;
		}
		&mut self.separators.as_mut()[at]
	}

	fn get_child_index(&mut self, at: usize) -> &mut Child<C> {
		if !self.children.as_ref()[at].state.read_index {
			let child = self.entry.read_child_index(at);
			self.children.as_mut()[at].entry_index = child;
			self.children.as_mut()[at].state.read_index = true;
		}
		&mut self.children.as_mut()[at]
	}

	fn get_fetched_child_index(&mut self, i: usize, column: &Column, log: &impl LogQuery) -> Result<&mut Child<C>> {
		let mut child = self.get_child_index(i);
		if !child.state.fetched {
			if let Some(ix) = child.entry_index {
				child.state.fetched = true;
				let entry = column.with_btree(|btree| btree.get_index::<Self, _>(ix, log))?;
				child.node = Some(Box::new(Self::from_encoded(entry)));
			}
		}
		Ok(child)
	}

	// TODO merge with get_fetched_child_index
	fn get_fetched_child_index_from_btree(&mut self, i: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<&mut Child<C>> {
		let mut child = self.get_child_index(i);
		if !child.state.fetched {
			if let Some(ix) = child.entry_index {
				child.state.fetched = true;
				let entry = btree.get_index::<Self, _>(ix, log)?;
				child.node = Some(Box::new(Self::from_encoded(entry)));
			}
		}
		Ok(child)
	}
}

/// For read access without changes or caching.
pub struct NodeReadNoCache<C: Config>{
	entry: Entry<C>,
	current_child: Option<Box<Self>>,
	current_child_index: Option<usize>,
	current_separator: Option<SeparatorInner<C>>,
	current_separator_index: Option<usize>,
}

impl<C: Config> NodeReadNoCache<C> {
	fn get_fetched_child_index(&mut self, i: usize, column: &Column, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		if Some(i) == self.current_child_index {
			return Ok(self.current_child.as_mut());
		}
		let child = self.entry.read_child_index(i);
		if let Some(ix) = child {
			let entry = column.with_btree(|btree| btree.get_index::<Self, _>(ix, log))?;
			self.current_child = Some(Box::new(Self::from_encoded(entry)));
			self.current_child_index = Some(i);
		}
		Ok(self.current_child.as_mut())
	}

	// TODO merge with get_fetched_child_index
	fn get_fetched_child_index_from_btree(&mut self, i: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		if Some(i) == self.current_child_index {
			return Ok(self.current_child.as_mut());
		}
		let child = self.entry.read_child_index(i);
		if let Some(ix) = child {
			let entry = btree.get_index::<Self, _>(ix, log)?;
			self.current_child = Some(Box::new(Self::from_encoded(entry)));
			self.current_child_index = Some(i);
		}
		Ok(self.current_child.as_mut())
	}

	fn get_separator(&mut self, at: usize) -> &mut Option<SeparatorInner<C>> {
		if Some(at) == self.current_separator_index {
			return &mut self.current_separator;
		}
		self.current_separator = self.entry.read_separator(at);
		self.current_separator_index = Some(at);
		&mut self.current_separator
	}
}

impl<C: Config> NodeT for NodeReadNoCache<C> {
	type Config = C;
	type Separator = Option<SeparatorInner<C>>;
	type Child = Option<Box<Self>>;

	fn new() -> Self {
		NodeReadNoCache {
			entry: Entry::empty(),
			current_child: None,
			current_child_index: None,
			current_separator: None,
			current_separator_index: None,
		}
	}

	fn clear(&mut self) {
		unreachable!("Read only");
		//self.0 = Entry::empty();
	}

	fn from_encoded(enc: <Self::Config as Config>::Encoded) -> Self {
		NodeReadNoCache {
			entry: Entry::from_encoded(enc),
			current_child: None,
			current_child_index: None,
			current_separator: None,
			current_separator_index: None,
		}
	}

	fn remove_separator(&mut self, _at: usize) -> Self::Separator {
		unreachable!("Read only");
	}

	fn remove_child(&mut self, _at: usize) -> Self::Child {
		unreachable!("Read only");
	}

	fn try_forget_child(&mut self, _at: usize) {
	}

	fn fetch_child_box(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		self.get_fetched_child_index(at, col, log)
	}

	fn fetch_child(&mut self, at: usize, col: &Column, log: &impl LogQuery) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index(at, col, log)?;
		Ok(child.map(|n| n.as_mut()))
	}

	fn fetch_child_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Self>> {
		let child = self.get_fetched_child_index_from_btree(at, btree, log)?;
		Ok(child.map(|n| n.as_mut()))
	}

	fn fetch_child_box_from_btree(&mut self, at: usize, btree: &BTreeTable, log: &impl LogQuery) -> Result<Option<&mut Box<Self>>> {
		self.get_fetched_child_index_from_btree(at, btree, log)
	}

	fn has_separator(&mut self, at: usize) -> bool {
		self.entry.read_has_separator(at)
	}

	fn separator_partial_key(&mut self, at: usize) -> Option<Vec<u8>> {
		self.get_separator(at);
		self.current_separator.as_ref().map(|s| {
			let size = s.buf_key_size();
			s.key.as_ref()[1..1 + size].to_vec()
		})
	}

	fn separator_value_index(&mut self, at: usize) -> Option<u64> {
		self.get_separator(at);
		self.current_separator.as_ref().map(|s| s.value)
	}

	fn separator_get_info(&mut self, at: usize) -> Option<(Address, bool)> {
		self.get_separator(at);
		self.current_separator.as_ref().map(|s| (Address::from_u64(s.value), s.is_splitted()))
	}
	
	fn set_separator(&mut self, _at: usize, _sep: Self::Separator) {
		unreachable!("Read only");
	}

	fn new_child(_child: Box<Self>, _index: Option<u64>) -> Self::Child {
		unreachable!("Read only");
	}

	fn inner_child(_child: Self::Child) -> (Option<u64>, Option<Box<Self>>) {
		unreachable!("Read only");
	}

	fn set_child(&mut self, _at: usize, _child: Self::Child) {
		unreachable!("Read only");
	}

	fn create_separator(_key: &[u8], _value: &[u8], _column: &Column, _log: &mut LogWriter, _existing: Option<u64>, _origin: ValueTableOrigin) -> Result<Self::Separator> {
		unreachable!("Read only");
	}

	fn split(
		&mut self,
		_at: usize,
		_skip_left_child: bool,
		_insert_right: Option<(usize, Self::Separator)>,
		_insert_right_child: Option<(usize, Self::Child)>,
		_has_child: bool,
		_removed_node: &mut RemovedChildren<Self>,
	) -> (Box<Self>, Option<u64>) {
		unreachable!("Read only");
	}

	fn shift_from(&mut self, _from: usize, _has_child: bool, _with_left_child: bool) {
		unreachable!("Read only");
	}

	fn remove_from(&mut self, _from: usize, _has_child: bool, _with_left_child: bool) {
		unreachable!("Read only");
	}

	fn number_separator(&mut self) -> usize {
		let mut i = 0;

		while self.entry.read_has_separator(i) {
			i += 1;
			if i == C::ORDER {
				break;
			}
		}
		i
	}

	// TODO factorize code
	fn position(&mut self, key: &[u8], values: &Vec<ValueTable>, log: &impl LogQuery) -> Result<(bool, usize)> {
		let mut i = 0;
		while let Some(mut separator) = self.entry.read_separator(i) {
			let key_size = separator.buf_key_size();
			let upper = if separator.is_splitted() {
				std::cmp::min(key_size, key.len())
			} else {
				key.len()
			};
			match key[..upper].cmp(&separator.key.as_ref()[1..1 + key_size]) {
				Ordering::Greater => (),
				Ordering::Less => {
					return Ok((false, i));
				},
				Ordering::Equal => {
					if separator.is_splitted() {
						if separator.splitted_key.is_none() {
							let address = Address::from_u64(separator.value);
							let tier = address.size_tier();
							let index = address.offset();
							separator.splitted_key = values[tier as usize].var_key_at(index, log)?;
						}
						match key[key_size..].cmp(separator.splitted_key.as_ref().unwrap()) {
							Ordering::Greater => (),
							Ordering::Less => {
								return Ok((false, i));
							},
							Ordering::Equal => {
								self.current_separator = Some(separator);
								self.current_separator_index = Some(i);
								return Ok((true, i));
							},
						}
					} else {
						return Ok((true, i));
					}
				},
			}
			i += 1;
			if i == C::ORDER {
				break;
			}
		}
		Ok((false, i))
	}

	fn write_plan(
		&mut self,
		_column: &Column,
		_writer: &mut LogWriter,
		_node_id: Option<u64>,
		_table_id: BTreeTableId,
		_btree: &mut BTreeIndex,
		_record_id: u64,
	) -> Result<Option<u64>> {
		unimplemented!("Read only");
	}
}
