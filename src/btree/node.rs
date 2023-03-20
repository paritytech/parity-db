// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! BTree node struct and methods.

use super::{
	iter::{LastIndex, SeekTo},
	*,
};
use crate::{
	column::Column,
	db::{RcKey, RcValue},
	error::Result,
	index::Address,
	log::{LogQuery, LogWriter},
	table::key::TableKey,
	Operation,
};
use std::cmp::Ordering;

impl Node {
	pub(crate) fn last_separator_index(&self) -> Option<usize> {
		self.separators.iter().rposition(|separator| separator.separator.is_some())
	}

	pub fn write_child(
		&mut self,
		i: usize,
		child: Self,
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<()> {
		if child.changed {
			let child_index = self.children[i].entry_index;
			let new_index = BTreeTable::write_node_plan(btree, child, log, child_index)?;
			if new_index.is_some() {
				self.changed = true;
				self.children[i].entry_index = new_index;
			}
		}
		Ok(())
	}

	pub fn write_split_child(
		right_ix: Option<Address>,
		right: Node,
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<Child> {
		let new_index = BTreeTable::write_node_plan(btree, right, log, right_ix)?;
		let right_ix = if new_index.is_some() { new_index } else { right_ix };
		Ok(Self::new_child(right_ix))
	}

	pub fn change(
		&mut self,
		parent: Option<(&mut Self, usize)>,
		depth: u32,
		changes: &mut &[Operation<RcKey, RcValue>],
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<(Option<(Separator, Child)>, bool)> {
		loop {
			if !btree.ref_counted &&
				changes.len() > 1 &&
				!matches!(changes[1], Operation::Reference(..)) &&
				changes[0].key() == changes[1].key()
			{
				*changes = &changes[1..];
				continue
			}
			let r = match &changes[0] {
				Operation::Set(key, value) =>
					self.insert(depth, key.value(), value.value(), changes, btree, log)?,
				_ => self.on_existing(depth, changes, btree, log)?,
			};
			if r.0.is_some() || r.1 {
				return Ok(r)
			}
			if changes.len() == 1 {
				break
			}
			if let Some((parent, p)) = &parent {
				let key = &changes[1].key();
				let (at, i) = self.position(key.value())?; // TODO could start position from current
				if at || i < self.number_separator() {
					*changes = &changes[1..];
					continue
				}
				let (at, i) = parent.position(key.value())?;
				if !at && &i == p && i < parent.number_separator() {
					*changes = &changes[1..];
					continue
				}
				// Could check other parents for case i == parent.number_separator.
				// Would mean unsafe Vec<*mut>, or other complex design: skipping
				// this case.
			}
			break
		}
		Ok((None, false))
	}

	fn insert(
		&mut self,
		depth: u32,
		key: &[u8],
		value: &[u8],
		changes: &mut &[Operation<RcKey, RcValue>],
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<(Option<(Separator, Child)>, bool)> {
		let has_child = depth != 0;

		let (at, i) = self.position(key)?;
		// insert
		if !at {
			if has_child {
				return Ok(if let Some(mut child) = self.fetch_child(i, btree, log)? {
					let r = child.change(Some((self, i)), depth - 1, changes, btree, log)?;
					self.write_child(i, child, btree, log)?;
					match r {
						(Some((sep, right)), _) => {
							// insert from child
							(self.insert_node(depth, i, sep, right, btree, log)?, false)
						},
						(None, true) => {
							self.rebalance(depth, i, btree, log)?;
							(None, self.need_rebalance())
						},
						r => r,
					}
				} else {
					(None, false)
				})
			}

			if self.has_separator(ORDER - 1) {
				// full
				let middle = ORDER / 2;
				let insert = i;
				let insert_separator = Self::create_separator(key, value, btree, log, None)?;

				return match insert.cmp(&middle) {
					Ordering::Equal => {
						let (right, right_ix) = self.split(middle, true, None, None, has_child);
						let right = Self::write_split_child(right_ix, right, btree, log)?;
						Ok((Some((insert_separator, right)), false))
					},
					Ordering::Less => {
						let (right, right_ix) = self.split(middle, false, None, None, has_child);
						let sep = self.remove_separator(middle - 1);
						self.shift_from(insert, has_child, false);
						self.set_separator(insert, insert_separator);
						let right = Self::write_split_child(right_ix, right, btree, log)?;
						Ok((Some((sep, right)), false))
					},
					Ordering::Greater => {
						let (right, right_ix) = self.split(
							middle + 1,
							false,
							Some((insert, insert_separator)),
							None,
							has_child,
						);
						let sep = self.remove_separator(middle);
						let right = Self::write_split_child(right_ix, right, btree, log)?;
						Ok((Some((sep, right)), false))
					},
				}
			}

			self.shift_from(i, has_child, false);
			self.set_separator(i, Self::create_separator(key, value, btree, log, None)?);
		} else {
			let existing = self.separator_address(i);
			self.set_separator(i, Self::create_separator(key, value, btree, log, existing)?);
		}
		Ok((None, false))
	}

	pub fn insert_node(
		&mut self,
		depth: u32,
		at: usize,
		separator: Separator,
		right_child: Child,
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<Option<(Separator, Child)>> {
		debug_assert!(depth != 0);
		let has_child = true;
		let child = right_child;
		if self.has_separator(ORDER - 1) {
			// full
			let middle = ORDER / 2;
			let insert = at;

			match insert.cmp(&middle) {
				Ordering::Equal => {
					let (mut right, right_ix) = self.split(middle, true, None, None, has_child);
					right.set_child(0, child);
					let right = Self::write_split_child(right_ix, right, btree, log)?;
					Ok(Some((separator, right)))
				},
				Ordering::Less => {
					let (right, right_ix) = self.split(middle, false, None, None, has_child);
					let sep = self.remove_separator(middle - 1);
					self.shift_from(insert, has_child, false);
					self.set_separator(insert, separator);
					self.set_child(insert + 1, child);
					let right = Self::write_split_child(right_ix, right, btree, log)?;
					Ok(Some((sep, right)))
				},
				Ordering::Greater => {
					let (right, right_ix) = self.split(
						middle + 1,
						false,
						Some((insert, separator)),
						Some((insert, child)),
						has_child,
					);
					let sep = self.remove_separator(middle);
					let right = Self::write_split_child(right_ix, right, btree, log)?;
					Ok(Some((sep, right)))
				},
			}
		} else {
			self.shift_from(at, has_child, false);
			self.set_separator(at, separator);
			self.set_child(at + 1, child);
			Ok(None)
		}
	}

	fn on_existing(
		&mut self,
		depth: u32,
		changes: &mut &[Operation<RcKey, RcValue>],
		values: TablesRef,
		log: &mut LogWriter,
	) -> Result<(Option<(Separator, Child)>, bool)> {
		let change = &changes[0];
		let key = change.key();
		let has_child = depth != 0;
		let (at, i) = self.position(key.value())?;
		if at {
			let existing = self.separator_address(i);
			if let Some(existing) = existing {
				if Column::write_existing_value_plan::<_, RcValue>(
					&TableKey::NoHash,
					values,
					existing,
					change,
					log,
					None,
					values.ref_counted,
				)?
				.0
				.is_none()
				{
					let _ = self.remove_separator(i);
					if depth != 0 {
						// replace by bigger value in left child.
						if let Some(mut child) = self.fetch_child(i, values, log)? {
							let (need_balance, sep) = child.remove_last(depth - 1, values, log)?;
							self.write_child(i, child, values, log)?;
							if let Some(sep) = sep {
								self.set_separator(i, sep);
							}
							if need_balance {
								self.rebalance(depth, i, values, log)?;
							}
						}
					} else {
						self.remove_from(i, false, true);
					}
				}
			}
		} else {
			if !has_child {
				return Ok((None, false))
			}
			return if let Some(mut child) = self.fetch_child(i, values, log)? {
				let r = child.change(Some((self, i)), depth - 1, changes, values, log)?;
				self.write_child(i, child, values, log)?;
				Ok(match r {
					(Some((sep, right)), _) => {
						// insert from child
						(self.insert_node(depth, i, sep, right, values, log)?, false)
					},
					(None, true) => {
						self.rebalance(depth, i, values, log)?;
						(None, self.need_rebalance())
					},
					r => r,
				})
			} else {
				Ok((None, false))
			}
		}

		Ok((None, self.need_rebalance()))
	}

	pub fn rebalance(
		&mut self,
		depth: u32,
		at: usize,
		values: TablesRef,
		log: &mut LogWriter,
	) -> Result<()> {
		let has_child = depth - 1 != 0;
		let middle = ORDER / 2;
		let mut balance_from_left = false;
		let mut left = None;
		if at > 0 {
			left = self.fetch_child(at - 1, values, log)?;
			if let Some(node) = left.as_ref() {
				if node.has_separator(middle) {
					balance_from_left = true;
				}
			}
		}
		if balance_from_left {
			let mut left = left.unwrap();
			let mut child = None;
			let last_sibling = left.last_separator_index().unwrap();
			if has_child {
				child = Some(left.remove_child(last_sibling + 1));
			}
			let separator2 = left.remove_separator(last_sibling);
			let separator = self.remove_separator(at - 1);
			self.set_separator(at - 1, separator2);
			let mut right = self.fetch_child(at, values, log)?.unwrap();
			right.shift_from(0, has_child, true);
			if let Some(child) = child {
				right.set_child(0, child);
			}
			right.set_separator(0, separator);
			self.write_child(at - 1, left, values, log)?;
			self.write_child(at, right, values, log)?;
			return Ok(())
		}

		let number_child = self.number_separator() + 1;
		let mut balance_from_right = false;
		let mut right = None;
		if at + 1 < number_child {
			right = self.fetch_child(at + 1, values, log)?;
			if let Some(node) = right.as_ref() {
				if node.has_separator(middle) {
					balance_from_right = true;
				}
			}
		}

		if balance_from_right {
			let mut child = None;
			let mut right = right.unwrap();
			if has_child {
				child = Some(right.remove_child(0));
			}
			let separator2 = right.remove_separator(0);
			right.remove_from(0, has_child, true);
			let separator = self.remove_separator(at);
			self.set_separator(at, separator2);
			let mut left = self.fetch_child(at, values, log)?.unwrap();
			let last_left = left.number_separator();
			left.set_separator(last_left, separator);
			if let Some(child) = child {
				left.set_child(last_left + 1, child);
			}
			self.write_child(at, left, values, log)?;
			self.write_child(at + 1, right, values, log)?;
			return Ok(())
		}

		let (at, at_right) = if at + 1 == number_child {
			right = self.fetch_child(at, values, log)?;
			(at - 1, at)
		} else {
			left = self.fetch_child(at, values, log)?;
			if right.is_none() {
				right = self.fetch_child(at + 1, values, log)?;
			}
			(at, at + 1)
		};

		let mut left = left.unwrap();
		let separator = self.remove_separator(at);
		let mut i = left.number_separator();
		left.set_separator(i, separator);
		i += 1;
		let mut right = right.unwrap();
		let right_len = right.number_separator();
		let mut right_i = 0;
		while right_i < right_len {
			let mut child = None;
			if has_child {
				child = Some(right.remove_child(right_i));
			}
			let separator = right.remove_separator(right_i);
			left.set_separator(i, separator);
			if let Some(child) = child {
				left.set_child(i, child);
			}
			i += 1;
			right_i += 1;
		}
		if has_child {
			let child = right.remove_child(right_i);
			left.set_child(i, child);
		}

		let removed = self.remove_child(at_right);
		if let Some(index) = removed.entry_index {
			BTreeTable::write_plan_remove_node(values, log, index)?;
		}
		self.write_child(at, left, values, log)?;
		let has_child = true; // rebalance on parent.
		self.remove_from(at, has_child, false);
		Ok(())
	}

	fn need_rebalance(&mut self) -> bool {
		let middle = ORDER / 2;
		!self.has_separator(middle - 1)
	}

	pub fn need_remove_root(
		&mut self,
		values: TablesRef,
		log: &mut LogWriter,
	) -> Result<Option<(Option<Address>, Node)>> {
		if self.number_separator() == 0 && self.fetch_child(0, values, log)?.is_some() {
			if let Some(node) = self.fetch_child(0, values, log)? {
				let child = self.remove_child(0);
				return Ok(Some((child.entry_index, node)))
			}
		}
		Ok(None)
	}

	pub fn remove_last(
		&mut self,
		depth: u32,
		values: TablesRef,
		log: &mut LogWriter,
	) -> Result<(bool, Option<Separator>)> {
		let last = if let Some(last) = self.last_separator_index() {
			last
		} else {
			return Ok((false, None))
		};
		let i = last;
		if depth == 0 {
			let result = self.remove_separator(i);

			Ok((self.need_rebalance(), Some(result)))
		} else if let Some(mut child) = self.fetch_child(i + 1, values, log)? {
			let result = child.remove_last(depth - 1, values, log)?;
			self.write_child(i + 1, child, values, log)?;
			if result.0 {
				self.rebalance(depth, i + 1, values, log)?;
				Ok((self.need_rebalance(), result.1))
			} else {
				Ok(result)
			}
		} else {
			Ok((false, None))
		}
	}

	pub fn get(
		&self,
		key: &[u8],
		values: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<Address>> {
		let (at, i) = self.position(key)?;
		if at {
			Ok(self.separator_address(i))
		} else {
			if let Some(child) = self.fetch_child(i, values, log)? {
				return child.get(key, values, log)
			}

			Ok(None)
		}
	}

	pub fn seek(
		self,
		seek_to: SeekTo,
		values: TablesRef,
		log: &impl LogQuery,
		mut depth: u32,
		stack: &mut Vec<(LastIndex, Self)>,
	) -> Result<()> {
		let mut from = self;
		loop {
			let (at, i) = if let Some(key) = seek_to.key() {
				from.position(key)?
			} else {
				(false, from.last_separator_index().map(|i| i + 1).unwrap_or(0))
			};
			if at {
				stack.push(match seek_to {
					SeekTo::Exclude(_) => (LastIndex::At(i), from),
					SeekTo::Include(_) => (LastIndex::Seeked(i), from),
					SeekTo::Last => unreachable!(),
				});
				return Ok(())
			}
			if depth == 0 {
				stack.push((LastIndex::Before(i), from));
				return Ok(())
			}
			if let Some(child) = from.fetch_child(i, values, log)? {
				stack.push((LastIndex::Descend(i), from));
				from = child;
				depth -= 1
			} else {
				return Err(Error::Corruption(format!("A btree node is missing a child at {i:?}")))
			}
		}
	}

	#[cfg(test)]
	pub fn is_balanced(
		&self,
		tables: TablesRef,
		log: &impl LogQuery,
		parent_size: usize,
	) -> Result<bool> {
		let size = self.number_separator();
		if parent_size != 0 && size < ORDER / 2 {
			return Ok(false)
		}

		let mut i = 0;
		while i < ORDER {
			let child = self.fetch_child(i, tables, log)?;
			i += 1;
			if child.is_none() {
				continue
			}
			if !child.unwrap().is_balanced(tables, log, size)? {
				return Ok(false)
			}
		}

		Ok(true)
	}
}

/// Nodes with data loaded in memory.
/// Nodes get only serialized when flushed in the global overlay
/// (there we need one entry per record id).
#[derive(Clone, Debug)]
pub struct Node {
	pub(super) separators: [Separator; ORDER],
	pub(super) children: [Child; ORDER_CHILD],
	pub(super) changed: bool,
}

impl Default for Node {
	fn default() -> Self {
		Node { separators: Default::default(), children: Default::default(), changed: true }
	}
}

#[derive(Clone, Default, Debug)]
pub struct Separator {
	pub(super) modified: bool,
	pub(super) separator: Option<SeparatorInner>,
}

#[derive(Clone, Debug)]
pub struct SeparatorInner {
	pub key: Vec<u8>,
	pub value: Address,
}

#[derive(Clone, Default, Debug)]
pub struct Child {
	pub(super) moved: bool,
	pub(super) entry_index: Option<Address>,
}

impl Node {
	pub fn from_encoded(enc: Vec<u8>) -> Result<Self> {
		let mut entry = Entry::from_encoded(enc);
		let mut node =
			Node { separators: Default::default(), children: Default::default(), changed: false };
		let mut i_children = 0;
		let mut i_separator = 0;
		loop {
			if let Some(child_index) = entry.read_child_index()? {
				node.children.as_mut()[i_children].entry_index = Some(child_index);
			}
			i_children += 1;
			if i_children == ORDER_CHILD {
				break
			}
			if let Some(sep) = entry.read_separator()? {
				node.separators.as_mut()[i_separator].separator = Some(sep);
				i_separator += 1
			} else {
				break
			}
		}
		Ok(node)
	}

	pub fn remove_separator(&mut self, at: usize) -> Separator {
		self.changed = true;
		let mut separator = std::mem::replace(
			&mut self.separators[at],
			Separator { modified: true, separator: None },
		);
		separator.modified = true;
		separator
	}

	pub fn remove_child(&mut self, at: usize) -> Child {
		self.changed = true;
		let mut child =
			std::mem::replace(&mut self.children[at], Child { moved: true, entry_index: None });
		child.moved = true;
		child
	}

	pub fn has_separator(&self, at: usize) -> bool {
		self.separators[at].separator.is_some()
	}

	pub fn separator_key(&self, at: usize) -> Option<Vec<u8>> {
		self.separators[at].separator.as_ref().map(|s| s.key.clone())
	}

	pub fn separator_address(&self, at: usize) -> Option<Address> {
		self.separators[at].separator.as_ref().map(|s| s.value)
	}

	pub fn set_separator(&mut self, at: usize, mut sep: Separator) {
		sep.modified = true;
		self.changed = true;
		self.separators.as_mut()[at] = sep;
	}

	pub fn new_child(index: Option<Address>) -> Child {
		Child { moved: true, entry_index: index }
	}

	pub fn set_child(&mut self, at: usize, mut child: Child) {
		child.moved = true;
		self.changed = true;
		self.children.as_mut()[at] = child;
	}

	fn create_separator(
		key: &[u8],
		value: &[u8],
		btree: TablesRef,
		log: &mut LogWriter,
		existing: Option<Address>,
	) -> Result<Separator> {
		let key = key.to_vec();
		let value = if let Some(address) = existing {
			Column::write_existing_value_plan(
				&TableKey::NoHash,
				btree,
				address,
				&Operation::Set((), value),
				log,
				None,
				btree.ref_counted,
			)?
			.1
			.unwrap_or(address)
		} else {
			Column::write_new_value_plan(&TableKey::NoHash, btree, value, log, None)?
		};
		let modified = Some(value) != existing;
		Ok(Separator { modified, separator: Some(SeparatorInner { key, value }) })
	}

	fn split(
		&mut self,
		at: usize,
		skip_left_child: bool,
		mut insert_right: Option<(usize, Separator)>,
		mut insert_right_child: Option<(usize, Child)>,
		has_child: bool,
	) -> (Self, Option<Address>) {
		let (right_ix, mut right) = (None, Self::default());
		let mut offset = 0;
		let right_start = at;
		for i in right_start..ORDER {
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
			let skip_offset = usize::from(skip_left_child);
			for i in right_start + skip_offset..ORDER_CHILD {
				let child = self.remove_child(i);
				if insert_right_child.as_ref().map(|ins| ins.0 + 1 == i).unwrap_or(false) {
					offset = 1;
					if let Some((_, mut child)) = insert_right_child.take() {
						child.moved = true;
						right.children.as_mut()[i - right_start] = child;
					}
				}
				right.children.as_mut()[i + offset - right_start] = child;
			}
			if let Some((insert, mut child)) = insert_right_child.take() {
				debug_assert!(insert == ORDER);
				child.moved = true;
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
			if i == ORDER {
				break
			}
			current = std::mem::replace(&mut self.separators[i], current);
		}
		if has_child {
			let mut i = if with_left_child { from } else { from + 1 };
			let mut current = self.remove_child(i);
			while current.entry_index.is_some() {
				current.moved = true;
				i += 1;
				if i == ORDER_CHILD {
					break
				}
				current = std::mem::replace(&mut self.children[i], current);
			}
		}
	}

	fn remove_from(&mut self, from: usize, has_child: bool, with_left_child: bool) {
		let mut i = from;
		self.changed = true;
		while i < ORDER - 1 {
			self.separators.as_mut()[i] = self.remove_separator(i + 1);
			self.separators.as_mut()[i].modified = true;
			if self.separators.as_mut()[i].separator.is_none() {
				break
			}
			i += 1;
		}
		if has_child {
			let mut i = if with_left_child { from } else { from + 1 };
			while i < ORDER_CHILD - 1 {
				self.children.as_mut()[i] = self.remove_child(i + 1);
				if self.children.as_mut()[i].entry_index.is_none() {
					break
				}
				i += 1;
			}
		}
	}

	fn number_separator(&self) -> usize {
		let mut i = 0;
		while self.separators[i].separator.is_some() {
			i += 1;
			if i == ORDER {
				break
			}
		}
		i
	}

	// Return true if match and matched position.
	// Return index of first element bigger than key otherwise.
	fn position(&self, key: &[u8]) -> Result<(bool, usize)> {
		let mut i = 0;
		while let Some(separator) = self.separators[i].separator.as_ref() {
			match key[..].cmp(&separator.key[..]) {
				Ordering::Greater => (),
				Ordering::Less => return Ok((false, i)),
				Ordering::Equal => return Ok((true, i)),
			}
			i += 1;
			if i == ORDER {
				break
			}
		}
		Ok((false, i))
	}

	pub fn fetch_child(
		&self,
		i: usize,
		values: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<Self>> {
		if let Some(ix) = self.children[i].entry_index {
			let entry = BTreeTable::get_encoded_entry(ix, log, values)?;
			return Ok(Some(Self::from_encoded(entry)?))
		}
		Ok(None)
	}
}
