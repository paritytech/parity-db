// Copyright 2022 Parity Technologies (UK) Ltd.
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

//! Btree overlay definition and methods.

use super::*;
use crate::table::key::TableKeyQuery;
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::{Error, Result};
use parking_lot::RwLock;

pub struct BTree {
	pub(super) depth: u32,
	pub(super) root_index: Option<Address>,
	pub(super) record_id: u64,
}

pub struct BTreeIterator<'a> {
	db: &'a crate::db::DbInner,
	pub(crate) iter: BtreeIterBackend,
	pub(crate) col: ColId,
	pub(crate) pending_next_backend: Option<Option<(Vec<u8>, Vec<u8>)>>,
	pub(crate) overlay_last_key: Option<Vec<u8>>,
	pub(crate) from_seek: bool,
}

pub struct BtreeIterBackend(BTree, BTreeIter);

impl<'a> BTreeIterator<'a> {
	pub(crate) fn new(
		db: &'a crate::db::DbInner,
		col: ColId, 
		log: &RwLock<crate::log::LogOverlays>,
	) -> Result<Self> {
		let column = match db.column(col) {
			Column::Hash(_) => {
				return Err(Error::InvalidConfiguration("Not an indexed column.".to_string()));
			},
			Column::Tree(col) => col,
		};
		let record_id = log.read().last_record_id(col);
		let tree = column.with_locked(|btree| BTree::open(btree, log, record_id))?;
		let iter = tree.iter();
		Ok(BTreeIterator {
			db,
			iter: BtreeIterBackend(tree, iter),
			col,
			pending_next_backend: None,
			overlay_last_key: None,
			from_seek: false,
		})
	}

	pub fn seek(&mut self, key: &[u8]) -> Result<()> {
		self.db.btree_iter_seek(self, key, false)
	}

	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		self.db.btree_iter_next(self)
	}

	pub fn next_backend(&mut self, record_id: u64, col: &BTreeTable, log: &impl LogQuery) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
		}
		iter.next(tree, col, log)
	}

	pub fn seek_backend(&mut self, key: Vec<u8>, record_id: u64, col: &BTreeTable, log: &impl LogQuery, after: bool) -> Result<()> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
		}
		iter.seek(key, tree, col, log, after)
	}
}

pub struct BTreeIter {
	state: Vec<(usize, Node)>,
	next_separator: bool,
	pub record_id: u64,
	// After state change, we seek to this last accessed key.
	pub last_key: Option<Vec<u8>>,
}

impl BTreeIter {
	pub fn next(&mut self, btree: &mut BTree, col: &BTreeTable, log: &impl LogQuery) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		if self.next_separator && self.state.is_empty() {
			return Ok(None);
		}
		if self.record_id != btree.record_id {
			self.state.clear();
			if let Some(last_key) = self.last_key.take() {
				self.seek(last_key, btree, col, log, true)?;
			}
		}
		if !self.next_separator {
			if self.state.is_empty() {
				let root = col.with_locked(|tables| {
					BTree::fetch_root(btree.root_index.unwrap_or(HEADER_POSITION), tables, log)
				})?;
				self.state.push((0, root));
			}
			while let Some((ix, node)) = self.state.last_mut() {
				if let Some(child) = col.with_locked(|btree| {
					node.fetch_child(*ix, btree, log)
				})? {
					self.state.push((0, child));
				} else {
					break;
				}
			}
			self.next_separator = true;
		}

		if let Some((ix, node)) = self.state.last_mut() {
			if *ix < ORDER {
				if let Some(address) = node.separator_address(*ix) {
					let key = node.separator_key(*ix).unwrap();
					*ix += 1;
					self.next_separator = false;

					let key_query = TableKeyQuery::Fetch(None);
					let r = col.get_at_value_index(key_query, address, log)?;
					self.last_key = Some(key.clone());
					return Ok(r.map(|r| (key, r.1)));
				}
			}
		}
		self.state.pop();
		self.next_separator = true;
		self.next(btree, col, log)
	}

	pub fn seek(&mut self, key: Vec<u8>, btree: &mut BTree, col: &BTreeTable, log: &impl LogQuery, after: bool) -> Result<()> {
		self.state.clear();
		self.record_id = btree.record_id;
		self.last_key = Some(key.to_vec());
		if col.with_locked(|b| {
			let root = BTree::fetch_root(btree.root_index.unwrap_or(HEADER_POSITION), b, log)?;
			Node::seek(root, key.as_ref(), b, log, btree.depth, &mut self.state)
		})? {
			// on value
			if after {
				if let Some((ix, _node)) = self.state.last_mut() {
					*ix += 1;
				}
				self.next_separator = false;
			} else {
				self.next_separator = true;
			}
		} else {
			self.next_separator = true;
		}
		Ok(())
	}
}

impl BTree {
	pub fn new(root_index: Option<Address>, depth: u32, record_id: u64) -> Self {
		BTree {
			root_index,
			depth,
			record_id,
		}
	}

	pub fn open(
		values: TableLocked,
		log: &impl LogQuery,
		record_id: u64,
	) -> Result<Self> {
		let btree_index = BTreeTable::btree_index(log, values)?;

		let root_index = if btree_index.root == HEADER_POSITION {
			None
		} else {
			Some(btree_index.root)
		};
		Ok(btree::BTree::new(root_index, btree_index.depth, record_id))
	}

	pub fn iter(&self) -> BTreeIter {
		BTreeIter {
			last_key: None,
			next_separator: false,
			state: vec![],
			record_id: self.record_id,
		}
	}

	pub fn write_sorted_changes(
		&mut self,
		mut changes: &[(Vec<u8>, Option<Vec<u8>>)],
		btree: TableLocked,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
	) -> Result<()> {
		let mut root = BTree::fetch_root(self.root_index.unwrap_or(HEADER_POSITION), btree, log)?;
		let changes = &mut changes;

		while changes.len() > 0 {
			match root.change(None, self.depth, changes, btree, log, origin)? {
				(Some((sep, right)), _) => {
					// add one level
					self.depth += 1;
					let left = std::mem::replace(&mut root, Node::new());
					let left_index = self.root_index.take();
					let new_index = BTreeTable::write_plan_node(
						btree,
						left,
						log,
						left_index,
						origin,
					)?;
					let new_index = if new_index.is_some() {
						new_index
					} else {
						left_index
					};
					root.set_child(0, Node::new_child(new_index));
					root.set_child(1, right);
					root.set_separator(0, sep);
				},
				(_, true) => {
					if let Some((node_index, node)) = root.need_remove_root(btree, log)? {
						self.depth -= 1;
						if let Some(index) = self.root_index.take() {
							BTreeTable::write_plan_remove_node(
								btree,
								log,
								index,
								origin,
							)?;
						}
						self.root_index = node_index;
						root = node;
					}
				},
				_ => (),
			}
			*changes = &changes[1..];
		}

		if root.changed {
			let new_index = BTreeTable::write_plan_node(
				btree,
				root,
				log,
				self.root_index,
				origin,
			)?;

			if new_index.is_some() {
				self.root_index = new_index;
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub fn is_balanced(&self, tables: TableLocked, log: &impl LogQuery) -> Result<bool> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(HEADER_POSITION), tables, log)?;
		root.is_balanced(tables, log, 0)
	}

	pub fn get(&mut self, key: &[u8], values: TableLocked, log: &impl LogQuery) -> Result<Option<Vec<u8>>> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(HEADER_POSITION), values, log)?;
		if let Some(address) = root.get(key, values, log)? {
			let key_query = TableKeyQuery::Fetch(None);
			let r = Column::get_at_value_index(key_query, address, values, log)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn fetch_root(root: Address, tables: TableLocked, log: &impl LogQuery) -> Result<Node> {
		Ok(if root == HEADER_POSITION {
			Node::new()
		} else {
			let root = BTreeTable::get_index(root, log, tables)?;
			Node::from_encoded(root)
		})
	}
}
