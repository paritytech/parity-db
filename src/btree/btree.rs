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
use crate::db::CommitOverlay;
use crate::table::key::TableKeyQuery;
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::Result;
use parking_lot::RwLock;
use crate::btree::BTreeTable;

pub struct BTree {
	pub(super) depth: u32,
	pub(super) root_index: Option<Address>,
	pub(super) record_id: u64,
}

pub struct BTreeIterator<'a> {
	table: &'a BTreeTable,
	log: &'a RwLock<crate::log::LogOverlays>,
	commit_overlay: &'a RwLock<Vec<CommitOverlay>>,
	iter: BtreeIterBackend,
	col: ColId,
	pending_next_backend: Option<Option<(Vec<u8>, Vec<u8>)>>,
	overlay_last_key: Option<Vec<u8>>,
	from_seek: bool,
}

pub struct BtreeIterBackend(BTree, BTreeIterState);

impl<'a> BTreeIterator<'a> {
	pub(crate) fn new(
		table: &'a BTreeTable,
		col: ColId, 
		log: &'a RwLock<crate::log::LogOverlays>,
		commit_overlay: &'a RwLock<Vec<CommitOverlay>>,
	) -> Result<Self> {
		let record_id = log.read().last_record_id(col);
		let tree = table.with_locked(|btree| BTree::open(btree, log, record_id))?;
		let iter = tree.iter();
		Ok(BTreeIterator {
			table,
			iter: BtreeIterBackend(tree, iter),
			col,
			pending_next_backend: None,
			overlay_last_key: None,
			from_seek: false,
			log,
			commit_overlay,
		})
	}

	pub fn seek(&mut self, key: &[u8]) -> Result<()> {
		let after = false;
		// seek require log do not change
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		self.from_seek = !after;
		self.overlay_last_key = Some(key.to_vec());
		self.pending_next_backend = None;
		self.seek_backend(key.to_vec(), record_id, self.table, &*log, after)
	}

	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let col = self.col;

		// Lock log over function call (no btree struct change).
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		let commit_overlay = self.commit_overlay.read();
		let next_commit_overlay = commit_overlay.get(col as usize).and_then(|o| o.btree_next(&self.overlay_last_key, self.from_seek));
		// No consistency over iteration, allows dropping lock to overlay.
		std::mem::drop(commit_overlay);
		let next_backend = if let Some(n) = self.pending_next_backend.take() {
			n
		} else {
			self.next_backend(record_id, self.table, &*log)?
		};

		match (next_commit_overlay, next_backend) {
			(Some((commit_key, commit_value)), Some((backend_key, backend_value))) => {
				match commit_key.cmp(&backend_key) {
					std::cmp::Ordering::Less => {
						if let Some(value) = commit_value {
							self.overlay_last_key = Some(commit_key.clone());
							self.from_seek = false;
							self.pending_next_backend = Some(Some((backend_key, backend_value)));
							return Ok(Some((commit_key, value)));
						} else {
							self.overlay_last_key = Some(commit_key);
							self.from_seek = false;
							self.pending_next_backend = Some(Some((backend_key, backend_value)));
							std::mem::drop(log);
							return self.next();
						}
					},
					std::cmp::Ordering::Greater => {
						return Ok(Some((backend_key, backend_value)));
					},
					std::cmp::Ordering::Equal => {
						if let Some(value) = commit_value {
							self.overlay_last_key = Some(commit_key);
							self.from_seek = false;
							return Ok(Some((backend_key, value)));
						} else {
							self.overlay_last_key = Some(commit_key);
							self.from_seek = false;
							std::mem::drop(log);
							return self.next();
						}
					},
				}
			},
			(Some((commit_key, commit_value)), None) => {
				if let Some(value) = commit_value {
					self.overlay_last_key = Some(commit_key.clone());
					self.from_seek = false;
					self.pending_next_backend = Some(None);
					return Ok(Some((commit_key, value)));
				} else {
					self.overlay_last_key = Some(commit_key);
					self.from_seek = false;
					self.pending_next_backend = Some(None);
					std::mem::drop(log);
					return self.next();
				}
			},
			(None, Some((backend_key, backend_value))) => {
				return Ok(Some((backend_key, backend_value)));
			},
			(None, None) => {
				self.pending_next_backend = Some(None);
				return Ok(None);
			},
		}
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

pub struct BTreeIterState {
	state: Vec<(usize, Node)>,
	next_separator: bool,
	pub record_id: u64,
	// After state change, we seek to this last accessed key.
	pub last_key: Option<Vec<u8>>,
}

impl BTreeIterState {
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
					BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), tables, log)
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
			let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), b, log)?;
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
		values: TablesRef,
		log: &impl LogQuery,
		record_id: u64,
	) -> Result<Self> {
		let btree_header = BTreeTable::btree_header(log, values)?;

		let root_index = if btree_header.root == NULL_ADDRESS {
			None
		} else {
			Some(btree_header.root)
		};
		Ok(btree::BTree::new(root_index, btree_header.depth, record_id))
	}

	pub fn iter(&self) -> BTreeIterState {
		BTreeIterState {
			last_key: None,
			next_separator: false,
			state: vec![],
			record_id: self.record_id,
		}
	}

	pub fn write_sorted_changes(
		&mut self,
		mut changes: &[(Vec<u8>, Option<Vec<u8>>)],
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<()> {
		let mut root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), btree, log)?;
		let changes = &mut changes;

		while changes.len() > 0 {
			match root.change(None, self.depth, changes, btree, log)? {
				(Some((sep, right)), _) => {
					// add one level
					self.depth += 1;
					let left = std::mem::replace(&mut root, Node::new());
					let left_index = self.root_index.take();
					let new_index = BTreeTable::write_node_plan(
						btree,
						left,
						log,
						left_index,
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
			let new_index = BTreeTable::write_node_plan(
				btree,
				root,
				log,
				self.root_index,
			)?;

			if new_index.is_some() {
				self.root_index = new_index;
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub fn is_balanced(&self, tables: TablesRef, log: &impl LogQuery) -> Result<bool> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), tables, log)?;
		root.is_balanced(tables, log, 0)
	}

	pub fn get(&self, key: &[u8], values: TablesRef, log: &impl LogQuery) -> Result<Option<Vec<u8>>> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), values, log)?;
		if let Some(address) = root.get(key, values, log)? {
			let key_query = TableKeyQuery::Fetch(None);
			let r = Column::get_value(key_query, address, values, log)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn fetch_root(root: Address, tables: TablesRef, log: &impl LogQuery) -> Result<Node> {
		Ok(if root == NULL_ADDRESS {
			Node::new()
		} else {
			let root = BTreeTable::get_encoded_entry(root, log, tables)?;
			Node::from_encoded(root)
		})
	}
}
