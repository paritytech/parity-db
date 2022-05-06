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

/// BTree iterator implementation.
/// This does not enforce consistency.
/// If a commit did happen after an
/// iterator is created, the iterator will
/// just replay a `seek` operation at its
/// latest accessed key.u
use super::*;
use crate::{
	btree::BTreeTable, db::CommitOverlay, error::Result, log::LogQuery, table::key::TableKeyQuery,
};
use parking_lot::RwLock;

pub struct BTreeIterator<'a> {
	table: &'a BTreeTable,
	log: &'a RwLock<crate::log::LogOverlays>,
	commit_overlay: &'a RwLock<Vec<CommitOverlay>>,
	iter: BtreeIterBackend,
	col: ColId,
	pending_next_backend: Option<Option<(Vec<u8>, Vec<u8>)>>,
	last_key: Option<Vec<u8>>,
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
		let iter = BTreeIterState::new(tree.record_id);
		Ok(BTreeIterator {
			table,
			iter: BtreeIterBackend(tree, iter),
			col,
			pending_next_backend: None,
			last_key: None,
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
		self.last_key = Some(key.to_vec());
		self.pending_next_backend = None;
		self.seek_backend(key, record_id, self.table, &*log, after)
	}

	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let col = self.col;

		// Lock log over function call (no btree struct change).
		let commit_overlay = self.commit_overlay.read();
		let next_commit_overlay = commit_overlay
			.get(col as usize)
			.and_then(|o| o.btree_next(&self.last_key, self.from_seek));
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		// No consistency over iteration, allows dropping lock to overlay.
		std::mem::drop(commit_overlay);
		if record_id != self.iter.1.record_id {
			self.pending_next_backend = None;
		}
		let next_backend = if let Some(n) = self.pending_next_backend.take() {
			n
		} else {
			self.next_backend(record_id, self.table, &*log)?
		};

		match (next_commit_overlay, next_backend) {
			(Some((commit_key, commit_value)), Some((backend_key, backend_value))) =>
				match commit_key.cmp(&backend_key) {
					std::cmp::Ordering::Less =>
						if let Some(value) = commit_value {
							self.last_key = Some(commit_key.clone());
							self.from_seek = false;
							self.pending_next_backend = Some(Some((backend_key, backend_value)));
							Ok(Some((commit_key, value)))
						} else {
							self.last_key = Some(commit_key);
							self.from_seek = false;
							self.pending_next_backend = Some(Some((backend_key, backend_value)));
							std::mem::drop(log);
							self.next()
						},
					std::cmp::Ordering::Greater => {
						self.last_key = Some(backend_key.clone());
						Ok(Some((backend_key, backend_value)))
					},
					std::cmp::Ordering::Equal =>
						if let Some(value) = commit_value {
							self.last_key = Some(commit_key);
							self.from_seek = false;
							Ok(Some((backend_key, value)))
						} else {
							self.last_key = Some(commit_key);
							self.from_seek = false;
							std::mem::drop(log);
							self.next()
						},
				},
			(Some((commit_key, commit_value)), None) =>
				if let Some(value) = commit_value {
					self.last_key = Some(commit_key.clone());
					self.from_seek = false;
					self.pending_next_backend = Some(None);
					Ok(Some((commit_key, value)))
				} else {
					self.last_key = Some(commit_key);
					self.from_seek = false;
					self.pending_next_backend = Some(None);
					std::mem::drop(log);
					self.next()
				},
			(None, Some((backend_key, backend_value))) => {
				self.last_key = Some(backend_key.clone());
				Ok(Some((backend_key, backend_value)))
			},
			(None, None) => {
				self.pending_next_backend = Some(None);
				Ok(None)
			},
		}
	}

	pub fn next_backend(
		&mut self,
		record_id: u64,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			if let Some(last_key) = self.last_key.as_ref() {
				iter.seek(last_key.as_slice(), tree, col, log, true)?;
			}
			iter.record_id = record_id;
		}
		iter.next(tree, col, log)
	}

	pub fn seek_backend(
		&mut self,
		key: &[u8],
		record_id: u64,
		col: &BTreeTable,
		log: &impl LogQuery,
		after: bool,
	) -> Result<()> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			iter.record_id = record_id;
		}
		iter.seek(key, tree, col, log, after)
	}
}

pub struct BTreeIterState {
	state: Vec<(usize, Node)>,
	next_separator: bool,
	pub record_id: u64,
}

impl BTreeIterState {
	pub fn new(record_id: u64) -> BTreeIterState {
		BTreeIterState { next_separator: false, state: vec![], record_id }
	}

	pub fn next(
		&mut self,
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		if self.next_separator && self.state.is_empty() {
			return Ok(None)
		}
		if !self.next_separator {
			if self.state.is_empty() {
				let root = col.with_locked(|tables| {
					BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), tables, log)
				})?;
				self.state.push((0, root));
			}
			while let Some((ix, node)) = self.state.last_mut() {
				if let Some(child) = col.with_locked(|btree| node.fetch_child(*ix, btree, log))? {
					self.state.push((0, child));
				} else {
					break
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
					return Ok(r.map(|r| (key, r.1)))
				}
			}
		}
		self.state.pop();
		self.next_separator = true;
		self.next(btree, col, log)
	}

	pub fn seek(
		&mut self,
		key: &[u8],
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
		after: bool,
	) -> Result<()> {
		self.state.clear();
		self.next_separator = false;
		let found = col.with_locked(|b| {
			let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), b, log)?;
			Node::seek(root, key, b, log, btree.depth, &mut self.state)
		})?;
		if found {
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
