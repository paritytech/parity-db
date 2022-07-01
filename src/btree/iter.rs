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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IterDirection {
	Backward,
	Forward,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum SeekTo {
	At,
	After,
}

pub struct BTreeIterator<'a> {
	table: &'a BTreeTable,
	log: &'a RwLock<crate::log::LogOverlays>,
	commit_overlay: &'a RwLock<Vec<CommitOverlay>>,
	iter: BtreeIterBackend,
	col: ColId,
	pending_next_backend: Option<Option<(Vec<u8>, Vec<u8>)>>,
	last_key: Option<Vec<u8>>,
	from_seek: bool,
	direction: IterDirection,
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
			direction: IterDirection::Forward,
			from_seek: false,
			log,
			commit_overlay,
		})
	}

	pub fn seek(&mut self, key: &[u8]) -> Result<()> {
		// seek require log do not change
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		self.from_seek = true;
		self.last_key = Some(key.to_vec());
		self.pending_next_backend = None;
		self.seek_backend(key, record_id, self.table, &*log, SeekTo::At, self.direction)
	}

	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let col = self.col;

		// Lock log over function call (no btree struct change).
		let commit_overlay = self.commit_overlay.read();
		let next_commit_overlay = commit_overlay.get(col as usize).and_then(|o| {
			o.btree_next(
				&self.last_key,
				self.from_seek || self.direction == IterDirection::Backward,
			)
		});
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		// No consistency over iteration, allows dropping lock to overlay.
		std::mem::drop(commit_overlay);
		if record_id != self.iter.1.record_id {
			self.pending_next_backend = None;
		}
		let next_backend =
			if self.pending_next_backend.is_some() && self.direction == IterDirection::Forward {
				self.pending_next_backend.take().expect("Checked above")
			} else {
				self.next_backend(record_id, self.table, &*log)?
			};

		match (next_commit_overlay, next_backend) {
			(Some((commit_key, commit_value)), Some((backend_key, backend_value))) =>
				match (commit_key.cmp(&backend_key), commit_value) {
					(std::cmp::Ordering::Less, Some(value)) => {
						self.last_key = Some(commit_key.clone());
						self.from_seek = false;
						self.pending_next_backend = Some(Some((backend_key, backend_value)));
						Ok(Some((commit_key, value)))
					},
					(std::cmp::Ordering::Less, None) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						self.pending_next_backend = Some(Some((backend_key, backend_value)));
						std::mem::drop(log);
						self.next()
					},
					(std::cmp::Ordering::Greater, _) => {
						self.last_key = Some(backend_key.clone());
						Ok(Some((backend_key, backend_value)))
					},
					(std::cmp::Ordering::Equal, Some(value)) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						Ok(Some((backend_key, value)))
					},
					(std::cmp::Ordering::Equal, None) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						std::mem::drop(log);
						self.next()
					},
				},
			(Some((commit_key, Some(commit_value))), None) => {
				self.last_key = Some(commit_key.clone());
				self.from_seek = false;
				self.pending_next_backend = Some(None);
				Ok(Some((commit_key, commit_value)))
			},
			(Some((commit_key, None)), None) => {
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

	pub fn prev(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let col = self.col;

		// Lock log over function call (no btree struct change).
		let commit_overlay = self.commit_overlay.read();
		let next_commit_overlay = commit_overlay.get(col as usize).and_then(|o| {
			o.btree_prev(&self.last_key, self.from_seek || self.direction == IterDirection::Forward)
		});
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		// No consistency over iteration, allows dropping lock to overlay.
		std::mem::drop(commit_overlay);
		if record_id != self.iter.1.record_id {
			self.pending_next_backend = None;
		}
		let next_backend =
			if self.pending_next_backend.is_some() && self.direction == IterDirection::Backward {
				self.pending_next_backend.take().expect("Checked above")
			} else {
				self.prev_backend(record_id, self.table, &*log)?
			};

		match (next_commit_overlay, next_backend) {
			(Some((commit_key, commit_value)), Some((backend_key, backend_value))) =>
				match (commit_key.cmp(&backend_key), commit_value) {
					(std::cmp::Ordering::Greater, Some(value)) => {
						self.last_key = Some(commit_key.clone());
						self.from_seek = false;
						self.pending_next_backend = Some(Some((backend_key, backend_value)));
						Ok(Some((commit_key, value)))
					},
					(std::cmp::Ordering::Greater, None) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						self.pending_next_backend = Some(Some((backend_key, backend_value)));
						std::mem::drop(log);
						self.prev()
					},
					(std::cmp::Ordering::Less, _) => {
						self.last_key = Some(backend_key.clone());
						Ok(Some((backend_key, backend_value)))
					},
					(std::cmp::Ordering::Equal, Some(value)) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						Ok(Some((backend_key, value)))
					},
					(std::cmp::Ordering::Equal, None) => {
						self.last_key = Some(commit_key);
						self.from_seek = false;
						std::mem::drop(log);
						self.prev()
					},
				},
			(Some((commit_key, Some(commit_value))), None) => {
				self.last_key = Some(commit_key.clone());
				self.from_seek = false;
				self.pending_next_backend = Some(None);
				Ok(Some((commit_key, commit_value)))
			},
			(Some((commit_key, None)), None) => {
				self.last_key = Some(commit_key);
				self.from_seek = false;
				self.pending_next_backend = Some(None);
				std::mem::drop(log);
				self.prev()
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
	) -> Result<Option<(Vec<u8>, Value)>> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id || self.direction != IterDirection::Forward {
			self.direction = IterDirection::Forward;
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			if let Some(last_key) = self.last_key.as_ref() {
				let seek_to = if self.from_seek { SeekTo::At } else { SeekTo::After };
				iter.seek(last_key.as_slice(), tree, col, log, seek_to, IterDirection::Forward)?;
			}
			iter.record_id = record_id;
		}

		iter.next(tree, col, log)
	}

	pub fn prev_backend(
		&mut self,
		record_id: u64,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Value)>> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id || self.direction != IterDirection::Backward {
			self.direction = IterDirection::Backward;
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			if let Some(last_key) = self.last_key.as_ref() {
				let seek_to = if self.from_seek { SeekTo::At } else { SeekTo::After };
				iter.seek(last_key.as_slice(), tree, col, log, seek_to, IterDirection::Backward)?;
				self.from_seek = false;
			}
			iter.record_id = record_id;
		}

		iter.prev(tree, col, log)
	}

	pub fn seek_backend(
		&mut self,
		key: &[u8],
		record_id: u64,
		col: &BTreeTable,
		log: &impl LogQuery,
		seek_to: SeekTo,
		direction: IterDirection,
	) -> Result<()> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			iter.record_id = record_id;
		}
		iter.seek(key, tree, col, log, seek_to, direction)
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
	) -> Result<Option<(Vec<u8>, Value)>> {
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
		self.next_separator = true;
		self.state.pop();
		self.next(btree, col, log)
	}

	pub fn prev(
		&mut self,
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Value)>> {
		if self.next_separator && self.state.is_empty() {
			return Ok(None)
		}
		if !self.next_separator {
			if self.state.is_empty() {
				let root = col.with_locked(|tables| {
					BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), tables, log)
				})?;
				self.state.push((ORDER - 1, root));
			}
			while let Some((ix, node)) = self.state.last_mut() {
				if let Some(child) = col.with_locked(|btree| node.fetch_child(*ix, btree, log))? {
					self.state.push((ORDER - 1, child));
				} else {
					break
				}
			}
			self.next_separator = true;
		}

		if let Some((ix, node)) = self.state.last_mut() {
			while node.separator_address(*ix).is_none() && *ix > 0 {
				*ix -= 1;
			}

			if let Some(address) = node.separator_address(*ix) {
				let key = node.separator_key(*ix).unwrap();
				self.next_separator = false;

				if *ix == 0 {
					self.next_separator = true;
					self.state.pop();
				} else {
					*ix -= 1;
				}

				let key_query = TableKeyQuery::Fetch(None);
				let r = col.get_at_value_index(key_query, address, log)?;
				return Ok(r.map(|r| (key, r.1)))
			}
		}

		self.next_separator = true;
		self.state.pop();
		self.prev(btree, col, log)
	}

	pub fn seek(
		&mut self,
		key: &[u8],
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
		seek_to: SeekTo,
		direction: IterDirection,
	) -> Result<()> {
		self.state.clear();
		self.next_separator = false;
		let found = col.with_locked(|b| {
			let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), b, log)?;
			let from_end = direction == IterDirection::Backward;
			Node::seek(root, key, b, log, btree.depth, &mut self.state, from_end)
		})?;

		match (found, seek_to, direction) {
			(false, _, _) | (true, SeekTo::At, _) => self.next_separator = true,
			(true, SeekTo::After, IterDirection::Forward) =>
				if let Some((ix, _node)) = self.state.last_mut() {
					*ix += 1;
				},
			(true, SeekTo::After, IterDirection::Backward) => match self.state.last_mut() {
				Some((0, _node)) => drop(self.state.pop()),
				Some((ix, _node)) => *ix -= 1,
				None => (),
			},
		}

		Ok(())
	}
}
