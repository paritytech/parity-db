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

	pub fn seek_to_first(&mut self) -> Result<()> {
		self.direction = IterDirection::Forward;
		self.seek(&[])
	}

	pub fn seek_to_last(&mut self) -> Result<()> {
		let log = self.log.read();
		let record_id = log.last_record_id(self.col);
		self.from_seek = true;
		self.last_key = None;
		self.pending_next_backend = None;
		self.direction = IterDirection::Backward;
		self.seek_backend_to_last(record_id, self.table, &*log)
	}

	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		let col = self.col;

		// Lock log over function call (no btree struct change).
		let commit_overlay = self.commit_overlay.read();
		self.from_seek |= self.direction == IterDirection::Backward;
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
		self.from_seek |= self.direction == IterDirection::Forward;
		let next_commit_overlay = commit_overlay
			.get(col as usize)
			.and_then(|o| o.btree_prev(&self.last_key, self.from_seek));
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
				self.pending_next_backend = None;
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
				self.pending_next_backend = None;
				let seek_to = if self.from_seek { SeekTo::At } else { SeekTo::After };
				iter.seek(last_key.as_slice(), tree, col, log, seek_to, IterDirection::Backward)?;
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

	pub fn seek_backend_to_last(
		&mut self,
		record_id: u64,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<()> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| BTree::open(btree, log, record_id))?;
			*tree = new_tree;
			iter.record_id = record_id;
		}
		iter.seek_to_last(tree, col, log)
	}
}

#[derive(Debug, PartialEq, Eq)]
pub enum NodeType {
	Child,
	Separator,
}

#[derive(Debug)]
pub struct BTreeIterState {
	state: Vec<(usize, NodeType, Node)>,
	fetch_root: bool,
	pub record_id: u64,
}

impl BTreeIterState {
	pub fn new(record_id: u64) -> BTreeIterState {
		BTreeIterState { state: vec![], fetch_root: true, record_id }
	}

	pub fn next(
		&mut self,
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Value)>> {
		if !self.fetch_root && self.state.is_empty() {
			return Ok(None)
		}

		if self.fetch_root {
			let root = col.with_locked(|tables| {
				BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), tables, log)
			})?;
			self.state.push((0, NodeType::Child, root));
			self.fetch_root = false;
		}

		while let Some((ix, ty @ NodeType::Child, node)) = self.state.last_mut() {
			*ty = NodeType::Separator;
			if let Some(child) = col.with_locked(|btree| node.fetch_child(*ix, btree, log))? {
				self.state.push((0, NodeType::Child, child));
			}
		}

		let (ix, ty, node) = self.state.last_mut().expect("We always have at least one entry");
		if *ix < ORDER {
			if let Some(address) = node.separator_address(*ix) {
				let key = node.separator_key(*ix).unwrap();
				*ix += 1;
				*ty = NodeType::Child;

				let key_query = TableKeyQuery::Fetch(None);
				let r = col.get_at_value_index(key_query, address, log)?;
				return Ok(r.map(|r| (key, r.1)))
			}
		}

		self.state.pop();
		self.next(btree, col, log)
	}

	pub fn prev(
		&mut self,
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<Option<(Vec<u8>, Value)>> {
		if self.state.is_empty() {
			return Ok(None)
		}
		while let Some((ix, ty @ NodeType::Child, node)) = self.state.last_mut() {
			*ty = NodeType::Separator;
			match col.with_locked(|btree| node.fetch_child(*ix, btree, log))? {
				Some(child) => {
					let last_separator = child.last_separator_index().unwrap_or_default();
					let last_child = child.last_child_index().unwrap_or_default();
					if *ix > 0 {
						*ix -= 1;
					} else {
						self.state.pop();
					}
					self.state.push(if last_child > last_separator {
						(last_child, NodeType::Child, child)
					} else {
						(last_separator, NodeType::Separator, child)
					});
				},
				None if *ix > 0 => *ix -= 1,
				None => drop(self.state.pop()),
			}
		}

		let (ix, ty, node) =
			if let Some(entry) = self.state.last_mut() { entry } else { return Ok(None) };
		*ty = NodeType::Child;
		if let Some(address) = node.separator_address(*ix) {
			let key = node.separator_key(*ix).unwrap();

			let key_query = TableKeyQuery::Fetch(None);
			let r = col.get_at_value_index(key_query, address, log)?;
			return Ok(r.map(|r| (key, r.1)))
		}

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
		self.fetch_root = false;
		col.with_locked(|b| {
			let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), b, log)?;
			match direction {
				IterDirection::Forward =>
					Node::seek(root, key, b, log, btree.depth, &mut self.state, seek_to),
				IterDirection::Backward =>
					Node::seek_prev(root, key, b, log, btree.depth, &mut self.state, seek_to),
			}
		})
	}

	pub fn seek_to_last(
		&mut self,
		btree: &mut BTree,
		col: &BTreeTable,
		log: &impl LogQuery,
	) -> Result<()> {
		self.state.clear();
		self.fetch_root = false;
		col.with_locked(|b| {
			let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), b, log)?;
			Node::seek_to_last(root, b, log, btree.depth, &mut self.state)
		})
	}
}
