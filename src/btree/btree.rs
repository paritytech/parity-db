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

//! BTree structure.

use super::*;
use crate::table::key::TableKeyQuery;
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::{Error, Result};
use parking_lot::RwLock;

/// In memory local btree overlay.

pub struct BTree {
	pub(super) depth: u32,
	pub(super) root: Box<Node>,
	pub(super) root_index: Option<u64>,
	pub(super) removed_children: RemovedChildren,
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
		let tree = column.with_locked(|btree| new_btree_inner(btree, log, record_id))?;
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
			let new_tree = col.with_locked(|btree| new_btree_inner(btree, log, record_id))?;
			*tree = new_tree;
		}
		iter.next(tree, col, log)
	}

	pub fn seek_backend(&mut self, key: Vec<u8>, record_id: u64, col: &BTreeTable, log: &impl LogQuery, after: bool) -> Result<()> {
		let BtreeIterBackend(tree, iter) = &mut self.iter;
		if record_id != tree.record_id {
			let new_tree = col.with_locked(|btree| new_btree_inner(btree, log, record_id))?;
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

pub struct RemovedChildren(pub(super) Vec<(Option<u64>, Option<Box<Node>>)>);

impl RemovedChildren {
	pub fn push(&mut self, index: Option<u64>, node: Option<Box<Node>>) {
		self.0.push((index, node));
	}

	pub fn available(&mut self) -> (Option<u64>, Option<Box<Node>>) {
		let mut index = None;
		let mut node = None;
		for i in self.0.iter_mut().rev() {
			if index.is_none() && i.0.is_some() {
				index = i.0.take();
			}
			if node.is_none() && i.1.is_some() {
				node = i.1.take();
			}
			if index.is_some() && node.is_some() {
				break;
			}
		}
		while let Some(&(None, None)) = self.0.last() {
			self.0.pop();
		}
		(index, node)
	}
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
				self.state.push((0, btree.root.as_ref().clone()));
			}
			while let Some((ix, node)) = self.state.last_mut() {

				if let Some(child) = col.with_locked(|btree| {
					node.force_fetch_node_at(*ix, log, btree)
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
		if col.with_locked(|b| Node::seek(btree.root.as_ref().clone(), key.as_ref(), b, log, btree.depth, &mut self.state))? {
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
	pub fn new(root_index: Option<u64>, root: Node, depth: u32, record_id: u64) -> Self {
		BTree {
			root: Box::new(root),
			root_index,
			depth,
			removed_children: RemovedChildren(Default::default()),
			record_id,
		}
	}

	pub fn iter(&self) -> BTreeIter {
		BTreeIter {
			last_key: None,
			next_separator: false,
			state: vec![],
			record_id: self.record_id,
		}
	}

	pub fn insert(
		&mut self,
		key: &[u8],
		value: &[u8],
		btree: TableLocked,
		log: &mut LogWriter,
		origin: ValueTableOrigin,
	) -> Result<()> {
		match self.root.insert(self.depth, key, value, self.record_id, btree, log, origin, &mut self.removed_children)? {
			Some((sep, right)) => {
				// add one level
				self.depth += 1;
				let left = std::mem::replace(&mut self.root, Box::new(Node::new()));
				self.root.set_child(0, Node::new_child(left, self.root_index));
				self.root_index = None;
				self.root.set_child(1, right);
				self.root.set_separator(0, sep);
				Ok(())
			},
			None => Ok(()),
		}
	}

	#[cfg(test)]
	pub fn get(&mut self, key: &[u8], btree: TableLocked, log: &impl LogQuery) -> Result<Option<Vec<u8>>> {
		if let Some(address) = self.root.get(key, btree, log)? {
			let key_query = TableKeyQuery::Fetch(None);
			let r = Column::get_at_value_index_locked(key_query, address, btree, log)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn get_with_lock_no_cache(&mut self, key: &[u8], values: TableLocked, log: &impl LogQuery) -> Result<Option<Vec<u8>>> {
		if let Some(address) = self.root.get_no_cache(key, values, log)? {
			let key_query = TableKeyQuery::Fetch(None);
			let r = Column::get_at_value_index_locked(key_query, address, values, log)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn remove(&mut self, key: &[u8], btree: TableLocked, log: &mut LogWriter, origin: ValueTableOrigin) -> Result<()> {
		self.root.remove(self.depth, key, self.record_id, btree, log, origin, &mut self.removed_children)?;
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use tempfile::tempdir;
	use parking_lot::RwLock;
	use crate::log::LogOverlays;
	use std::collections::BTreeMap;

	fn test_basic(change_set: &[(Vec<u8>, Option<Vec<u8>>)]) {
		let record_id = 1;
		let col_nb = 0usize;
		let tmp = tempdir().unwrap();
		let mut options = crate::options::Options::with_columns(tmp.path(), 1);
		options.columns[col_nb].btree_index = true;
		let origin = crate::column::ValueTableOrigin::BTree(crate::btree::BTreeTableId::new(col_nb as u8));
		let db = crate::Db::open_or_create(&options).unwrap();

		let root = Node::new();
		let mut tree = BTree::new(None, root, 0, record_id);
		let overlays = RwLock::new(LogOverlays::default());
		let mut log_overlay = LogWriter::new(&overlays, record_id);
		let col = match db.column(col_nb) {
			Column::Hash(_) => unreachable!(),
			Column::Tree(col) => col,
		};

		for (key, value) in change_set.iter() {
			if let Some(value) = value.as_ref() {
				col.with_locked(|col| tree.insert(key, value, col, &mut log_overlay, origin)).unwrap();
			} else {
				col.with_locked(|col| tree.remove(key, col, &mut log_overlay, origin)).unwrap();
			}
		}

		let state: BTreeMap<Vec<u8>, Option<Vec<u8>>> = change_set.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		for (key, value) in state.iter() {
			assert_eq!(&col.with_locked(|col| tree.get(key, col, &log_overlay)).unwrap(), value);
		}
		assert!(col.with_locked(|col| tree.root.is_balanced(col, &log_overlay, 0)).unwrap());
	}

	#[test]
	fn test_simple() {
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
		]);
		test_basic(&[
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key5".to_vec(), Some(b"value5".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key11".to_vec(), Some(b"value31".to_vec())),
			(b"key12".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key51".to_vec(), Some(b"value31".to_vec())),
			(b"key52".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key31".to_vec(), Some(b"value31".to_vec())),
			(b"key32".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key6".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key0".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key4".to_vec(), Some(b"value3".to_vec())),
			(b"key5".to_vec(), Some(b"value4".to_vec())),
			(b"key6".to_vec(), Some(b"value4".to_vec())),
			(b"key7".to_vec(), Some(b"value2".to_vec())),
			(b"key8".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key4".to_vec(), Some(b"value3".to_vec())),
			(b"key5".to_vec(), Some(b"value4".to_vec())),
			(b"key7".to_vec(), Some(b"value2".to_vec())),
			(b"key8".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			([5u8; 250].to_vec(), Some(b"value5".to_vec())),
			([5u8; 200].to_vec(), Some(b"value3".to_vec())),
			([5u8; 100].to_vec(), Some(b"value4".to_vec())),
			([5u8; 150].to_vec(), Some(b"value2".to_vec())),
			([5u8; 101].to_vec(), Some(b"value1".to_vec())),
			([5u8; 250].to_vec(), None),
			([5u8; 101].to_vec(), None),
		]);
	}

	#[test]
	fn test_random() {
		for i in 0..100 {
			test_random_inner(60, 60, i);
		}
		for i in 0..500 {
			test_random_inner(20, 60, i);
		}
	}
	fn test_random_inner(size: usize, key_size: usize, seed: u64) {
		use rand::{RngCore, SeedableRng};
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut data = Vec::<(Vec<u8>, Option<Vec<u8>>)>::new();
		for i in 0..size {
			let nb_delete: u32 = rng.next_u32(); // should be out of loop, yet it makes alternance insert/delete in some case.
			let nb_delete = (nb_delete as usize % size) / 2;
			let mut key = vec![0u8; key_size];
			rng.fill_bytes(&mut key[..]);
			let value = if i > size - nb_delete {
				let random_key = rng.next_u32();
				let random_key = (random_key % 4) > 0;
				if !random_key {
					key = data[i - size / 2].0.clone();
				}
				None
			} else {
				Some(key.clone())
			};
			let var_keysize = rng.next_u32();
			let var_keysize = var_keysize as usize % (key_size / 2);
			key.truncate(key_size - var_keysize);
			data.push((key, value));
		}
		test_basic(data.as_slice());
	}
}
