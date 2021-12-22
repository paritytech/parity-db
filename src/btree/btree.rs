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

//! BTree structure.

use super::*;
use crate::table::ValueTable;
use crate::table::key::{TableKeyQuery, NoHash};
use crate::log::{LogWriter, LogQuery};
use crate::column::Column;
use crate::error::Result;
use crate::compress::Compress;
use parking_lot::RwLock;

/// In memory local btree overlay.

pub struct BTree<N: NodeT> {
	pub(super) depth: u32,
	root: Box<N>,
	pub(super) root_index: Option<u64>,
	removed_children: RemovedChildren<N>,
	record_id: u64,
}

pub struct BTreeIterator<'a> {
	db: &'a crate::db::DbInner,
	pub(crate) iter: BTreeIterVariants,
	pub(crate) col: ColId,
	pub(crate) pending_next_backend: Option<Option<(Vec<u8>, Vec<u8>)>>,
	pub(crate) overlay_last_key: Option<Vec<u8>>,
	pub(crate) from_seek: bool,
}

pub enum BTreeIterVariants {
	Order2_3(BTree<Node<Order2_3>>, BTreeIter<Node<Order2_3>>),
	// TODO
}

impl<'a> BTreeIterator<'a> {
	pub(crate) fn new(
		db: &'a crate::db::DbInner,
		col: ColId, 
		log: &RwLock<crate::log::LogOverlays>,
	) -> Result<Self> {
		let column = db.column(col);
		let variant = column.with_btree(|btree| Ok(btree.variant))?;
		match variant {
			ConfigVariants::Order2_3 => {
				// TODO move new_btree_inner to parent.
				let record_id = log.read().btree_last_record_id(col);
				let (tree, _table_id) = new_btree_inner::<Node<crate::btree::Order2_3>, _>(column, log, record_id)?;
				let iter = tree.iter();
				Ok(BTreeIterator {
					db,
					iter: BTreeIterVariants::Order2_3(tree, iter),
					col,
					pending_next_backend: None,
					overlay_last_key: None,
					from_seek: false,
				})
			},
		}
	}

	pub fn seek(&mut self, key: &[u8]) -> Result<()> {
		self.db.btree_iter_seek(self, key, false)
	}

	pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		self.db.btree_iter_next(self)
	}
	/*
	pub fn last_key(&self) -> &Option<Vec<u8>> {
		match &self.iter {
			BTreeIterVariants::Order2_3(_tree, iter) => {
				&iter.last_key
			},
		}
	}

	pub fn set_last_key(&mut self, key: Vec<u8>) {
		match &mut self.iter {
			BTreeIterVariants::Order2_3(_tree, iter) => {
				iter.last_key = Some(key);
			},
		}
	}*/

	pub fn next_backend(&mut self, record_id: u64, col: &Column, log: &impl LogQuery) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		match &mut self.iter {
			BTreeIterVariants::Order2_3(tree, iter) => {
				if record_id != tree.record_id {
					let (new_tree, _table_id) = new_btree_inner::<Node<crate::btree::Order2_3>, _>(col, log, record_id)?;
					*tree = new_tree;
				}
				iter.next(tree, col, log)
			}
		}
	}

	pub fn seek_backend(&mut self, key: Vec<u8>, record_id: u64, col: &Column, log: &impl LogQuery, after: bool) -> Result<()> {
		match &mut self.iter {
			BTreeIterVariants::Order2_3(tree, iter) => {
				if record_id != tree.record_id {
					let (new_tree, _table_id) = new_btree_inner::<Node<crate::btree::Order2_3>, _>(col, log, record_id)?;
					*tree = new_tree;
				}
				iter.seek(key, tree, col, log, after)
			}
		}
	}
}

pub struct BTreeIter<N: NodeT> {
	// TODO could just use NodeBox and fetch, but this could allow caching, then this is better
	state: Vec<(usize, *mut Box<N>)>,
	next_separator: bool,
	pub record_id: u64,
	pub last_key: Option<Vec<u8>>, // used to seek if state did change.
}

pub struct RemovedChildren<N: NodeT>(Vec<(Option<u64>, Option<Box<N>>)>);

impl<N: NodeT> RemovedChildren<N> {
	pub fn push(&mut self, index: Option<u64>, node: Option<Box<N>>) {
		self.0.push((index, node));
	}

	pub fn available(&mut self) -> (Option<u64>, Option<Box<N>>) {
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

impl<N: NodeT> BTreeIter<N> {
	pub fn next(&mut self, btree: &mut BTree<N>, col: &Column, log: &impl LogQuery) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
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
				self.state.push((0, &mut btree.root));
			}
			while let Some((ix, node)) = self.state.last_mut() {
				let node: &mut Box<N> = unsafe { node.as_mut().unwrap() };
				if let Some(child) = node.fetch_child_box(*ix, col, log)? {
					self.state.push((0, child));
				} else {
					break;
				}
			}
			self.next_separator = true;
		}

		if let Some((ix, node)) = self.state.last_mut() {
			let node: &mut Box<N> = unsafe { node.as_mut().unwrap() };
			if *ix < N::Config::ORDER {
				if let Some(address) = node.separator_get_info(*ix) {
					let key = node.separator_key(*ix).unwrap();
					// Warning, this only work as long as we have one iterator
					// for one btree. Otherwhise using Rc would be needed.
					node.try_forget_child(*ix);
					*ix += 1;
					self.next_separator = false;

					let mut k = ();
					let key_query = TableKeyQuery::Fetch::<NoHash>(&mut k);
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

	pub fn seek(&mut self, key: Vec<u8>, btree: &mut BTree<N>, col: &Column, log: &impl LogQuery, after: bool) -> Result<()> {
		self.state.clear();
		self.record_id = btree.record_id;
		self.last_key = Some(key.to_vec());
		if col.with_value_tables_and_btree(|b, t, c| N::seek(&mut btree.root, key.as_ref(), b, t, log, btree.depth, &mut self.state, c))? {
			// on value
			if after {
				if let Some((ix, node)) = self.state.last_mut() {
					let node: &mut Box<N> = unsafe { node.as_mut().unwrap() };
					node.try_forget_child(*ix);
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

impl<N: NodeT> BTree<N> {
	pub fn new(root_index: Option<u64>, root: N, depth: u32, record_id: u64) -> Self {
		BTree {
			root: Box::new(root),
			root_index,
			depth,
			removed_children: RemovedChildren(Default::default()),
			record_id,
		}
	}

	pub fn iter(&self) -> BTreeIter<N> {
		BTreeIter {
			last_key: None,
			next_separator: false,
			state: vec![],
			record_id: self.record_id,
		}
	}

	pub fn insert(&mut self, key: &[u8], value: &[u8], column: &Column, log: &mut LogWriter, origin: ValueTableOrigin) -> Result<()> {
		match self.root.insert(self.depth, key, value, self.record_id, column, log, origin, &mut self.removed_children)? {
			Some((sep, right)) => {
				// add one level
				self.depth += 1;
				let left = std::mem::replace(&mut self.root, Box::new(N::new()));
				self.root.set_child_node(0, left, self.root_index);
				self.root_index = None;
				self.root.set_child(1, right);
				self.root.set_separator(0, sep);
				Ok(())
			},
			None => Ok(()),
		}
	}

	#[cfg(test)]
	pub fn get(&mut self, key: &[u8], column: &Column, log: &impl LogQuery) -> Result<Option<Vec<u8>>> {
		column.with_value_tables_and_btree(|b, t, c| self.get_with_lock(key, b, t, log, c))
	}

	pub fn get_with_lock(&mut self, key: &[u8], btree: &BTreeTable, values: &Vec<ValueTable>, log: &impl LogQuery, comp: &Compress) -> Result<Option<Vec<u8>>> {
		if let Some(address) = self.root.get(key, btree, values, log, comp)? {
			let mut k = ();
			let key_query = TableKeyQuery::Fetch::<NoHash>(&mut k);
			let r = Column::get_at_value_index_locked(key_query, address, values, log, comp)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn remove(&mut self, key: &[u8], column: &Column, log: &mut LogWriter, origin: ValueTableOrigin) -> Result<()> {
		self.root.remove(self.depth, key, self.record_id, column, log, origin, &mut self.removed_children)?;
		Ok(())
	}

	pub fn write_plan(&mut self, column: &Column, writer: &mut LogWriter, table_id: BTreeTableId, record_id: u64, btree_index: &mut BTreeIndex, origin: ValueTableOrigin) -> Result<()> {
		if let Some(ix) = self.root.write_plan(column, writer, self.root_index, table_id, btree_index, record_id, origin)? {
			self.root_index = Some(ix);
		}
		for (node_index, _node) in self.removed_children.0.drain(..) {
			if let Some(index) = node_index {
				let k = NoHash;
				column.with_tables_and_self(|tables, s| s.write_existing_value_plan(
					&k,
					tables,
					Address::from_u64(index),
					None,
					writer,
					origin,
				))?;
			}
		}
		self.record_id = record_id;
		btree_index.root = self.root_index.unwrap_or(0);
		btree_index.depth = self.depth;
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
		let db = crate::Db::open_inner(&options, true, false, crate::db::TestDbTarget::Standard, false).unwrap();

		let root = Node::new();
		let mut tree = BTree::<Node<crate::btree::Order2_3>>::new(None, root, 0, record_id);
		let overlays = RwLock::new(LogOverlays::default());
		let mut log_overlay = LogWriter::new(&overlays, record_id);
		for (key, value) in change_set.iter() {
			if let Some(value) = value.as_ref() {
				tree.insert(key, value, db.column(col_nb), &mut log_overlay, origin).unwrap();
			} else {
				tree.remove(key, db.column(col_nb), &mut log_overlay, origin).unwrap();
			}
		}

		let state: BTreeMap<Vec<u8>, Option<Vec<u8>>> = change_set.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		let mut iter = tree.iter();
		for (key, value) in state.iter() {
			assert_eq!(&tree.get(key, db.column(col_nb), &log_overlay).unwrap(), value);
			if let Some(value) = value.as_ref() {
				assert_eq!(iter.next(&mut tree, db.column(col_nb), &log_overlay).unwrap(), Some((key.clone(), value.clone())));
			}
		}
		assert_eq!(iter.next(&mut tree, db.column(col_nb), &log_overlay).unwrap(), None);
		assert!(tree.root.is_balanced(db.column(col_nb), &log_overlay, 0).unwrap());
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

		// TODO big key
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
			let var_keysize = var_keysize as usize % (size / 2);
			key.truncate(key_size - var_keysize);
			data.push((key, value));
		}
		test_basic(data.as_slice());
	}
}
