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

// BTree indexes are stored as node value.
// The header is stored at a fix address, the first offset of the
// first value table that can contain the header.
//
// Header (metadata)
// [ROOT: 8][DEPTH: 4]
//
// ROOT: u64 LE current index for root node.
// DEPTH: u32 LE current tree depth.
//
// Complete entry:
// [CHILD: 8]: Internal index of child node, 0 for undefined (points to the metadata).
// [VALUE_PTR: 8]: Pointer to the value table for this key. If 0, no value (key should be 0 length
// [KEY_HEADER: 1]: header of key, contains length up to 254. If 255, then the length is u32
// encoded on the next 4 bytes.
// [KEY: KEYSIZE]: stored key.
//
// This sequence is repeated ORDER time and a last CHILD index is added.

use node::SeparatorInner;
use crate::table::{Entry as LogEntry, ValueTable};
use crate::table::key::{TableKey, TableKeyQuery};
use crate::options::Options;
use crate::index::Address;
use crate::error::Result;
use crate::column::{ColId, ValueTableOrigin, Column};
use crate::log::{LogQuery, LogWriter};
use crate::compress::Compress;
use crate::btree::node::Node;
pub use btree::BTreeIterator;

mod btree;
mod node;

pub(crate) const HEADER_POSITION: u64 = 0;
pub(crate) const HEADER_SIZE: u64 = 8 + 4;
pub(crate) const ENTRY_CAPACITY: usize = ORDER * 33 + ORDER * 8 + ORDER_CHILD * 8;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct BTreeTableId(u8); // contain its column id

impl BTreeTableId {
	pub fn new(col: ColId) -> Self {
		BTreeTableId(col)
	}

	pub fn col(&self) -> ColId {
		self.0 as ColId
	}

	pub fn file_name(&self) -> String {
		format!("btree_{:02}", self.col())
	}

	pub fn as_u8(&self) -> u8 {
		self.0
	}

	pub fn from_u8(c: u8) -> Self {
		BTreeTableId(c)
	}
}

impl std::fmt::Display for BTreeTableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{:02}", self.col())
	}
}

#[derive(Clone, PartialEq)]
pub struct BTreeIndex {
	pub root: u64,
	pub depth: u32,
}

pub struct BTreeLogOverlay {
	pub index: BTreeIndex,
	pub record_id: u64,
}

impl BTreeLogOverlay {
	pub fn new(record_id: u64, index: BTreeIndex) -> Self {
		BTreeLogOverlay {
			index,
			record_id,
		}
	}
}

const ORDER: usize = 8;
const ORDER_CHILD: usize = ORDER + 1;

struct Entry {
	encoded: LogEntry<Vec<u8>>,
}

impl Entry {
	fn empty() -> Self {
		Self::from_encoded(Vec::with_capacity(ENTRY_CAPACITY))
	}

	fn from_encoded(enc: Vec<u8>) -> Self {
		Entry {
			encoded: LogEntry::new(enc),
		}
	}

	fn read_separator(&mut self) -> Option<SeparatorInner> {
		if self.encoded.offset() == self.encoded.inner_mut().len() {
			return None;
		}
		let value = self.encoded.read_u64();
		let head = self.encoded.read_slice(1);
		let head = head[0];
		let size = if head == u8::MAX {
			self.encoded.read_u32() as usize
		} else {
			head as usize
		};
		let key = self.encoded.read_slice(size).to_vec();
		if value == 0 {
			return None;
		}
		Some(SeparatorInner {
			key,
			value,
		})
	}

	fn write_separator(&mut self, key: &Vec<u8>, value: u64) {
		let size = key.len();
		let inner_size = self.encoded.inner_mut().len();
		if size >= u8::MAX as usize {
			self.encoded.inner_mut().resize(inner_size + 8 + 1 + 4 + size, 0); 
		} else {
			self.encoded.inner_mut().resize(inner_size + 8 + 1 + size, 0); 
		};
		self.encoded.write_u64(value);
		if size >= u8::MAX as usize {
			self.encoded.write_slice(&[u8::MAX]);
			self.encoded.write_u32(size as u32);
		} else {
			self.encoded.write_slice(&[size as u8]);
		}
		self.encoded.write_slice(key.as_slice());
	}

	fn read_child_index(&mut self) -> Option<u64> {
		let index = self.encoded.read_u64();
		if index == 0 {
			None
		} else {
			Some(index)
		}
	}

	fn write_child_index(&mut self, index: u64) {
		let inner_size = self.encoded.inner_mut().len();
		self.encoded.inner_mut().resize(inner_size + 8, 0);
		self.encoded.write_u64(index);
	}

	fn write_header(&mut self, btree_index: &BTreeIndex) {
		self.encoded.set_offset(0);
		self.encoded.inner_mut().resize(8 + 4, 0);
		self.encoded.write_u64(btree_index.root);
		self.encoded.write_u32(btree_index.depth);
	}
}

pub struct BTreeTable {
	pub id: BTreeTableId,
}

impl BTreeTable {
	pub fn open(
		id: BTreeTableId,
	) -> Result<Self> {
		Ok(BTreeTable {
			id,
		})
	}

	fn btree_index_address(values: &Vec<ValueTable>) -> Option<Address> {
		for (tier, v) in values.iter().enumerate() {
			if v.value_size(&TableKey::NoHash).map(|s| s >= HEADER_SIZE as u16).unwrap_or(true) {
				return Some(Address::new(1, tier as u8));
			}
		}
		None
	}

	fn btree_index(log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<BTreeIndex> {
		let mut root = HEADER_POSITION;
		let mut depth = 0;
		if let Some(address) = Self::btree_index_address(values) {
			let key_query = TableKeyQuery::Fetch(None);
			let tier = address.size_tier();
			if values[tier as usize].is_init() {
				if let Some(encoded) = Column::get_at_value_index_locked(key_query, address, values, log, comp)? {
					let mut buf: LogEntry<Vec<u8>> = LogEntry::new(encoded.1);
					root = buf.read_u64();
					depth = buf.read_u32();
				}
			}
		}
		Ok(BTreeIndex {
			root,
			depth,
		})
	}

	pub fn get(&self, key: &[u8], log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<Option<Vec<u8>>> {
		let btree_index = Self::btree_index(log, values, comp)?;
		if btree_index.root == HEADER_POSITION {
			return Ok(None);
		}
		let record_id = 0; // lifetime of Btree is the query, so no invalidate.
		let root = self.get_index(btree_index.root, log, values, comp)?;
		let root = Node::from_encoded(root);
		// keeping log locked when parsing tree.
		let mut tree = btree::BTree::new(Some(btree_index.root), root, btree_index.depth, record_id);
		tree.get_with_lock_no_cache(key, self, values, log, comp)
	}

	fn get_index(&self, at: u64, log: &impl LogQuery, tables: &Vec<ValueTable>, comp: &Compress) -> Result<Vec<u8>> {
		let key_query = TableKeyQuery::Check(&TableKey::NoHash);
		if let Some((_tier, value)) = Column::get_at_value_index_locked(key_query, Address::from_u64(at), tables, log, comp)? {
			Ok(value)
		} else {
			Err(crate::error::Error::Corruption("Missing btree index".to_string()))
		}
	}
}

pub fn new_btree_inner(
	column: &Column,
	log: &impl LogQuery,
	record_id: u64,
) -> Result<(btree::BTree, BTreeTableId)> {
	let (root, root_index, btree_index, table_id) = column.with_value_tables_and_btree(|btree, values, comp| {
		let btree_index = BTreeTable::btree_index(log, values, comp)?;

		let (root_index, root) = if btree_index.root == HEADER_POSITION {
			(None, Node::new())
		} else {
			let root = btree.get_index(btree_index.root, log, values, comp)?;
			(Some(btree_index.root), Node::from_encoded(root))
		};
		Ok((
			root,
			root_index,
			btree_index,
			btree.id,
		))
	})?;
	Ok((
		btree::BTree::new(root_index, root, btree_index.depth, record_id),
		table_id,
	))
}

pub mod commit_overlay {
	use super::*;
	use crate::db::BTreeCommitOverlay;
	use crate::column::{Column, ColId};
	use crate::error::Result;

	pub struct BTreeChangeSet {
		pub col: ColId,
		pub changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
	}

	impl BTreeChangeSet {
		pub fn new(col: ColId) -> Self {
			BTreeChangeSet { col, changes: Default::default() }
		}

		pub fn push(&mut self, k: &[u8], v: Option<Vec<u8>>) {
			// no key hashing
			self.changes.push((k.to_vec(), v));
		}

		pub fn copy_to_overlay(&self, overlay: &mut BTreeCommitOverlay, record_id: u64, bytes: &mut usize, options: &Options) {
			let ref_counted = options.columns[self.col as usize].ref_counted;
			for commit in self.changes.iter() {
				match commit {
					(key, Some(value)) => {
						*bytes += key.len();
						*bytes += value.len();
						overlay.insert(key.clone(), (record_id, Some(value.clone())));
					},
					(key, None) => {
						*bytes += key.len();
						// Don't add removed ref-counted values to overlay.
						// (current ref_counted implementation does not
						// make much sense for btree indexed content).
						if !ref_counted {
							overlay.insert(key.clone(), (record_id, None));
						}
					},
				}
			}
		}

		pub fn clean_overlay(&mut self, overlay: &mut BTreeCommitOverlay, record_id: u64) {
			use std::collections::btree_map::Entry;
			for (key, _) in self.changes.drain(..) {
				if let Entry::Occupied(e) = overlay.entry(key) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}

		pub fn write_plan(
			&self,
			column: &Column,
			writer: &mut LogWriter,
			ops: &mut u64,
		) -> Result<()> {
			let origin = crate::column::ValueTableOrigin::BTree(crate::btree::BTreeTableId::new(self.col));
			// TODO consider runing on btree with ValueTable as params.
			let record_id = writer.record_id();
			// This is not racy as we have a single thread writing plan, so a single btree instance.
			let (mut tree, table_id) = new_btree_inner(column, writer, record_id)?;
			if tree.root_index.is_none() {
				// reserve the header address.
				if let Some(address) = column.with_value_tables(|t| Ok(BTreeTable::btree_index_address(t)))? {
					let new_address = column.with_tables_and_self(|t, s| s.write_new_value_plan(
						&TableKey::NoHash,
						t,
						vec![0; HEADER_SIZE as usize].as_slice(),
						writer,
						origin,
					))?;
					assert!(new_address == address);
				}
			}
			for change in self.changes.iter() {
				match change {
					(key, None) => {
						tree.remove(key, column, writer, origin)?;
					},
					(key, Some(value)) => {
						tree.insert(key, value, column, writer, origin)?;
					},
				}
				*ops += 1;
			}
			let mut btree_index = BTreeIndex {
				root: tree.root_index.unwrap_or(0),
				depth: tree.depth,
			};
			let old_btree_index = btree_index.clone();

			tree.write_plan(column, writer, table_id, record_id, &mut btree_index, origin)?;

			if old_btree_index != btree_index {
				let mut entry = Entry::empty();
				entry.write_header(&btree_index);
				if let Some(address) = column.with_value_tables(|t| Ok(BTreeTable::btree_index_address(t)))? {
					column.with_tables_and_self(|t, s| s.write_existing_value_plan(
						&TableKey::NoHash,
						t,
						address,
						Some(&entry.encoded.as_ref()[..HEADER_SIZE as usize]),
						writer,
						origin,
					))?;
				}
			}
			writer.insert_btree_index(table_id, record_id, btree_index); // TODO only usefull to put record id
			Ok(())
		}
	}
}
