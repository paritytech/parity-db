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
use crate::table::{Entry as LogEntry, ValueTable, Value};
use crate::table::key::{TableKey, TableKeyQuery};
use crate::options::Options;
use crate::index::Address;
use crate::error::Result;
use crate::column::{ColId, ValueTableOrigin, Column, TableLocked};
use crate::log::{LogQuery, LogWriter, LogAction, LogReader};
use crate::compress::Compress;
use crate::btree::node::Node;
use parking_lot::RwLock;
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
	id: BTreeTableId, // TODO not pub
	tables: RwLock<Vec<ValueTable>>,
	preimage: bool,
	ref_counted: bool,
	compression: Compress, // TODO do not compress btree nodes!!!
}

impl BTreeTable {
	pub fn open(
		id: BTreeTableId,
		values: Vec<ValueTable>,
		metadata: &crate::options::Metadata,
	) -> Result<Self> {
		if let Some(address) = Self::btree_index_address(&values) {
			let size_tier = address.size_tier() as usize;
			if !values[size_tier].is_init() {
				let btree_index = BTreeIndex {
					root: HEADER_POSITION,
					depth: 0,
				};
				let mut entry = Entry::empty();
				entry.write_header(&btree_index);
				values[size_tier].init_with_entry(&*entry.encoded.inner_mut())?;
			}
		}
		let col = id.col();
		let options = &metadata.columns[col as usize];
		Ok(BTreeTable {
			id,
			tables: RwLock::new(values),
			preimage: options.preimage,
			ref_counted: options.ref_counted,
			compression: Compress::new(options.compression, options.compression_treshold),
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

	fn btree_index(log: &impl LogQuery, values: TableLocked) -> Result<BTreeIndex> {
		let mut root = HEADER_POSITION;
		let mut depth = 0;
		if let Some(address) = Self::btree_index_address(values.tables) {
			let key_query = TableKeyQuery::Fetch(None);
			if let Some(encoded) = Column::get_at_value_index_locked(key_query, address, values, log)? {
				let mut buf: LogEntry<Vec<u8>> = LogEntry::new(encoded.1);
				root = buf.read_u64();
				depth = buf.read_u32();
			}
		}
		Ok(BTreeIndex {
			root,
			depth,
		})
	}

	pub(crate) fn get_at_value_index(&self, key: TableKeyQuery, address: Address, log: &impl LogQuery) -> Result<Option<(u8, Value)>> {
		let tables = self.tables.read();
		let btree = self.locked(&*tables);
		Column::get_at_value_index_locked(key, address, btree, log)
	}

	pub fn flush(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.iter() {
			t.flush()?;
		}
		Ok(())
	}

	pub fn get(key: &[u8], log: &impl LogQuery, values: TableLocked) -> Result<Option<Vec<u8>>> {
		let btree_index = Self::btree_index(log, values)?;
		if btree_index.root == HEADER_POSITION {
			return Ok(None);
		}
		let record_id = 0; // lifetime of Btree is the query, so no invalidate.
		let root = Self::get_index(btree_index.root, log, values)?;
		let root = Node::from_encoded(root);
		// keeping log locked when parsing tree.
		let mut tree = btree::BTree::new(Some(btree_index.root), root, btree_index.depth, record_id);
		tree.get_with_lock_no_cache(key, values, log)
	}

	fn get_index(at: u64, log: &impl LogQuery, tables: TableLocked) -> Result<Vec<u8>> {
		let key_query = TableKeyQuery::Check(&TableKey::NoHash);
		if let Some((_tier, value)) = Column::get_at_value_index_locked(key_query, Address::from_u64(at), tables, log)? {
			Ok(value)
		} else {
			Err(crate::error::Error::Corruption("Missing btree index".to_string()))
		}
	}

	pub(crate) fn locked<'a>(&'a self, tables: &'a Vec<ValueTable>) -> TableLocked<'a> {
		TableLocked {
			tables,
			preimage: self.preimage,
			ref_counted: self.ref_counted,
			compression: &self.compression,
		}
	}

	pub(crate) fn with_locked<R>(&self, mut apply: impl FnMut(TableLocked) -> Result<R>) -> Result<R> {
		let locked_tables = &*self.tables.read();
		let locked = self.locked(locked_tables);
		apply(locked)
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.read();
		match action {
			LogAction::InsertValue(record) => {
				tables[record.table.size_tier() as usize].enact_plan(record.index, log)?;
			},
			_ => panic!("Unexpected log action"),
		}
		Ok(())
	}

	pub fn validate_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.upgradable_read();
		match action {
			LogAction::InsertValue(record) => {
				tables[record.table.size_tier() as usize].validate_plan(record.index, log)?;
			},
			_ => panic!("Unexpected log action"),
		}
		Ok(())
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.iter() {
			t.complete_plan(log)?;
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.iter() {
			t.refresh_metadata()?;
		}
		Ok(())
	}
}

pub fn new_btree_inner(
	values: TableLocked,
	log: &impl LogQuery,
	record_id: u64,
) -> Result<btree::BTree> {
	let btree_index = BTreeTable::btree_index(log, values)?;

	let (root_index, root) = if btree_index.root == HEADER_POSITION {
		(None, Node::new())
	} else {
		let root = BTreeTable::get_index(btree_index.root, log, values)?;
		(Some(btree_index.root), Node::from_encoded(root))
	};
	Ok(btree::BTree::new(root_index, root, btree_index.depth, record_id))
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
			btree: &BTreeTable,
			writer: &mut LogWriter,
			ops: &mut u64,
		) -> Result<()> {
			let origin = crate::column::ValueTableOrigin::BTree(btree.id);
			let record_id = writer.record_id();
			// This is racy but we have a single thread writing plan, so only a single writing btree at a
			// time.
			let mut tree = btree.with_locked(|btree| new_btree_inner(btree, writer, record_id))?;
			for change in self.changes.iter() {
				match change {
					(key, None) => {
						tree.remove(key, btree, writer, origin)?;
					},
					(key, Some(value)) => {
						tree.insert(key, value, btree, writer, origin)?;
					},
				}
				*ops += 1;
			}
			let mut btree_index = BTreeIndex {
				root: tree.root_index.unwrap_or(0),
				depth: tree.depth,
			};
			let old_btree_index = btree_index.clone();

			tree.write_plan(btree, writer, record_id, &mut btree_index, origin)?;

			if old_btree_index != btree_index {
				let mut entry = Entry::empty();
				entry.write_header(&btree_index);
				btree.with_locked(|btree| {
					if let Some(address) = BTreeTable::btree_index_address(btree.tables) {
						Column::write_existing_value_plan(
							&TableKey::NoHash,
							btree,
							address,
							Some(&entry.encoded.as_ref()[..HEADER_SIZE as usize]),
							writer,
							origin,
							None,
						)?;
					}
					Ok(())
				})?;
			}
			Ok(())
		}
	}
}
