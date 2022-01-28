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
// [CHILD: 8]: Child node address, 0 for undefined (points to the metadata).
// [VALUE_PTR: 8]: Address of the value for this key. 0, for no value.
// [KEY_HEADER: 1]: Header of key, contains length up to 254. If 255, then the length is u32
// encoded on the next 4 bytes.
// [KEY: KEYSIZE]: stored key.
//
// This sequence is repeated ORDER times, followed by an additional CHILD index.

use node::SeparatorInner;
use crate::table::{Entry as ValueTableEntry, ValueTable, Value};
use crate::table::key::{TableKey, TableKeyQuery};
use crate::options::Options;
use crate::index::Address;
use crate::error::Result;
use crate::column::{ColId, ValueTableOrigin, Column, TablesRef};
use crate::log::{LogQuery, LogWriter, LogAction, LogReader};
use crate::compress::Compress;
use crate::btree::node::Node;
use crate::btree::btree::BTree;
use parking_lot::RwLock;
pub use btree::BTreeIterator;

mod btree;
mod node;

const ORDER: usize = 8;
const ORDER_CHILD: usize = ORDER + 1;
pub(crate) const NULL_ADDRESS: Address = Address::from_u64(0);
pub(crate) const HEADER_SIZE: u64 = 8 + 4;
pub(crate) const HEADER_ADDRESS: Address = {
	debug_assert!(HEADER_SIZE < crate::table::MIN_ENTRY_SIZE as u64);
	Address::new(1, 0)
};
pub(crate) const ENTRY_CAPACITY: usize = ORDER * 33 + ORDER * 8 + ORDER_CHILD * 8;

#[derive(Clone, PartialEq)]
pub struct BTreeIndex {
	pub root: Address,
	pub depth: u32,
}

struct Entry {
	encoded: ValueTableEntry<Vec<u8>>,
}

impl Entry {
	fn empty() -> Self {
		Self::from_encoded(Vec::with_capacity(ENTRY_CAPACITY))
	}

	fn from_encoded(enc: Vec<u8>) -> Self {
		Entry {
			encoded: ValueTableEntry::new(enc),
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
		let value = Address::from_u64(value);
		Some(SeparatorInner {
			key,
			value,
		})
	}

	fn write_separator(&mut self, key: &Vec<u8>, value: Address) {
		let size = key.len();
		let inner_size = self.encoded.inner_mut().len();
		if size >= u8::MAX as usize {
			self.encoded.inner_mut().resize(inner_size + 8 + 1 + 4 + size, 0); 
		} else {
			self.encoded.inner_mut().resize(inner_size + 8 + 1 + size, 0); 
		};
		self.encoded.write_u64(value.as_u64());
		if size >= u8::MAX as usize {
			self.encoded.write_slice(&[u8::MAX]);
			self.encoded.write_u32(size as u32);
		} else {
			self.encoded.write_slice(&[size as u8]);
		}
		self.encoded.write_slice(key.as_slice());
	}

	fn read_child_index(&mut self) -> Option<Address> {
		let index = self.encoded.read_u64();
		if index == 0 {
			None
		} else {
			Some(Address::from_u64(index))
		}
	}

	fn write_child_index(&mut self, index: Address) {
		let inner_size = self.encoded.inner_mut().len();
		self.encoded.inner_mut().resize(inner_size + 8, 0);
		self.encoded.write_u64(index.as_u64());
	}

	fn write_header(&mut self, btree_index: &BTreeIndex) {
		self.encoded.set_offset(0);
		self.encoded.inner_mut().resize(8 + 4, 0);
		self.encoded.write_u64(btree_index.root.as_u64());
		self.encoded.write_u32(btree_index.depth);
	}
}

pub struct BTreeTable {
	id: ColId,
	tables: RwLock<Vec<ValueTable>>,
	preimage: bool,
	ref_counted: bool,
	compression: Compress,
}

impl BTreeTable {
	pub fn open(
		id: ColId,
		values: Vec<ValueTable>,
		metadata: &crate::options::Metadata,
	) -> Result<Self> {
		let size_tier = HEADER_ADDRESS.size_tier() as usize;
		if !values[size_tier].is_init() {
			let btree_index = BTreeIndex {
				root: NULL_ADDRESS,
				depth: 0,
			};
			let mut entry = Entry::empty();
			entry.write_header(&btree_index);
			values[size_tier].init_with_entry(&*entry.encoded.inner_mut())?;
		}
		let options = &metadata.columns[id as usize];
		Ok(BTreeTable {
			id,
			tables: RwLock::new(values),
			preimage: options.preimage,
			ref_counted: options.ref_counted,
			compression: Compress::new(options.compression, options.compression_treshold),
		})
	}

	fn btree_index(log: &impl LogQuery, values: TablesRef) -> Result<BTreeIndex> {
		let mut root = NULL_ADDRESS;
		let mut depth = 0;
		let key_query = TableKeyQuery::Fetch(None);
		if let Some(encoded) = Column::get_at_value_index(key_query, HEADER_ADDRESS, values, log)? {
			let mut buf: ValueTableEntry<Vec<u8>> = ValueTableEntry::new(encoded.1);
			root = Address::from_u64(buf.read_u64());
			depth = buf.read_u32();
		}
		Ok(BTreeIndex {
			root,
			depth,
		})
	}

	fn get_at_value_index(&self, key: TableKeyQuery, address: Address, log: &impl LogQuery) -> Result<Option<(u8, Value)>> {
		let tables = self.tables.read();
		let btree = self.locked(&*tables);
		Column::get_at_value_index(key, address, btree, log)
	}

	pub fn flush(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.iter() {
			t.flush()?;
		}
		Ok(())
	}

	pub fn get(key: &[u8], log: &impl LogQuery, values: TablesRef) -> Result<Option<Vec<u8>>> {
		let btree_index = Self::btree_index(log, values)?;
		if btree_index.root == NULL_ADDRESS {
			return Ok(None);
		}
		let record_id = 0; // lifetime of Btree is the query, so no invalidate.
		// keeping log locked when parsing tree.
		let mut tree = BTree::new(Some(btree_index.root), btree_index.depth, record_id);
		tree.get(key, values, log)
	}

	fn get_index(at: Address, log: &impl LogQuery, tables: TablesRef) -> Result<Vec<u8>> {
		let key_query = TableKeyQuery::Check(&TableKey::NoHash);
		if let Some((_tier, value)) = Column::get_at_value_index(key_query, at, tables, log)? {
			Ok(value)
		} else {
			Err(crate::error::Error::Corruption("Missing btree index".to_string()))
		}
	}

	fn locked<'a>(&'a self, tables: &'a Vec<ValueTable>) -> TablesRef<'a> {
		TablesRef {
			tables,
			preimage: self.preimage,
			ref_counted: self.ref_counted,
			compression: &self.compression,
		}
	}

	pub(crate) fn with_locked<R>(&self, mut apply: impl FnMut(TablesRef) -> Result<R>) -> Result<R> {
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

	pub fn write_plan(
		tables: TablesRef,
		btree: &mut BTree,
		writer: &mut LogWriter,
		record_id: u64,
		btree_index: &mut BTreeIndex,
		origin: ValueTableOrigin,
	) -> Result<()> {
		let root = BTree::fetch_root(btree.root_index.unwrap_or(NULL_ADDRESS), tables, writer)?;
		if let Some(ix) = Self::write_plan_node(tables, root, writer, btree.root_index, origin)? {
			btree.root_index = Some(ix);
		}
		btree.record_id = record_id;
		btree_index.root = btree.root_index.unwrap_or(NULL_ADDRESS);
		btree_index.depth = btree.depth;
		Ok(())
	}

	fn write_plan_remove_node(
		tables: TablesRef,
		writer: &mut LogWriter,
		node_index: Address,
		origin: ValueTableOrigin,
	) -> Result<()> {
		Column::write_existing_value_plan(
			&TableKey::NoHash,
			tables,
			node_index,
			None,
			writer,
			origin,
			None,
		)?;
		Ok(())
	}

	fn write_plan_node(
		mut tables: TablesRef,
		mut node: Node,
		writer: &mut LogWriter,
		node_id: Option<Address>,
		origin: ValueTableOrigin,
	) -> Result<Option<Address>> {
		for child in node.children.as_mut().iter_mut() {
			if child.moved {
				node.changed = true;
			}
		}

		for separator in node.separators.as_mut().iter_mut() {
			if separator.modified {
				node.changed = true;
			}
		}

		if !node.changed {
			return Ok(None);
		}

		let mut entry = Entry::empty();
		let mut i_children = 0;
		let mut i_separator = 0;
		loop {
			if let Some(index) = node.children.as_mut()[i_children].entry_index {
				entry.write_child_index(index);
			} else {
				entry.write_child_index(NULL_ADDRESS);
			}
			i_children += 1;
			if i_children == ORDER_CHILD {
				break;
			}
			if let Some(sep) = &node.separators.as_mut()[i_separator].separator {
				entry.write_separator(&sep.key, sep.value);
				i_separator += 1
			} else {
				break;
			}
		}

		let old_comp = tables.compression;
		tables.compression = &crate::compress::NO_COMPRESSION;
		let mut result = None;
		if let Some(existing) = node_id {
			let k = TableKey::NoHash;
			if let (_, Some(new_index)) = Column::write_existing_value_plan(
				&k,
				tables,
				existing,
				Some(entry.encoded.as_ref()),
				writer,
				origin,
				None,
			)? {
				result = Some(new_index)
			}
		} else {
			let k = TableKey::NoHash;
			result = Some(Column::write_new_value_plan(
				&k,
				tables,
				entry.encoded.as_ref(),
				writer,
				origin,
				None,
			)?);
		}
		tables.compression = old_comp;

		Ok(result)
	}
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
			// No key hashing
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
			&mut self,
			btree: &BTreeTable,
			writer: &mut LogWriter,
			ops: &mut u64,
		) -> Result<()> {
			let origin = crate::column::ValueTableOrigin::BTree(btree.id);
			let record_id = writer.record_id();

			let locked_tables = btree.tables.read();
			let locked = btree.locked(&*locked_tables);
			let mut tree = BTree::open(locked, writer, record_id)?;

			let mut btree_index = BTreeIndex {
				root: tree.root_index.unwrap_or(NULL_ADDRESS),
				depth: tree.depth,
			};
			let old_btree_index = btree_index.clone();

			self.changes.sort_by_key(|(k, _)| k.clone());
			tree.write_sorted_changes(&mut self.changes.as_slice(), locked, writer, origin)?;
			*ops += self.changes.len() as u64;
			BTreeTable::write_plan(locked, &mut tree, writer, record_id, &mut btree_index, origin)?;

			if old_btree_index != btree_index {
				let mut entry = Entry::empty();
				entry.write_header(&btree_index);
				Column::write_existing_value_plan(
					&TableKey::NoHash,
					locked,
					HEADER_ADDRESS,
					Some(&entry.encoded.as_ref()[..HEADER_SIZE as usize]),
					writer,
					origin,
					None,
				)?;
			}
			Ok(())
		}
	}
}
