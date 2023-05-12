// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	btree::{btree::BTree, node::Node},
	column::{ColId, Column, TablesRef},
	compress::Compress,
	error::{Error, Result},
	index::Address,
	log::{LogAction, LogQuery, LogReader, LogWriter},
	options::{Metadata, Options, DEFAULT_COMPRESSION_THRESHOLD},
	parking_lot::RwLock,
	table::{
		key::{TableKey, TableKeyQuery},
		Entry as ValueTableEntry, Value, ValueTable,
	},
	Operation,
};
pub use iter::{BTreeIterator, LastIndex, LastKey};
use node::SeparatorInner;

#[allow(clippy::module_inception)]
mod btree;
mod iter;
mod node;

const ORDER: usize = 8;
const ORDER_CHILD: usize = ORDER + 1;
const NULL_ADDRESS: Address = Address::from_u64(0);
const HEADER_SIZE: u64 = 8 + 4;
const HEADER_ADDRESS: Address = {
	debug_assert!(HEADER_SIZE < crate::table::MIN_ENTRY_SIZE as u64);
	Address::new(1, 0)
};
const MAX_KEYSIZE_ENCODED_SIZE: usize = 33;
const ENTRY_CAPACITY: usize = ORDER * MAX_KEYSIZE_ENCODED_SIZE + ORDER * 8 + ORDER_CHILD * 8;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IterDirection {
	Backward,
	Forward,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BTreeHeader {
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
		Entry { encoded: ValueTableEntry::new(enc) }
	}

	fn read_separator(&mut self) -> Result<Option<SeparatorInner>> {
		if self.encoded.offset() == self.encoded.inner_mut().len() {
			return Ok(None)
		}
		self.encoded
			.check_remaining_len(8 + 1, || Error::Corruption("Unaligned separator".into()))?;
		let value = self.encoded.read_u64();
		let head = self.encoded.read_slice(1);
		let head = head[0];
		let size = if head == u8::MAX {
			self.encoded
				.check_remaining_len(4, || Error::Corruption("Cannot read size of key".into()))?;
			self.encoded.read_u32() as usize
		} else {
			head as usize
		};
		self.encoded.check_remaining_len(size, || {
			Error::Corruption(format!("Entry too small for key of len {}", size))
		})?;
		let key = self.encoded.read_slice(size).to_vec();
		if value == 0 {
			return Ok(None)
		}
		let value = Address::from_u64(value);
		Ok(Some(SeparatorInner { key, value }))
	}

	fn write_separator(&mut self, key: &[u8], value: Address) {
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
		self.encoded.write_slice(key);
	}

	fn read_child_index(&mut self) -> Result<Option<Address>> {
		self.encoded
			.check_remaining_len(8, || Error::Corruption("Entry too small for Index".into()))?;
		let index = self.encoded.read_u64();
		Ok(if index == 0 { None } else { Some(Address::from_u64(index)) })
	}

	fn write_child_index(&mut self, index: Address) {
		let inner_size = self.encoded.inner_mut().len();
		self.encoded.inner_mut().resize(inner_size + 8, 0);
		self.encoded.write_u64(index.as_u64());
	}

	fn write_header(&mut self, btree_header: &BTreeHeader) {
		self.encoded.set_offset(0);
		self.encoded.inner_mut().resize(8 + 4, 0);
		self.encoded.write_u64(btree_header.root.as_u64());
		self.encoded.write_u32(btree_header.depth);
	}
}

#[derive(Debug)]
pub struct BTreeTable {
	id: ColId,
	tables: RwLock<Vec<ValueTable>>,
	ref_counted: bool,
	compression: Compress,
}

impl BTreeTable {
	pub fn open(
		id: ColId,
		values: Vec<ValueTable>,
		options: &Options,
		metadata: &Metadata,
	) -> Result<Self> {
		let size_tier = HEADER_ADDRESS.size_tier() as usize;
		if !values[size_tier].is_init() {
			let btree_header = BTreeHeader { root: NULL_ADDRESS, depth: 0 };
			let mut entry = Entry::empty();
			entry.write_header(&btree_header);
			values[size_tier].init_with_entry(&*entry.encoded.inner_mut())?;
		}
		let col_options = &metadata.columns[id as usize];
		Ok(BTreeTable {
			id,
			tables: RwLock::new(values),
			ref_counted: col_options.ref_counted,
			compression: Compress::new(
				col_options.compression,
				options
					.compression_threshold
					.get(&id)
					.copied()
					.unwrap_or(DEFAULT_COMPRESSION_THRESHOLD),
			),
		})
	}

	fn btree_header(log: &impl LogQuery, values: TablesRef) -> Result<BTreeHeader> {
		let mut root = NULL_ADDRESS;
		let mut depth = 0;
		let key_query = TableKeyQuery::Fetch(None);
		if let Some(encoded) = Column::get_value(key_query, HEADER_ADDRESS, values, log)? {
			let mut buf: ValueTableEntry<Vec<u8>> = ValueTableEntry::new(encoded.1);
			buf.check_remaining_len(8 + 4, || Error::Corruption("Invalid header length.".into()))?;
			root = Address::from_u64(buf.read_u64());
			depth = buf.read_u32();
		}
		Ok(BTreeHeader { root, depth })
	}

	fn get_at_value_index(
		&self,
		key: TableKeyQuery,
		address: Address,
		log: &impl LogQuery,
	) -> Result<Option<(u8, Value)>> {
		let tables = self.tables.read();
		let btree = self.locked(&tables);
		Column::get_value(key, address, btree, log)
	}

	pub fn flush(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.iter() {
			t.flush()?;
		}
		Ok(())
	}

	pub fn get(key: &[u8], log: &impl LogQuery, values: TablesRef) -> Result<Option<Vec<u8>>> {
		let btree_header = Self::btree_header(log, values)?;
		if btree_header.root == NULL_ADDRESS {
			return Ok(None)
		}
		let record_id = 0; // lifetime of Btree is the query, so no invalidate.
				   // keeping log locked when parsing tree.
		let tree = BTree::new(Some(btree_header.root), btree_header.depth, record_id);
		tree.get(key, values, log)
	}

	fn get_encoded_entry(at: Address, log: &impl LogQuery, tables: TablesRef) -> Result<Vec<u8>> {
		let key_query = TableKeyQuery::Check(&TableKey::NoHash);
		if let Some((_tier, value)) = Column::get_value(key_query, at, tables, log)? {
			Ok(value)
		} else {
			Err(Error::Corruption(format!("Missing btree entry at {at}")))
		}
	}

	fn locked<'a>(&'a self, tables: &'a [ValueTable]) -> TablesRef<'a> {
		TablesRef {
			tables,
			ref_counted: self.ref_counted,
			preimage: false,
			compression: &self.compression,
			col: self.id,
		}
	}

	pub fn with_locked<R>(&self, mut apply: impl FnMut(TablesRef) -> Result<R>) -> Result<R> {
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
			_ => {
				log::error!(target: "parity-db", "Unexpected log action");
				return Err(Error::Corruption("Unexpected log action".to_string()))
			},
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

	fn write_plan_remove_node(
		tables: TablesRef,
		writer: &mut LogWriter,
		node_index: Address,
	) -> Result<()> {
		Column::write_existing_value_plan::<_, Vec<u8>>(
			&TableKey::NoHash,
			tables,
			node_index,
			&Operation::Dereference(()),
			writer,
			None,
			false,
		)?;
		Ok(())
	}

	fn write_node_plan(
		mut tables: TablesRef,
		mut node: Node,
		writer: &mut LogWriter,
		node_id: Option<Address>,
	) -> Result<Option<Address>> {
		for child in node.children.as_mut().iter_mut() {
			if child.moved {
				node.changed = true;
			} else if child.entry_index.is_none() {
				break
			}
		}

		for separator in node.separators.as_mut().iter_mut() {
			if separator.modified {
				node.changed = true;
			} else if separator.separator.is_none() {
				break
			}
		}

		if !node.changed {
			return Ok(None)
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
				break
			}
			if let Some(sep) = &node.separators.as_mut()[i_separator].separator {
				entry.write_separator(&sep.key, sep.value);
				i_separator += 1
			} else {
				break
			}
		}

		let old_comp = tables.compression;
		tables.compression = &crate::compress::NO_COMPRESSION;
		let result = Ok(if let Some(existing) = node_id {
			let k = TableKey::NoHash;
			if let (_, Some(new_index)) = Column::write_existing_value_plan(
				&k,
				tables,
				existing,
				&Operation::Set((), entry.encoded),
				writer,
				None,
				false,
			)? {
				Some(new_index)
			} else {
				None
			}
		} else {
			let k = TableKey::NoHash;
			Some(Column::write_new_value_plan(&k, tables, entry.encoded.as_ref(), writer, None)?)
		});
		tables.compression = old_comp;

		result
	}
}

pub mod commit_overlay {
	use super::*;
	use crate::{
		column::{ColId, Column},
		db::{BTreeCommitOverlay, Operation, RcKey, RcValue},
		error::Result,
	};

	#[derive(Debug)]
	pub struct BTreeChangeSet {
		pub col: ColId,
		pub changes: Vec<Operation<RcKey, RcValue>>,
	}

	impl BTreeChangeSet {
		pub fn new(col: ColId) -> Self {
			BTreeChangeSet { col, changes: Default::default() }
		}

		pub fn push(&mut self, change: Operation<Value, Value>) {
			// No key hashing
			self.changes.push(match change {
				Operation::Set(k, v) => Operation::Set(k.into(), v.into()),
				Operation::Dereference(k) => Operation::Dereference(k.into()),
				Operation::Reference(k) => Operation::Reference(k.into()),
			});
		}

		pub fn copy_to_overlay(
			&self,
			overlay: &mut BTreeCommitOverlay,
			record_id: u64,
			bytes: &mut usize,
			options: &Options,
		) -> Result<()> {
			let ref_counted = options.columns[self.col as usize].ref_counted;
			for change in self.changes.iter() {
				match change {
					Operation::Set(key, value) => {
						*bytes += key.value().len();
						*bytes += value.value().len();
						overlay.insert(key.clone(), (record_id, Some(value.clone())));
					},
					Operation::Dereference(key) => {
						// Don't add removed ref-counted values to overlay.
						// (current ref_counted implementation does not
						// make much sense for btree indexed content).
						if !ref_counted {
							*bytes += key.value().len();
							overlay.insert(key.clone(), (record_id, None));
						}
					},
					Operation::Reference(..) => {
						// Don't add (we allow remove value in overlay when using rc: some
						// indexing on top of it is expected).
						if !ref_counted {
							return Err(Error::InvalidInput(format!(
								"No Rc for column {}",
								self.col
							)))
						}
					},
				}
			}
			Ok(())
		}

		pub fn clean_overlay(&mut self, overlay: &mut BTreeCommitOverlay, record_id: u64) {
			use std::collections::btree_map::Entry;
			for change in self.changes.drain(..) {
				let key = change.into_key();
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
			let record_id = writer.record_id();

			let locked_tables = btree.tables.read();
			let locked = btree.locked(&locked_tables);
			let mut tree = BTree::open(locked, writer, record_id)?;

			let mut btree_header =
				BTreeHeader { root: tree.root_index.unwrap_or(NULL_ADDRESS), depth: tree.depth };
			let old_btree_header = btree_header.clone();

			self.changes.sort();
			tree.write_sorted_changes(self.changes.as_slice(), locked, writer)?;
			*ops += self.changes.len() as u64;

			btree_header.root = tree.root_index.unwrap_or(NULL_ADDRESS);
			btree_header.depth = tree.depth;

			if old_btree_header != btree_header {
				let mut entry = Entry::empty();
				entry.write_header(&btree_header);
				Column::write_existing_value_plan(
					&TableKey::NoHash,
					locked,
					HEADER_ADDRESS,
					&Operation::Set((), &entry.encoded.as_ref()[..HEADER_SIZE as usize]),
					writer,
					None,
					false,
				)?;
			}
			#[cfg(test)]
			tree.is_balanced(locked, writer)?;
			Ok(())
		}
	}
}
