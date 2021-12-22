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

// On disk layout for indexed btree table.
// BTree indexes are stored in a single file.
//
// Entry 0 (metadata)
// [LAST_REMOVED: 8][FILLED: 8][ROOT: 8][DEPTH: 4]
//
// LAST_REMOVED - 64-bit index of removed entries linked list head
// FILLED - highest index filled with live data
// ROOT: u64 LE current index for root node. TODO could be smaller than u64
// DEPTH: u32 LE current tree depth.
//
// Complete entry: TODO consider packing child.
// [CHILD: 8]: Internal index of child node, 0 for undefined (points to the metadata).
// TODO consider u32?
// [VALUE_PTR: 8]: Pointer to the value table for this key. If 0, no value (key should be 0 length
// [KEY_HEADER: 1]: header of key, contains length up to 254. 255 indicate that key is stored with
// value.
// [KEY: KEYSIZE - KEY_HEADER]: stored key.
// too).
//
// This sequence is repeated ORDER time and a last CHILD index is added, so total size is:
// (ORDER * (KEYSIZE + 8 + 1)) + ((ORDER + 1) * (8))

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use node::{NodeT, SeparatorInner};
use crate::table::{Entry as LogEntry, ValueTable};
use crate::file::{TableFile, TableFileId};
use crate::options::Options;
use std::sync::atomic::Ordering;
use std::io::Read;
use crate::error::Result;
use crate::column::{ColId, ValueTableOrigin};
use crate::log::{LogQuery, LogReader, LogWriter};
use std::collections::HashMap;
use crate::compress::Compress;
use crate::btree::node::{Node, NodeReadNoCache};
pub use btree::BTreeIterator;

mod btree;
mod node;

pub(crate) const HEADER_POSITION: u64 = 0;
pub(crate) const HEADER_ENTRIES: u8 = 1;

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
	pub last_removed: u64,
	pub filled: u64,
}

pub struct BTreeLogOverlay {
	pub map: HashMap<u64, (u64, Vec<u8>), crate::IdentityBuildHasherU64>, // index -> (record_id, entry)
	pub index: BTreeIndex,
	pub record_id: u64,
}

impl BTreeLogOverlay {
	pub fn new(record_id: u64, index: BTreeIndex) -> Self {
		BTreeLogOverlay {
			map: Default::default(),
			index,
			record_id,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConfigVariants {
	Order2_3_64 = 0,
	// TODO
}

impl From<u8> for ConfigVariants {
	fn from(i: u8) -> Self {
		match i {
			0 => ConfigVariants::Order2_3_64,
			_ => panic!("Unsupported btree index variant"),
		}
	}
}

pub trait Config: Sized {
	// actualy keysize is - 1 (header is include in this value).
	const KEYSIZE: usize; // Max supported value is 254
	const ORDER: usize;
	const ORDER_CHILD: usize = Self::ORDER + 1;
	const ENTRY_SIZE: usize = (Self::ORDER * (Self::KEYSIZE + 8)  + (Self::ORDER_CHILD) * 8);
	type Separators: Default + AsRef<[node::Separator<Self>]> + AsMut<[node::Separator<Self>]>;
	type Children: Default + AsRef<[node::Child<Self>]> + AsMut<[node::Child<Self>]>;
	type Encoded: AsRef<[u8]> + AsMut<[u8]>;
	type KeyBuf: AsRef<[u8]> + AsMut<[u8]>;
	fn default_encoded() -> Self::Encoded;
	fn uninit_encoded() -> Self::Encoded;
	fn default_keybuf() -> Self::KeyBuf;
}

macro_rules! def_config {
	($name:ident, $order:expr, $key_size:expr) => {
		pub struct $name;

		impl Config for $name {
			const KEYSIZE: usize = $key_size;
			const ORDER: usize = $order;
			type Separators = [node::Separator<Self>; Self::ORDER];
			type Children = [node::Child<Self>; Self::ORDER_CHILD];
			type Encoded = [u8; Self::ENTRY_SIZE];
			type KeyBuf = [u8; Self::KEYSIZE];
			fn default_encoded() -> Self::Encoded {
				[0; Self::ENTRY_SIZE]
			}
			fn uninit_encoded() -> Self::Encoded {
				unsafe { MaybeUninit::uninit().assume_init() }
			}
			fn default_keybuf() -> Self::KeyBuf {
				[0; Self::KEYSIZE]
			}
		}
	};
}

def_config!(Order2_3_64, 2, 64);
def_config!(Order2_3_128, 2, 128);
def_config!(Order2_3_256, 2, 254);

struct Entry<C: Config> {
	encoded: LogEntry<C::Encoded>,
	_ph: PhantomData<C>,
}

impl<C: Config> Entry<C> {
	fn empty() -> Self {
		Self::from_encoded(C::default_encoded())
	}

	fn from_encoded(enc: C::Encoded) -> Self {
		Entry {
			encoded: LogEntry::new(enc),
			_ph: PhantomData,
		}
	}

	fn read_has_separator(&mut self, at: usize) -> bool {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start + 8);
		let value = self.encoded.read_u64();
		value != 0
	}

	fn read_separator(&mut self, at: usize) -> Option<SeparatorInner<C>> {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start + 8);
		let value = self.encoded.read_u64();
		let key_slice = self.encoded.read_slice(C::KEYSIZE);
		if value == 0 {
			return None;
		}
		let is_splitted = key_slice[0] == u8::MAX;
		let size = (key_slice[0] & 0b0111_1111) as usize;
		let mut key_buf = C::default_keybuf();
		if is_splitted {
			key_buf.as_mut()[..].copy_from_slice(key_slice.as_ref());
		} else {
			key_buf.as_mut()[..size + 1].copy_from_slice(&key_slice.as_ref()[..size + 1]);
		};
		Some(SeparatorInner {
			key: key_buf,
			splitted_key: None,
			value,
		})
	}

	fn write_separator(&mut self, at: usize, key: &C::KeyBuf, value: u64) {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start + 8);
		self.encoded.write_u64(value);
		let head = key.as_ref()[0];
		if head == u8::MAX {
			self.encoded.write_slice(key.as_ref());
		} else {
			self.encoded.write_slice(&key.as_ref()[..1 + head as usize]);
		}
	}

	fn remove_separator(&mut self, at: usize) {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start + 8);
		self.encoded.write_u64(HEADER_POSITION);
		self.encoded.write_slice(&[0]);
	}

	fn read_child_index(&mut self, at: usize) -> Option<u64> {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start);
		let index = self.encoded.read_u64();
		if index == 0 {
			None
		} else {
			Some(index)
		}
	}

	fn write_child_index(&mut self, at: usize, index: u64) {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start);
		self.encoded.write_u64(index);
	}

	fn remove_child_index(&mut self, at: usize) {
		let start = at * (8 + 8 + C::KEYSIZE);
		self.encoded.set_offset(start);
		self.encoded.write_u64(HEADER_POSITION);
	}

	fn write_header(&mut self, btree_index: &BTreeIndex) {
		self.encoded.set_offset(0);
		self.encoded.write_u64(btree_index.last_removed);
		self.encoded.write_u64(btree_index.filled);
		self.encoded.write_u64(btree_index.root);
		self.encoded.write_slice(&btree_index.depth.to_le_bytes()[..]);
	}

	fn write_removed(&mut self, last_removed: u64) {
		self.encoded.set_offset(0);
		self.encoded.write_u64(last_removed);
	}
}

pub struct BTreeTable {
	pub id: BTreeTableId,
	file: TableFile,
	variant: ConfigVariants,
	initial_index: BTreeIndex,
}

impl BTreeTable {
	pub fn open(
		path: &std::path::PathBuf,
		id: BTreeTableId,
		variant: ConfigVariants,
	) -> Result<Self> {
		match variant {
			ConfigVariants::Order2_3_64 => {
				Self::open_inner::<Order2_3_64>(path, id, variant)
			},
		}
	}

	fn open_inner<C: Config>(
		path: &std::path::PathBuf,
		id: BTreeTableId,
		variant: ConfigVariants,
	) -> Result<Self> {
		let mut filepath: std::path::PathBuf = std::path::PathBuf::clone(path);
		filepath.push(id.file_name());
		let file = TableFile::open(filepath, C::ENTRY_SIZE as u64, TableFileId::BTree(id))?;

		let mut last_removed = HEADER_POSITION;
		let mut filled = HEADER_ENTRIES as u64;
		let mut root = HEADER_POSITION;
		let mut depth = 0;
		if let Some(file) = &mut *file.file.write() {
			let mut buf: LogEntry<C::Encoded> = LogEntry::new_uninit();
			file.read_exact(buf.as_mut())?;
			last_removed = buf.read_u64();
			filled = buf.read_u64();
			root = buf.read_u64();
			depth = buf.read_u32();
		}

		Ok(BTreeTable {
			id,
			file,
			variant,
			initial_index: BTreeIndex {
				last_removed,
				filled,
				root,
				depth,
			},
		})
	}

	pub fn flush(&self) -> Result<()> {
		self.file.flush()
	}

	pub fn get(&self, key: &[u8], log: &impl LogQuery, values: &Vec<ValueTable>, comp: &Compress) -> Result<Option<Vec<u8>>> {
		match self.variant {
			ConfigVariants::Order2_3_64 => {
				self.get_inner::<NodeReadNoCache<Order2_3_64>, _>(key, log, values, comp)
			}
		}
	}
	fn get_inner<N: NodeT, Q: LogQuery>(&self, key: &[u8], log: &Q, values: &Vec<ValueTable>, comp: &Compress) -> Result<Option<Vec<u8>>> {
		let (root_index, depth) = if let Some(btree_index) = log.btree_index(self.id) {
			(btree_index.root, btree_index.depth)
		} else {
			(self.initial_index.root, self.initial_index.depth)
		};
		if root_index == HEADER_POSITION {
			return Ok(None);
		}
		let record_id = 0; // lifetime of Btree is the query, so no invalidate.
		let root = self.get_index::<N, Q>(root_index, log)?;
		let root = N::from_encoded(root);
		// keeping log locked when parsing tree.
		let mut tree = btree::BTree::<N>::new(Some(root_index), root, depth, record_id);
		tree.get_with_lock(key, self, values, log, comp)
	}

	fn get_index<N: NodeT, Q: LogQuery>(&self, at: u64, log: &Q) -> Result<<N::Config as Config>::Encoded> {
		let mut dest = N::Config::uninit_encoded();
		if !log.btree(self.id, at, dest.as_mut()) {
			self.file.read_at(dest.as_mut(), at * N::Config::ENTRY_SIZE as u64)?;
		}
		Ok(dest)
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		match self.variant {
			ConfigVariants::Order2_3_64 => {
				self.enact_plan_inner::<Node<Order2_3_64>>(index, log)
			}
		}
	}
	fn enact_plan_inner<N: NodeT>(&self, index: u64, log: &mut LogReader) -> Result<()> {
		while index >= self.file.capacity.load(Ordering::Relaxed) {
			self.file.grow(N::Config::ENTRY_SIZE as u64)?;
		}
		let mut buf = N::Config::uninit_encoded();
		log.read(buf.as_mut())?;

		self.file.write_at(buf.as_ref(), index * N::Config::ENTRY_SIZE as u64)?;

		log::trace!(target: "parity-db", "{}: Enacted btree {}: {:x?}", self.id, index, buf.as_ref());
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		match self.variant {
			ConfigVariants::Order2_3_64 => {
				self.validate_plan_inner::<Order2_3_64>(index, log)
			}
		}
	}
	fn validate_plan_inner<C: Config> (&self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut buf = C::uninit_encoded();
		log.read(buf.as_mut())?;
		log::trace!(target: "parity-db", "{}: Validated btree in slot {}", self.id, index);
		Ok(())
	}

	pub fn complete_plan(&self, _log: &mut LogWriter) -> Result<()> {
		Ok(())
	}
}

pub fn new_btree_inner<N: NodeT, L: crate::log::LogQuery>(
	column: &crate::column::Column,
	log: &L,
	record_id: u64,
) -> Result<(btree::BTree<N>, BTreeTableId, u64, u64)> {
	let (root, root_index, btree_index, table_id) = column.with_btree(|btree| {
		let btree_index = log.btree_index(btree.id).unwrap_or_else(|| btree.initial_index.clone());
		let (root_index, root) = if btree_index.root == HEADER_POSITION {
			(None, N::new())
		} else {
			let root = btree.get_index::<N, _>(btree_index.root, log)?;
			(Some(btree_index.root), N::from_encoded(root))
		};
		Ok((
			root,
			root_index,
			btree_index,
			btree.id,
		))
	})?;
	Ok((
		btree::BTree::<N>::new(root_index, root, btree_index.depth, record_id),
		table_id,
		btree_index.last_removed,
		btree_index.filled,
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
			match column.with_btree(|btree| Ok(btree.variant))? {
				ConfigVariants::Order2_3_64 => self.write_plan_inner::<Node<Order2_3_64>>(
					column,
					writer,
					ops,
				),
			}
		}

		fn write_plan_inner<N: NodeT>(
			&self,
			column: &Column,
			writer: &mut LogWriter,
			ops: &mut u64,
		) -> Result<()> {
			let origin = crate::column::ValueTableOrigin::BTree(crate::btree::BTreeTableId::new(self.col));
			// TODO consider runing on btree with ValueTable as params.
			let record_id = writer.record_id();
			// This is not racy as we have a single thread writing plan, so a single btree instance.
			let (mut tree, table_id, last_removed, filled) = new_btree_inner::<N, _>(column, writer, record_id)?;
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
				last_removed,
				filled,
				root: tree.root_index.unwrap_or(0),
				depth: tree.depth,
			};
			let old_btree_index = btree_index.clone();

			tree.write_plan(column, writer, table_id, record_id, &mut btree_index)?;

			if old_btree_index != btree_index {
				let mut entry = Entry::<N::Config>::empty();
				entry.write_header(&btree_index);
				writer.insert_btree(table_id, HEADER_POSITION, entry.encoded.as_ref().to_vec(), record_id, &btree_index);
				writer.insert_btree_index(table_id, record_id, btree_index);
			}
			Ok(())
		}
	}
}
