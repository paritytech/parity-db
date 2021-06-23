// Copyright 2015-2020 Parity Technologies (UK) Ltd.
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

use std::convert::TryInto;
use parking_lot::{RwLockUpgradableReadGuard, RwLock};
use crate::{
	error::{Error, Result},
	column::ColId,
	log::{LogReader, LogWriter, LogQuery},
	display::hex,
	stats::{self, ColumnStats},
};

const CHUNK_LEN: usize = CHUNK_ENTRIES  * ENTRY_LEN as usize / 8; // 512 bytes
const CHUNK_ENTRIES: usize = 1 << CHUNK_ENTRIES_BITS;
const CHUNK_ENTRIES_BITS: u8 = 6;
const HEADER_SIZE: usize = 512;
const META_SIZE: usize = crate::table::SIZE_TIERS * 1024; // Contains header and column stats
const KEY_LEN: usize = 32;
const ENTRY_LEN: u8 = 64;

const EMPTY_CHUNK: Chunk = [0u8; CHUNK_LEN];

pub type Key = [u8; KEY_LEN];
pub type Chunk = [u8; CHUNK_LEN];

#[derive(PartialEq, Eq)]
pub struct Entry(u64);

impl Entry {
	#[inline]
	fn new(address: Address, key_material: u64, index_bits: u8) -> Entry {
		Entry((key_material << Self::address_bits(index_bits)) | address.as_u64())
	}

	#[inline]
	pub fn address_bits(index_bits: u8) -> u8 {
		// with n index bits there are n * 64 possible entries and 16 size tiers
		index_bits + CHUNK_ENTRIES_BITS + crate::table::SIZE_TIERS_BITS
	}

	#[inline]
	pub fn last_address(index_bits: u8) -> u64 {
		(1u64 << Self::address_bits(index_bits)) - 1
	}

	#[inline]
	pub fn address(&self, index_bits: u8) -> Address {
		Address::from_u64(self.0 & Self::last_address(index_bits))
	}

	#[inline]
	pub fn key_material(&self, index_bits: u8) -> u64 {
		self.0 >> Self::address_bits(index_bits)
	}

	#[inline]
	fn extract_key(key: u64, index_bits: u8) -> u64 {
		(key << index_bits) >> Self::address_bits(index_bits)
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.0 == 0
	}

	fn as_u64(&self) -> u64 {
		self.0
	}

	fn empty() -> Self {
		Entry(0)
	}

	fn from_u64(e: u64) -> Self {
		Entry(e)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Address(u64);

impl Address {
	pub fn new(offset: u64, size_tier: u8) -> Address {
		Address((offset << 4) | size_tier as u64)
	}

	pub fn from_u64(a: u64) -> Address {
		Address(a)
	}

	pub fn offset(&self) -> u64 {
		self.0 >> 4
	}

	pub fn size_tier(&self) -> u8 {
		(self.0 & 0x0f) as u8
	}

	pub fn as_u64(&self) -> u64 {
		self.0
	}
}

impl std::fmt::Display for Address {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "addr {:02}:{}", self.size_tier(), self.offset())
	}
}

pub enum PlanOutcome {
	Written,
	NeedReindex,
	Skipped,
}

pub struct IndexTable {
	pub id: TableId,
	map: RwLock<Option<memmap2::MmapMut>>,
	path: std::path::PathBuf,
}

fn total_entries(index_bits: u8) -> u64 {
	total_chunks(index_bits) * CHUNK_ENTRIES as u64
}

fn total_chunks(index_bits: u8) -> u64 {
	1u64 << index_bits
}

fn file_size(index_bits: u8) -> u64 {
	total_entries(index_bits) * 8 + META_SIZE as u64
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TableId(u16);

impl TableId {
	pub fn new(col: ColId, index_bits: u8) -> TableId {
		TableId(((col as u16) << 8)| (index_bits as u16))
	}

	pub fn from_u16(id: u16) -> TableId {
		TableId(id)
	}

	pub fn col(&self) -> ColId {
		(self.0 >> 8) as ColId
	}

	pub fn index_bits(&self) -> u8 {
		(self.0 & 0xff) as u8
	}

	pub fn file_name(&self) -> String {
		format!("index_{:02}_{}", self.col(), self.index_bits())
	}

	pub fn as_u16(&self) -> u16 {
		self.0
	}

	pub fn total_chunks(&self) -> u64 {
		total_chunks(self.index_bits())
	}

	pub fn total_entries(&self) -> u64 {
		total_entries(self.index_bits())
	}
}

impl std::fmt::Display for TableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "index {:02}_{}", self.col(), self.index_bits())
	}
}

impl IndexTable {
	pub fn open_existing(path: &std::path::Path, id: TableId) -> Result<Option<IndexTable>> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = match std::fs::OpenOptions::new().read(true).write(true).open(path.as_path()) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
			Ok(file) => file,
		};

		file.set_len(file_size(id.index_bits()))?;
		let map = unsafe { memmap2::MmapMut::map_mut(&file)? };
		log::debug!(target: "parity-db", "Opened existing index {}", id);
		Ok(Some(IndexTable {
			id,
			path,
			map: RwLock::new(Some(map)),
		}))
	}

	pub fn create_new(path: &std::path::Path, id: TableId) -> IndexTable {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());
		IndexTable {
			id,
			path,
			map: RwLock::new(None),
		}
	}

	pub fn load_stats(&self) -> ColumnStats {
		debug_assert!(META_SIZE >= HEADER_SIZE + stats::TOTAL_SIZE);
		if let Some(map) = &*self.map.read() {
			ColumnStats::from_slice(&map[HEADER_SIZE .. HEADER_SIZE + stats::TOTAL_SIZE])
		} else {
			ColumnStats::empty()
		}
	}

	pub fn write_stats(&self, stats: &ColumnStats) {
		debug_assert!(META_SIZE >= HEADER_SIZE + stats::TOTAL_SIZE);
		if let Some(map) = &mut *self.map.write() {
			let mut slice = &mut map[HEADER_SIZE .. HEADER_SIZE + stats::TOTAL_SIZE];
			stats.to_slice(&mut slice);
		}
	}

	fn chunk_at(index: u64, map: &memmap2::MmapMut) -> &[u8] {
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		&map[offset .. offset + CHUNK_LEN]
	}

	fn find_entry(&self, key: u64, sub_index: usize, chunk: &[u8]) -> (Entry, usize) {
		let partial_key = Entry::extract_key(key, self.id.index_bits());
		for i in sub_index .. CHUNK_ENTRIES {
			let entry = Self::read_entry(&chunk, i);
			if !entry.is_empty() && entry.key_material(self.id.index_bits()) == partial_key {
				return (entry, i);
			}
		}
		return (Entry::empty(), 0)
	}

	pub fn get(&self, key: &Key, sub_index: usize, log: &impl LogQuery) -> (Entry, usize) {
		log::trace!(target: "parity-db", "{}: Querying {}", self.id, hex(&key));
		let key = u64::from_be_bytes((key[0..8]).try_into().unwrap());
		let chunk_index = self.chunk_index(key);

		if let Some(entry) = log.with_index(self.id, chunk_index, |chunk| {
				log::trace!(target: "parity-db", "{}: Querying overlay at {}", self.id, chunk_index);
				self.find_entry(key, sub_index, chunk)
			}) {
			return entry;
		}

		if let Some(map) = &*self.map.read() {
			log::trace!(target: "parity-db", "{}: Querying chunk at {}", self.id, chunk_index);
			let chunk = Self::chunk_at(chunk_index, map);
			return self.find_entry(key, sub_index, chunk);

		}
		return (Entry::empty(), 0)
	}

	pub fn entries(&self, chunk_index: u64, log: &impl LogQuery) -> [Entry; CHUNK_ENTRIES] {
		let mut chunk = [0; CHUNK_LEN];
		if let Some(entry) = log.with_index(self.id, chunk_index, |chunk|
			Self::transmute_chunk(*chunk)) {
			return entry;
		}
		if let Some(map) = &*self.map.read() {
			let source = Self::chunk_at(chunk_index, map);
			chunk.copy_from_slice(source);
			return Self::transmute_chunk(chunk);
		}
		return Self::transmute_chunk(EMPTY_CHUNK);
	}

	#[inline(always)]
	fn transmute_chunk(chunk: [u8; CHUNK_LEN]) -> [Entry; CHUNK_ENTRIES] {
		let mut result: [Entry; CHUNK_ENTRIES] = unsafe { std::mem::transmute(chunk) };
		if !cfg!(target_endian = "little") {
			for i in 0 .. CHUNK_ENTRIES {
				result[i] = Entry::from_u64(u64::from_le(result[i].0));
			}
		}
		result
	}

	#[inline(always)]
	fn write_entry(entry: &Entry, at: usize, chunk: &mut [u8; CHUNK_LEN]) {
		chunk[at * 8 .. at * 8 + 8].copy_from_slice(&entry.as_u64().to_le_bytes());
	}

	#[inline(always)]
	fn read_entry(chunk: &[u8], at: usize) -> Entry {
		Entry::from_u64(u64::from_le_bytes(chunk[at * 8 .. at * 8 + 8].try_into().unwrap()))
	}

	#[inline(always)]
	fn chunk_index(&self, key: u64) -> u64 {
		key >> (ENTRY_LEN - self.id.index_bits())
	}

	fn plan_insert_chunk(
		&self,
		key: u64,
		address: Address,
		source: &[u8],
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let chunk_index = self.chunk_index(key);
		if address.as_u64() > Entry::last_address(self.id.index_bits()) {
			// Address overflow
			log::warn!(target: "parity-db", "{}: Address space overflow at {}: {}", self.id, chunk_index, address);
			return Ok(PlanOutcome::NeedReindex);
		}
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let partial_key = Entry::extract_key(key, self.id.index_bits());
		let new_entry = Entry::new(address, partial_key, self.id.index_bits());
		if let Some(i) = sub_index {
			let entry = Self::read_entry(&chunk, i);
			assert!(entry.key_material(self.id.index_bits()) == new_entry.key_material(self.id.index_bits()));
			Self::write_entry(&new_entry, i, &mut chunk);
			log::trace!(target: "parity-db", "{}: Replaced at {}.{}: {}", self.id, chunk_index, i, new_entry.address(self.id.index_bits()));
			log.insert_index(self.id, chunk_index, &chunk);
			return Ok(PlanOutcome::Written);
		}
		for i in 0 .. CHUNK_ENTRIES {
			let entry = Self::read_entry(&chunk, i);
			if entry.is_empty() {
				Self::write_entry(&new_entry, i, &mut chunk);
				log::trace!(target: "parity-db", "{}: Inserted at {}.{}: {}", self.id, chunk_index, i, new_entry.address(self.id.index_bits()));
				log.insert_index(self.id, chunk_index, &chunk);
				return Ok(PlanOutcome::Written);
			}
		}
		log::trace!(target: "parity-db", "{}: Full at {}", self.id, chunk_index);
		return Ok(PlanOutcome::NeedReindex);
	}

	pub fn write_insert_plan(&self, key: &Key, address: Address, sub_index: Option<usize>, log: &mut LogWriter) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Inserting {} -> {}", self.id, hex(&key), address);
		let key = u64::from_be_bytes((key[0..8]).try_into().unwrap());
		let chunk_index = self.chunk_index(key);

		if let Some(chunk) = log.with_index(self.id, chunk_index, |chunk| chunk.clone()) {
			return self.plan_insert_chunk(key, address, &chunk, sub_index, log)
		}

		if let Some(map) = &*self.map.read() {
			let chunk = Self::chunk_at(chunk_index, map);
			return self.plan_insert_chunk(key, address, chunk, sub_index, log);
		}

		let chunk = &EMPTY_CHUNK;
		self.plan_insert_chunk(key, address, chunk, sub_index, log)
	}

	fn plan_remove_chunk(&self, key: u64, source: &[u8], sub_index: usize, log: &mut LogWriter) -> Result<PlanOutcome> {
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let chunk_index = self.chunk_index(key);
		let partial_key = Entry::extract_key(key, self.id.index_bits());

		let i = sub_index;
		let entry = Self::read_entry(&chunk, i);
		if !entry.is_empty() && entry.key_material(self.id.index_bits()) == partial_key {
			let new_entry = Entry::empty();
			Self::write_entry(&new_entry, i, &mut chunk);
			log.insert_index(self.id, chunk_index, &chunk);
			log::trace!(target: "parity-db", "{}: Removed at {}.{}", self.id, chunk_index, i);
			return Ok(PlanOutcome::Written);
		}
		Ok(PlanOutcome::Skipped)
	}

	pub fn write_remove_plan(&self, key: &Key, sub_index: usize, log: &mut LogWriter) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Removing {}", self.id, hex(&key));
		let key = u64::from_be_bytes((key[0..8]).try_into().unwrap());
		let chunk_index = self.chunk_index(key);

		if let Some(chunk) = log.with_index(self.id, chunk_index, |chunk| chunk.clone()) {
			return self.plan_remove_chunk(key, &chunk, sub_index, log);
		}

		if let Some(map) = &*self.map.read() {
			let chunk = Self::chunk_at(chunk_index, map);
			return self.plan_remove_chunk(key, chunk, sub_index, log);
		}

		Ok(PlanOutcome::Skipped)
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut map = self.map.upgradable_read();
		if map.is_none() {
			let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
			let file = std::fs::OpenOptions::new().write(true).read(true).create_new(true).open(self.path.as_path())?;
			log::debug!(target: "parity-db", "Created new index {}", self.id);
			//TODO: check for potential overflows on 32-bit platforms
			file.set_len(file_size(self.id.index_bits()))?;
			*wmap = Some(unsafe { memmap2::MmapMut::map_mut(&file)? });
			map = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wmap);
		}

		let map = map.as_ref().unwrap();
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		// Nasty mutable pointer cast. We do ensure that all chunks that are being written are accessed
		// through the overlay in other threads.
		let ptr: *mut u8 = map.as_ptr() as *mut u8;
		let mut chunk: &mut[u8] = unsafe {
			let ptr = ptr.offset(offset as isize);
			std::slice::from_raw_parts_mut(ptr, CHUNK_LEN)
		};
		log.read(&mut chunk)?;
		log::trace!(target: "parity-db", "{}: Enacted chunk {}", self.id, index);
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index >= self.id.total_entries() {
			return Err(Error::Corruption("Bad index".into()));
		}
		let mut chunk = [0; CHUNK_LEN];
		log.read(&mut chunk)?;
		log::trace!(target: "parity-db", "{}: Validated chunk {}", self.id, index);
		Ok(())
	}

	pub fn drop_file(self) -> Result<()> {
		std::mem::drop(self.map);
		std::fs::remove_file(self.path.as_path())?;
		log::debug!(target: "parity-db", "{}: Dropped table", self.id);
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Some(map) = &*self.map.read() {
			map.flush()?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_entries() {
		let mut chunk = IndexTable::transmute_chunk(EMPTY_CHUNK);
		let mut chunk2 = EMPTY_CHUNK;
		for i in 0 .. CHUNK_ENTRIES {
			use std::collections::hash_map::DefaultHasher;
			use std::hash::{Hash, Hasher};
			let mut hasher = DefaultHasher::new();
			i.hash(&mut hasher);
			let hash = hasher.finish();
			let entry = Entry::from_u64(hash as u64);
			IndexTable::write_entry(&entry, i, &mut chunk2);
			chunk[i] = entry;
		}

		assert!(IndexTable::transmute_chunk(chunk2) == chunk);
	}
}
