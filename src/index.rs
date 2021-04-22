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

const CHUNK_LEN: usize = 512; // bytes
const CHUNK_ENTRIES: usize = 64;
const HEADER_SIZE: usize = 512;
const META_SIZE: usize = 16 * 1024; // Contains header and column stats
// TODO restore const META_SIZE: usize = 32 * 1024; // Contains header and column stats
const KEY_LEN: usize = 32;

const EMPTY_CHUNK: Chunk = [0u8; CHUNK_LEN];

pub type Key = [u8; KEY_LEN];
pub type Chunk = [u8; CHUNK_LEN];

pub struct Entry(u64);

impl Entry {
	// TODO remove pub
	#[inline]
	pub fn new(address: Address, key_material: u64, index_bits: u8) -> Entry {
		Entry((key_material << Self::address_bits(index_bits)) | address.as_u64())
	}

	#[inline]
	pub fn address_bits(index_bits: u8) -> u8 {
		index_bits + 10 // with n index bits there are n * 64 possible entries and 16 size tiers
	}

	#[inline]
	pub fn address(&self, index_bits: u8) -> Address {
		Address::from_u64(self.0 & ((1u64 << Self::address_bits(index_bits)) - 1))
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

	fn open_mmap(path: &std::path::Path, id: &TableId) -> Result<Option<memmap2::MmapMut>> {
		let file = match std::fs::OpenOptions::new().read(true).write(true).open(path) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
			Ok(file) => file,
		};

		//TODO: check for potential overflows on 32-bit platforms
		file.set_len(file_size(id.index_bits()))?;
		Ok(Some(unsafe { memmap2::MmapMut::map_mut(&file)? }))
	}

	pub fn open_existing(path: &std::path::Path, id: TableId) -> Result<Option<IndexTable>> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		Ok(Self::open_mmap(path.as_path(), &id)?.map(|map| {
			log::debug!(target: "parity-db", "Opened existing index {}", id);
			IndexTable {
				id,
				path,
				map: RwLock::new(Some(map)),
			}
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
		if let Some(map) = &*self.map.read() {
			ColumnStats::from_slice(&map[HEADER_SIZE .. HEADER_SIZE + stats::TOTAL_SIZE])
		} else {
			ColumnStats::empty()
		}
	}

	pub fn write_stats(&self, stats: &ColumnStats) {
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
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			if !entry.is_empty() && entry.key_material(self.id.index_bits()) == partial_key {
				return (entry, i);
			}
		}
		return (Entry::empty(), 0)
	}

	pub fn get<Q: LogQuery>(&self, key: &Key, sub_index: usize, log: &Q) -> (Entry, usize) {
		log::trace!(target: "parity-db", "{}: Querying {}", self.id, hex(&key));
		let key = u64::from_be_bytes((key[0..8]).try_into().unwrap());
		let chunk_index = key >> (64 - self.id.index_bits());

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

	pub fn entries<Q: LogQuery>(&self, chunk_index: u64, log: &Q) -> [Entry; 64] {
		let mut chunk = [0; CHUNK_LEN];
		if let Some(entry) = log.with_index(self.id, chunk_index, |chunk|
			unsafe { std::mem::transmute(*chunk) }) {
			return entry;
		}
		if let Some(map) = &*self.map.read() {
			let source = Self::chunk_at(chunk_index, map);
			chunk.copy_from_slice(source);
			return unsafe { std::mem::transmute(chunk) };
		}
		return unsafe { std::mem::transmute(EMPTY_CHUNK) };
	}

	pub(crate) fn for_all(&self, mut apply: impl FnMut(&Key, Entry) -> Option<Entry>, with_progress: Option<u64>) {
		let mut key = Key::default();
		let mut chunk_index = 0u64;
		let index_bits = self.id.index_bits();
		let total_chunks = total_chunks(index_bits);
		if with_progress.is_some() {
			// TODO replace those warn with trace
			log::warn!(target: "parity-db", "Starting full index iteration at {:?}", std::time::Instant::now());
			log::warn!(target: "parity-db", "for {} chunks", total_chunks);
		}
		let mut chunk = [0; CHUNK_LEN];
		while chunk_index < total_chunks {
			if let Some(step) = with_progress.as_ref() {
				if chunk_index % step == 0 {
					log::warn!(target: "parity-db", "Chunk iteration at {}", chunk_index);
				}
			}
			let mut entries: [Entry; CHUNK_ENTRIES] = {
				// TODO factor with 'entries' + fix for be arch
				if let Some(map) = &*self.map.read() {
					let source = Self::chunk_at(chunk_index, map);
					chunk.copy_from_slice(source);
					unsafe { std::mem::transmute(chunk) }
				} else {
					break;
				}
			};
			let mut changed = false;
			for i in 0 .. CHUNK_ENTRIES {
				let entry = Entry(entries[i].0);
				if !entry.is_empty() {
					let key_index_bits = chunk_index << (64 - index_bits);
					let key_material = entry.key_material(index_bits) << 10;
					key[0..8].copy_from_slice(&(key_index_bits | key_material).to_be_bytes()[..]);
					if let Some(entry) = apply(&key, entry) {
						changed = true;
						entries[i] = entry;
					}
				}
			}
			if changed {
				// TODO could write handle for all precessing.
				if let Some(map) = &mut *self.map.write() {
					// TODO factor with chunk_at (and read logger?)
					let offset = META_SIZE + chunk_index as usize * CHUNK_LEN;
					let dest = &mut map[offset .. offset + CHUNK_LEN];
					// TODO fix for be arch
					let source: [u8; CHUNK_LEN] = unsafe { std::mem::transmute(chunk) };
					dest.copy_from_slice(&source[..]);
				} else {
					break;
				}
			}
	
			chunk_index += 1;
		}
		if with_progress.is_some() {
			log::warn!(target: "parity-db", "Ended full index iteration at {:?}", std::time::Instant::now());
		}
	}

	fn plan_insert_chunk(
		&self,
		key: u64,
		address: Address,
		source: &[u8],
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let chunk_index = key >> (64 - self.id.index_bits());
		if address.as_u64() >= (1 << Entry::address_bits(self.id.index_bits())) {
			// Address overflow
			log::warn!(target: "parity-db", "{}: Address space overflow at {}: {}", self.id, chunk_index, address);
			return Ok(PlanOutcome::NeedReindex);
		}
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let partial_key = Entry::extract_key(key, self.id.index_bits());
		let new_entry = Entry::new(address, partial_key, self.id.index_bits());
		if let Some(i) = sub_index {
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			assert!(entry.key_material(self.id.index_bits()) == new_entry.key_material(self.id.index_bits()));
			&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
			log::trace!(target: "parity-db", "{}: Replaced at {}.{}: {}", self.id, chunk_index, i, new_entry.address(self.id.index_bits()));
			log.insert_index(self.id, chunk_index, &chunk);
			return Ok(PlanOutcome::Written);
		}
		for i in 0 .. CHUNK_ENTRIES {
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			if entry.is_empty() {
				&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
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
		let chunk_index = key >> (64 - self.id.index_bits());

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
		let chunk_index = key >> (64 - self.id.index_bits());
		let partial_key = Entry::extract_key(key, self.id.index_bits());

		let i = sub_index;
		let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
		if !entry.is_empty() && entry.key_material(self.id.index_bits()) == partial_key {
			let new_entry = Entry::empty();
			&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
			log.insert_index(self.id, chunk_index, &chunk);
			log::trace!(target: "parity-db", "{}: Removed at {}.{}", self.id, chunk_index, i);
			return Ok(PlanOutcome::Written);
		}
		Ok(PlanOutcome::Skipped)
	}

	pub fn write_remove_plan(&self, key: &Key, sub_index: usize, log: &mut LogWriter) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Removing {}", self.id, hex(&key));
		let key = u64::from_be_bytes((key[0..8]).try_into().unwrap());
		let chunk_index = key >> (64 - self.id.index_bits());

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
			if let Some(mmap) = Self::open_mmap(self.path.as_path(), &self.id)? {
				log::debug!(target: "parity-db", "Created new index {}", self.id);
				*wmap = Some(mmap);
				map = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wmap);
			} else {
				return Err(std::io::ErrorKind::NotFound.into());
			}
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

	// Copy full index file.
	pub(crate) fn backup_index(&self, path: &std::path::Path) -> Result<()> {
		let mut mmap = self.map.write().take();
		mmap.as_mut().map(|mmap| mmap.flush());
		std::mem::drop(mmap);
		let mut path_bu: std::path::PathBuf = path.into();
		let mut bu_name = self.id.file_name();
		bu_name.push_str("_old");
		path_bu.push(bu_name);
		std::fs::copy(&self.path, path_bu)?;

		*self.map.write() = Self::open_mmap(self.path.as_path(), &self.id)?;
		Ok(())
	}

	pub(crate) fn remove_backup_index(&self, path: &std::path::Path) -> Result<()> {
		let mut path_bu: std::path::PathBuf = path.into();
		let mut bu_name = self.id.file_name();
		bu_name.push_str("_old");
		path_bu.push(bu_name);
		std::fs::remove_file(path_bu)?;
		Ok(())
	}
}

#[test]
fn test_key_build() {
	use rand::Rng;
	for index_bits in 16..32 {
		let key_init: u64 = rand::thread_rng().gen();
		let address: u64 = rand::thread_rng().gen();
		// fit address to right range
		let address = Entry(address).address(index_bits);
		// remove the key data from value key that is
		// checked against value.
		let in_value = key_init & 0xffff;
		let partial_key = Entry::extract_key(key_init, index_bits);
		let entry = Entry::new(address, partial_key, index_bits);
		let chunk_index = key_init >> (64 - index_bits);
		let key_index_bits = chunk_index << (64 - index_bits);
		let key_material = entry.key_material(index_bits) << 10;
		let key = key_index_bits | key_material | in_value;
		/*println!("address {:x}", address.0);
		println!("partial_key {:x}", partial_key);
		println!("entry {:x}", entry.0);
		println!("chunk_index {:x}", chunk_index);
		println!("key_index_bits {:x}", key_index_bits);
		println!("in_value {:x}", in_value);
		println!("key_material {:x}", key_material);
		println!("key_init {:x}", key_init);
		println!("key {:x}", key);*/
		assert!(key_init == key);
	}
}
