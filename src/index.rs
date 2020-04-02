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
use crate::{
	error::Result,
	column::ColId,
	log::{Log, LogReader, LogWriter},
	table::Address,
	display::hex,
};

const CHUNK_LEN: usize = 512; // bytes
const CHUNK_ENTRIES: usize = 64;
const META_SIZE: usize = 8 * 1024; // typical SSD page size
const KEY_LEN: usize = 32;

const EMPTY_CHUNK: Chunk = [0u8; CHUNK_LEN];

const PARTIAL_KEY_BITS: u8 = 28;
const PARTIAL_KEY_MASK: u32 = (1u32 << PARTIAL_KEY_BITS) - 1;

const ADDRESS_BITS: u8 = 36;
const ADDRESS_MASK: u64 = (1u64 << ADDRESS_BITS) - 1;

pub type Key = [u8; KEY_LEN];
pub type Chunk = [u8; CHUNK_LEN];

pub struct Entry(u64);

impl Entry {
	fn new(address: Address, key_bits: u32) -> Entry {
		Entry(((key_bits as u64) << ADDRESS_BITS) | address.as_u64())
	}

	pub fn address(&self) -> Address {
		Address::from_u64(self.0 & ADDRESS_MASK) // lower 40 bits
	}

	#[inline]
	fn key_bits(&self) -> u32 {
		((self.0 >> ADDRESS_BITS) as u32) & PARTIAL_KEY_MASK // next 20 bits
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

pub enum PlanOutcome {
	Written,
	NeedRebalance,
	Skipped,
}

pub struct IndexTable {
	pub id: TableId,
	map: Option<memmap::MmapMut>,
	path: std::path::PathBuf,
	entries: std::sync::atomic::AtomicU64,
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
		let map = unsafe { memmap::MmapMut::map_mut(&file)? };
		log::debug!(target: "parity-db", "Opened existing index {}", id);
		Ok(Some(IndexTable {
			id,
			path,
			map: Some(map),
			entries: std::sync::atomic::AtomicU64::new(0),
		}))
	}

	pub fn create_new(path: &std::path::Path, id: TableId) -> IndexTable {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());
		IndexTable {
			id,
			path,
			map: None,
			entries: std::sync::atomic::AtomicU64::new(0),
		}
	}

	fn open_map(&mut self) -> Result<()> {
		if self.map.is_some() {
			return Ok(());
		}
		let file = std::fs::OpenOptions::new().write(true).read(true).create_new(true).open(self.path.as_path())?;
		log::debug!(target: "parity-db", "Created new index {}", self.id);
		//TODO: check for potential overflows on 32-bit platforms
		file.set_len(file_size(self.id.index_bits()))?;
		self.map = Some(unsafe { memmap::MmapMut::map_mut(&file)? });
		Ok(())
	}

	fn chunk_at(index: u64, map: &memmap::MmapMut) -> &[u8] {
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		&map[offset .. offset + CHUNK_LEN]
	}

	fn find_entry(&self, key: u64, chunk: &[u8]) -> Entry {
		let partial_key = ((key >> (64 - PARTIAL_KEY_BITS - self.id.index_bits())) as u32) & PARTIAL_KEY_MASK;
		for i in 0 .. CHUNK_ENTRIES {
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			if !entry.is_empty() && entry.key_bits() == partial_key {
				log::trace!(target: "parity-db", "{}: Found entry at {}, bits={}", self.id, i, entry.key_bits());
				return entry;
			}
		}
		return Entry::empty()
	}

	pub fn get(&self, key: &Key, log: &Log) -> Entry {
		log::debug!(target: "parity-db", "{}: Querying {}", self.id, hex(&key));
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		let chunk_index = key >> (64 - self.id.index_bits());

		if let Some(chunk) = log.index_overlay_at(self.id, chunk_index) {
			log::trace!(target: "parity-db", "{}: Querying overlay at {}", self.id, chunk_index);
			return self.find_entry(key, chunk);
		}

		if let Some(map) = &self.map {
			log::trace!(target: "parity-db", "{}: Querying chunk at {}", self.id, chunk_index);
			let chunk = Self::chunk_at(chunk_index, map);
			return self.find_entry(key, chunk);

		}
		return Entry::empty()
	}

	pub fn get_planned(&self, key: &Key, log: &LogWriter) -> Entry {
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		let chunk_index = key >> (64 - self.id.index_bits());

		if let Some(chunk) = log.index_overlay_at(self.id, chunk_index) {
			log::trace!(target: "parity-db", "{}: Found overlay at {}", self.id, chunk_index);
			return self.find_entry(key, chunk);
		}

		if let Some(map) = &self.map {
			let chunk = Self::chunk_at(chunk_index, map);
			log::trace!(target: "parity-db", "{}: Checking at {}", self.id, chunk_index);
			return self.find_entry(key, chunk);

		}
		return Entry::empty()
	}

	pub fn planned_entries(&self, chunk_index: u64, log: &LogWriter) -> [Entry; 64] {
		let mut chunk = [0; CHUNK_LEN];
		if let Some(source) = log.index_overlay_at(self.id, chunk_index) {
			chunk.copy_from_slice(source);
			return unsafe { std::mem::transmute(chunk) };
		}

		if let Some(map) = &self.map {
			let source = Self::chunk_at(chunk_index, map);
			chunk.copy_from_slice(source);
			return unsafe { std::mem::transmute(chunk) };
		}
		return unsafe { std::mem::transmute(EMPTY_CHUNK) };
	}

	fn plan_insert_chunk(
		&self,
		key: u64,
		address: Address,
		source: &[u8],
		log: &mut LogWriter,
		overwrite: bool
	) -> Result<PlanOutcome> {
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let chunk_index = key >> (64 - self.id.index_bits());
		let partial_key = ((key >> (64 - PARTIAL_KEY_BITS - self.id.index_bits())) as u32) & PARTIAL_KEY_MASK;
		let mut first_empty = None;

		for i in 0 .. CHUNK_ENTRIES {
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			if entry.is_empty() {
				if first_empty.is_none() {
					first_empty = Some(i);
				}
			} else {
				if entry.key_bits() == partial_key {
					// Replace here
					if overwrite {
						let new_entry = Entry::new(address, partial_key);
						&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
						log.insert_index(self.id, chunk_index, &chunk);
						log::trace!(target: "parity-db", "{}: Replaced at {}.{}", self.id, chunk_index, i);
						return Ok(PlanOutcome::Written);
					} else {
						return Ok(PlanOutcome::Skipped);
					}
				}
			}
		}
		if let Some(i) = first_empty {
			let new_entry = Entry::new(address, partial_key);
			&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
			log::trace!(target: "parity-db", "{}: Inserted at {}.{}: {}", self.id, chunk_index, i, new_entry.address());
			log.insert_index(self.id, chunk_index, &chunk);
			self.entries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			return Ok(PlanOutcome::Written);
		} else {
			log::trace!(target: "parity-db", "{}: Full at {}", self.id, chunk_index);
			return Ok(PlanOutcome::NeedRebalance);
		}
	}

	pub fn write_insert_plan(&self, key: &Key, address: Address, log: &mut LogWriter, overwrite: bool) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Inserting {} -> {}", self.id, hex(&key), address);
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		let chunk_index = key >> (64 - self.id.index_bits());

		if let Some(chunk) = log.index_overlay_at(self.id, chunk_index).cloned() {
			return self.plan_insert_chunk(key, address, &chunk, log, overwrite);
		}

		if let Some(map) = &self.map {
			let chunk = Self::chunk_at(chunk_index, map);
			return self.plan_insert_chunk(key, address, chunk, log, overwrite);
		}

		let chunk = &EMPTY_CHUNK;
		self.plan_insert_chunk(key, address, chunk, log, overwrite)
	}

	fn plan_remove_chunk(&self, key: u64, source: &[u8], log: &mut LogWriter) -> Result<PlanOutcome> {
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let chunk_index = key >> (64 - self.id.index_bits());
		let partial_key = ((key >> (64 - PARTIAL_KEY_BITS - self.id.index_bits())) as u32) & PARTIAL_KEY_MASK;

		for i in 0 .. CHUNK_ENTRIES {
			let entry = Entry::from_u64(u64::from_le_bytes(chunk[i * 8 .. i * 8 + 8].try_into().unwrap()));
			if !entry.is_empty() && entry.key_bits() == partial_key {
				let new_entry = Entry::empty();
				&mut chunk[i * 8 .. i * 8 + 8].copy_from_slice(&new_entry.as_u64().to_le_bytes());
				log.insert_index(self.id, chunk_index, &chunk);
				log::trace!(target: "parity-db", "{}: Removed at {}.{}", self.id, chunk_index, i);
				return Ok(PlanOutcome::Written);
			}
		}
		Ok(PlanOutcome::Skipped)
	}

	pub fn write_remove_plan(&self, key: &Key, log: &mut LogWriter) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Removing {}", self.id, hex(&key));
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		let chunk_index = key >> (64 - self.id.index_bits());

		if let Some(chunk) = log.index_overlay_at(self.id, chunk_index).cloned() {
			return self.plan_remove_chunk(key, &chunk, log);
		}

		if let Some(map) = &self.map {
			let chunk = Self::chunk_at(chunk_index, map);
			return self.plan_remove_chunk(key, chunk, log);
		}

		Ok(PlanOutcome::Skipped)
	}

	pub fn enact_plan(&mut self, index: u64, log: &mut LogReader) -> Result<()> {
		self.open_map()?;
		let map = self.map.as_mut().unwrap();
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		let mut chunk = &mut map[offset .. offset + CHUNK_LEN];
		log.read(&mut chunk)?;
		log::trace!(target: "parity-db", "{}: Enacted chunk {}", self.id, index);
		Ok(())
	}

	pub fn entries(&self) -> u64 {
		self.entries.load(std::sync::atomic::Ordering::Relaxed)
	}

	pub fn drop_file(self) -> Result<()> {
		std::mem::drop(self.map);
		std::fs::remove_file(self.path.as_path())?;
		log::debug!(target: "parity-db", "{}: Dropped table", self.id);
		Ok(())
	}
}
