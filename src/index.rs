// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::ColId,
	display::hex,
	error::{try_io, Error, Result},
	log::{LogQuery, LogReader, LogWriter},
	parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard},
	stats::{self, ColumnStats},
	table::{key::TableKey, SIZE_TIERS_BITS},
	Key,
};
#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::convert::TryInto;

// Index chunk consists of 8 64-bit entries.
const CHUNK_LEN: usize = CHUNK_ENTRIES * ENTRY_BYTES; // 512 bytes
const CHUNK_ENTRIES: usize = 1 << CHUNK_ENTRIES_BITS;
const CHUNK_ENTRIES_BITS: u8 = 6;
const HEADER_SIZE: usize = 512;
const META_SIZE: usize = 16 * 1024; // Contains header and column stats
const ENTRY_BITS: u8 = 64;
pub const ENTRY_BYTES: usize = ENTRY_BITS as usize / 8;

const EMPTY_CHUNK: Chunk = [0u8; CHUNK_LEN];

pub type Chunk = [u8; CHUNK_LEN];

#[allow(clippy::assertions_on_constants)]
const _: () = assert!(META_SIZE >= HEADER_SIZE + stats::TOTAL_SIZE);

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Entry(u64);

impl Entry {
	#[inline]
	fn new(address: Address, partial_key: u64, index_bits: u8) -> Entry {
		Entry((partial_key << Self::address_bits(index_bits)) | address.as_u64())
	}

	#[inline]
	pub fn address_bits(index_bits: u8) -> u8 {
		// with n index bits there are n * 64 possible entries and 256 size tiers
		index_bits + CHUNK_ENTRIES_BITS + SIZE_TIERS_BITS
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
	pub fn partial_key(&self, index_bits: u8) -> u64 {
		self.0 >> Self::address_bits(index_bits)
	}

	#[inline]
	fn extract_key(key_prefix: u64, index_bits: u8) -> u64 {
		(key_prefix << index_bits) >> Self::address_bits(index_bits)
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.0 == 0
	}

	pub fn as_u64(&self) -> u64 {
		self.0
	}

	fn empty() -> Self {
		Entry(0)
	}

	fn from_u64(e: u64) -> Self {
		Entry(e)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Address(u64);

impl Address {
	pub const fn new(offset: u64, size_tier: u8) -> Address {
		Address((offset << SIZE_TIERS_BITS) | size_tier as u64)
	}

	pub const fn from_u64(a: u64) -> Address {
		Address(a)
	}

	pub fn offset(&self) -> u64 {
		self.0 >> SIZE_TIERS_BITS
	}

	pub fn size_tier(&self) -> u8 {
		(self.0 & ((1 << SIZE_TIERS_BITS) as u64 - 1)) as u8
	}

	pub fn as_u64(&self) -> u64 {
		self.0
	}
}

impl std::fmt::Display for Address {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "addr {:02}:{}", hex(&[self.size_tier()]), self.offset())
	}
}

pub enum PlanOutcome {
	Written,
	NeedReindex,
	Skipped,
}

#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct TableId(u16);

impl TableId {
	pub fn new(col: ColId, index_bits: u8) -> TableId {
		TableId(((col as u16) << 8) | (index_bits as u16))
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

	pub fn is_file_name(col: ColId, name: &str) -> bool {
		name.starts_with(&format!("index_{col:02}_"))
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
		write!(f, "i{:02}-{:02}", self.col(), self.index_bits())
	}
}

impl IndexTable {
	pub fn open_existing(path: &std::path::Path, id: TableId) -> Result<Option<IndexTable>> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = match std::fs::OpenOptions::new().read(true).write(true).open(path.as_path()) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
			Err(e) => return Err(Error::Io(e)),
			Ok(file) => file,
		};

		try_io!(file.set_len(file_size(id.index_bits())));
		let map = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
		log::debug!(target: "parity-db", "Opened existing index {}", id);
		Ok(Some(IndexTable { id, path, map: RwLock::new(Some(map)) }))
	}

	pub fn create_new(path: &std::path::Path, id: TableId) -> IndexTable {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());
		IndexTable { id, path, map: RwLock::new(None) }
	}

	pub fn load_stats(&self) -> Result<ColumnStats> {
		if let Some(map) = &*self.map.read() {
			Ok(ColumnStats::from_slice(try_io!(Ok(
				&map[HEADER_SIZE..HEADER_SIZE + stats::TOTAL_SIZE]
			))))
		} else {
			Ok(ColumnStats::empty())
		}
	}

	pub fn write_stats(&self, stats: &ColumnStats) -> Result<()> {
		if let Some(map) = &mut *self.map.write() {
			let slice = try_io!(Ok(&mut map[HEADER_SIZE..HEADER_SIZE + stats::TOTAL_SIZE]));
			stats.to_slice(slice);
		}
		Ok(())
	}

	fn chunk_at(index: u64, map: &memmap2::MmapMut) -> Result<&[u8; CHUNK_LEN]> {
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		let ptr = unsafe { &*(map[offset..offset + CHUNK_LEN].as_ptr() as *const [u8; CHUNK_LEN]) };
		Ok(try_io!(Ok(ptr)))
	}

	#[cfg(target_arch = "x86_64")]
	fn find_entry(
		&self,
		key_prefix: u64,
		sub_index: usize,
		chunk: &[u8; CHUNK_LEN],
	) -> (Entry, usize) {
		self.find_entry_sse2(key_prefix, sub_index, chunk)
	}

	#[cfg(not(target_arch = "x86_64"))]
	fn find_entry(
		&self,
		key_prefix: u64,
		sub_index: usize,
		chunk: &[u8; CHUNK_LEN],
	) -> (Entry, usize) {
		self.find_entry_base(key_prefix, sub_index, chunk)
	}

	#[cfg(target_arch = "x86_64")]
	fn find_entry_sse2(
		&self,
		key_prefix: u64,
		sub_index: usize,
		chunk: &[u8; CHUNK_LEN],
	) -> (Entry, usize) {
		assert!(chunk.len() >= CHUNK_ENTRIES * 8); // Bound checking (not done by SIMD instructions)
		const _: () = assert!(
			CHUNK_ENTRIES % 4 == 0,
			"We assume here we got buffer with a number of elements that is a multiple of 4"
		);

		let shift = std::cmp::max(32, Entry::address_bits(self.id.index_bits()));
		unsafe {
			let target = _mm_set1_epi32(((key_prefix << self.id.index_bits()) >> shift) as i32);
			let shift_mask = _mm_set_epi64x(0, shift.into());
			let mut i = (sub_index >> 2) << 2; // We keep an alignment of 4
			let mut skip = (sub_index - i) as i32;
			while i + 4 <= CHUNK_ENTRIES {
				// We load the value 2 by 2
				// Then we remove the address by shifting such that the partial key is in the low
				// part
				let first_two = _mm_shuffle_epi32::<0b11011000>(_mm_srl_epi64(
					_mm_loadu_si128(chunk[i * 8..].as_ptr() as *const __m128i),
					shift_mask,
				));
				let last_two = _mm_shuffle_epi32::<0b11011000>(_mm_srl_epi64(
					_mm_loadu_si128(chunk[(i + 2) * 8..].as_ptr() as *const __m128i),
					shift_mask,
				));
				// We set into current the input low parts
				let current = _mm_unpacklo_epi64(first_two, last_two);
				let cmp = _mm_movemask_epi8(_mm_cmpeq_epi32(current, target)) >> (skip * 4);
				if cmp != 0 {
					let position = i + skip as usize + (cmp.trailing_zeros() as usize) / 4;
					return (Self::read_entry(chunk, position), position)
				}
				i += 4;
				skip = 0;
			}
		}
		(Entry::empty(), 0)
	}

	#[cfg(any(not(target_arch = "x86_64"), test))]
	fn find_entry_base(
		&self,
		key_prefix: u64,
		sub_index: usize,
		chunk: &[u8; CHUNK_LEN],
	) -> (Entry, usize) {
		let partial_key = Entry::extract_key(key_prefix, self.id.index_bits());
		for i in sub_index..CHUNK_ENTRIES {
			let entry = Self::read_entry(chunk, i);
			if entry.partial_key(self.id.index_bits()) == partial_key {
				return (entry, i)
			}
		}
		(Entry::empty(), 0)
	}

	// Only returns 54 bits of the actual key.
	pub fn recover_key_prefix(&self, chunk: u64, entry: Entry) -> Key {
		// Restore first 54 bits of the key.
		let partial_key = entry.partial_key(self.id.index_bits());
		let k = 64 - Entry::address_bits(self.id.index_bits());
		let index_key = (chunk << (64 - self.id.index_bits())) |
			(partial_key << (64 - k - self.id.index_bits()));
		let mut key = Key::default();
		key[0..8].copy_from_slice(&index_key.to_be_bytes());
		key
	}

	pub fn get(&self, key: &Key, sub_index: usize, log: &impl LogQuery) -> Result<(Entry, usize)> {
		log::trace!(target: "parity-db", "{}: Querying {}", self.id, hex(key));
		let key = TableKey::index_from_partial(key);
		let chunk_index = self.chunk_index(key);

		if let Some(entry) = log.with_index(self.id, chunk_index, |chunk| {
			log::trace!(target: "parity-db", "{}: Querying overlay at {}", self.id, chunk_index);
			self.find_entry(key, sub_index, chunk)
		}) {
			return Ok(entry)
		}

		if let Some(map) = &*self.map.read() {
			log::trace!(target: "parity-db", "{}: Querying chunk at {}", self.id, chunk_index);
			let chunk = Self::chunk_at(chunk_index, map)?;
			return Ok(self.find_entry(key, sub_index, chunk))
		}
		Ok((Entry::empty(), 0))
	}

	pub fn entries(&self, chunk_index: u64, log: &impl LogQuery) -> Result<[Entry; CHUNK_ENTRIES]> {
		let mut chunk = [0; CHUNK_LEN];
		if let Some(entry) =
			log.with_index(self.id, chunk_index, |chunk| Self::transmute_chunk(*chunk))
		{
			return Ok(entry)
		}
		if let Some(map) = &*self.map.read() {
			let source = Self::chunk_at(chunk_index, map)?;
			chunk.copy_from_slice(source);
			return Ok(Self::transmute_chunk(chunk))
		}
		Ok(Self::transmute_chunk(EMPTY_CHUNK))
	}

	#[inline(always)]
	fn transmute_chunk(chunk: [u8; CHUNK_LEN]) -> [Entry; CHUNK_ENTRIES] {
		let mut result: [Entry; CHUNK_ENTRIES] = unsafe { std::mem::transmute(chunk) };
		if !cfg!(target_endian = "little") {
			for item in result.iter_mut() {
				*item = Entry::from_u64(u64::from_le(item.0));
			}
		}
		result
	}

	#[inline(always)]
	fn write_entry(entry: &Entry, at: usize, chunk: &mut [u8; CHUNK_LEN]) {
		chunk[at * 8..at * 8 + 8].copy_from_slice(&entry.as_u64().to_le_bytes());
	}

	#[inline(always)]
	fn read_entry(chunk: &[u8; CHUNK_LEN], at: usize) -> Entry {
		Entry::from_u64(u64::from_le_bytes(chunk[at * 8..at * 8 + 8].try_into().unwrap()))
	}

	#[inline(always)]
	fn chunk_index(&self, key_prefix: u64) -> u64 {
		key_prefix >> (ENTRY_BITS - self.id.index_bits())
	}

	fn plan_insert_chunk(
		&self,
		key_prefix: u64,
		address: Address,
		source: &[u8],
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let chunk_index = self.chunk_index(key_prefix);
		if address.as_u64() > Entry::last_address(self.id.index_bits()) {
			// Address overflow
			log::warn!(target: "parity-db", "{}: Address space overflow at {}: {}", self.id, chunk_index, address);
			return Ok(PlanOutcome::NeedReindex)
		}
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let partial_key = Entry::extract_key(key_prefix, self.id.index_bits());
		let new_entry = Entry::new(address, partial_key, self.id.index_bits());
		if let Some(i) = sub_index {
			let entry = Self::read_entry(&chunk, i);
			assert_eq!(
				entry.partial_key(self.id.index_bits()),
				new_entry.partial_key(self.id.index_bits())
			);
			Self::write_entry(&new_entry, i, &mut chunk);
			log::trace!(target: "parity-db", "{}: Replaced at {}.{}: {}", self.id, chunk_index, i, new_entry.address(self.id.index_bits()));
			log.insert_index(self.id, chunk_index, i as u8, &chunk);
			return Ok(PlanOutcome::Written)
		}
		for i in 0..CHUNK_ENTRIES {
			let entry = Self::read_entry(&chunk, i);
			if entry.is_empty() {
				Self::write_entry(&new_entry, i, &mut chunk);
				log::trace!(target: "parity-db", "{}: Inserted at {}.{}: {}", self.id, chunk_index, i, new_entry.address(self.id.index_bits()));
				log.insert_index(self.id, chunk_index, i as u8, &chunk);
				return Ok(PlanOutcome::Written)
			}
		}
		log::debug!(target: "parity-db", "{}: Index chunk full at {}", self.id, chunk_index);
		Ok(PlanOutcome::NeedReindex)
	}

	pub fn write_insert_plan(
		&self,
		key: &Key,
		address: Address,
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Inserting {} -> {}", self.id, hex(key), address);
		let key_prefix = TableKey::index_from_partial(key);
		let chunk_index = self.chunk_index(key_prefix);

		if let Some(chunk) = log.with_index(self.id, chunk_index, |chunk| *chunk) {
			return self.plan_insert_chunk(key_prefix, address, &chunk, sub_index, log)
		}

		if let Some(map) = &*self.map.read() {
			let chunk = Self::chunk_at(chunk_index, map)?;
			return self.plan_insert_chunk(key_prefix, address, chunk, sub_index, log)
		}

		let chunk = &EMPTY_CHUNK;
		self.plan_insert_chunk(key_prefix, address, chunk, sub_index, log)
	}

	fn plan_remove_chunk(
		&self,
		key_prefix: u64,
		source: &[u8],
		sub_index: usize,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let mut chunk = [0; CHUNK_LEN];
		chunk.copy_from_slice(source);
		let chunk_index = self.chunk_index(key_prefix);
		let partial_key = Entry::extract_key(key_prefix, self.id.index_bits());

		let i = sub_index;
		let entry = Self::read_entry(&chunk, i);
		if !entry.is_empty() && entry.partial_key(self.id.index_bits()) == partial_key {
			let new_entry = Entry::empty();
			Self::write_entry(&new_entry, i, &mut chunk);
			log.insert_index(self.id, chunk_index, i as u8, &chunk);
			log::trace!(target: "parity-db", "{}: Removed at {}.{}", self.id, chunk_index, i);
			return Ok(PlanOutcome::Written)
		}
		Ok(PlanOutcome::Skipped)
	}

	pub fn write_remove_plan(
		&self,
		key: &Key,
		sub_index: usize,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Removing {}", self.id, hex(key));
		let key_prefix = TableKey::index_from_partial(key);

		let chunk_index = self.chunk_index(key_prefix);

		if let Some(chunk) = log.with_index(self.id, chunk_index, |chunk| *chunk) {
			return self.plan_remove_chunk(key_prefix, &chunk, sub_index, log)
		}

		if let Some(map) = &*self.map.read() {
			let chunk = Self::chunk_at(chunk_index, map)?;
			return self.plan_remove_chunk(key_prefix, chunk, sub_index, log)
		}

		Ok(PlanOutcome::Skipped)
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut map = self.map.upgradable_read();
		if map.is_none() {
			let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
			let file = try_io!(std::fs::OpenOptions::new()
				.write(true)
				.read(true)
				.create_new(true)
				.open(self.path.as_path()));
			log::debug!(target: "parity-db", "Created new index {}", self.id);
			//TODO: check for potential overflows on 32-bit platforms
			try_io!(file.set_len(file_size(self.id.index_bits())));
			let mut mmap = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
			self.madvise_random(&mut mmap);
			*wmap = Some(mmap);
			map = RwLockWriteGuard::downgrade_to_upgradable(wmap);
		}

		let map = map.as_ref().unwrap();
		let offset = META_SIZE + index as usize * CHUNK_LEN;
		// Nasty mutable pointer cast. We do ensure that all chunks that are being written are
		// accessed through the overlay in other threads.
		let ptr: *mut u8 = map.as_ptr() as *mut u8;
		let chunk: &mut [u8] = unsafe {
			let ptr = ptr.add(offset);
			std::slice::from_raw_parts_mut(ptr, CHUNK_LEN)
		};
		let mut mask_buf = [0u8; 8];
		log.read(&mut mask_buf)?;
		let mut mask = u64::from_le_bytes(mask_buf);
		while mask != 0 {
			let i = mask.trailing_zeros();
			mask &= !(1 << i);
			log.read(try_io!(Ok(
				&mut chunk[i as usize * ENTRY_BYTES..(i as usize + 1) * ENTRY_BYTES]
			)))?;
		}
		log::trace!(target: "parity-db", "{}: Enacted chunk {}", self.id, index);
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index >= self.id.total_entries() {
			return Err(Error::Corruption("Bad index".into()))
		}
		let mut buf = [0u8; 8];
		log.read(&mut buf)?;
		let mut mask = u64::from_le_bytes(buf);
		while mask != 0 {
			let i = mask.trailing_zeros();
			mask &= !(1 << i);
			log.read(&mut buf[..])?;
		}
		log::trace!(target: "parity-db", "{}: Validated chunk {}", self.id, index);
		Ok(())
	}

	pub fn skip_plan(log: &mut LogReader) -> Result<()> {
		let mut buf = [0u8; 8];
		log.read(&mut buf)?;
		let mut mask = u64::from_le_bytes(buf);
		while mask != 0 {
			let i = mask.trailing_zeros();
			mask &= !(1 << i);
			log.read(&mut buf[..])?;
		}
		Ok(())
	}

	pub fn drop_file(self) -> Result<()> {
		drop(self.map);
		try_io!(std::fs::remove_file(self.path.as_path()));
		log::debug!(target: "parity-db", "{}: Dropped table", self.id);
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Some(map) = &*self.map.read() {
			// Flush everything except stats.
			try_io!(map.flush_range(META_SIZE, map.len() - META_SIZE));
		}
		Ok(())
	}

	#[cfg(unix)]
	fn madvise_random(&self, map: &mut memmap2::MmapMut) {
		unsafe {
			libc::madvise(
				map.as_mut_ptr() as _,
				file_size(self.id.index_bits()) as usize,
				libc::MADV_RANDOM,
			);
		}
	}

	#[cfg(not(unix))]
	fn madvise_random(&self, _map: &mut memmap2::MmapMut) {}
}

#[cfg(test)]
mod test {
	use super::*;
	use rand::{Rng, SeedableRng};
	use std::path::PathBuf;

	#[cfg(feature = "bench")]
	use test::Bencher;
	#[cfg(feature = "bench")]
	extern crate test;

	#[test]
	fn test_entries() {
		let mut chunk = IndexTable::transmute_chunk(EMPTY_CHUNK);
		let mut chunk2 = EMPTY_CHUNK;
		for (i, chunk) in chunk.iter_mut().enumerate().take(CHUNK_ENTRIES) {
			use std::{
				collections::hash_map::DefaultHasher,
				hash::{Hash, Hasher},
			};
			let mut hasher = DefaultHasher::new();
			i.hash(&mut hasher);
			let hash = hasher.finish();
			let entry = Entry::from_u64(hash);
			IndexTable::write_entry(&entry, i, &mut chunk2);
			*chunk = entry;
		}

		assert!(IndexTable::transmute_chunk(chunk2) == chunk);
	}

	#[test]
	fn test_find_entries() {
		let partial_keys = [1, 1 << 10, 1 << 20];
		for index_bits in [16, 18, 20, 22] {
			let index_table = IndexTable {
				id: TableId(index_bits.into()),
				map: RwLock::new(None),
				path: PathBuf::new(),
			};

			let data_address = Address::from_u64((1 << index_bits) - 1);

			let mut chunk = [0; CHUNK_ENTRIES * 8];
			for (i, partial_key) in partial_keys.iter().enumerate() {
				chunk[i * 8..(i + 1) * 8].copy_from_slice(
					&Entry::new(data_address, *partial_key, index_bits).as_u64().to_le_bytes(),
				);
			}

			for partial_key in &partial_keys {
				let key_prefix = *partial_key << (CHUNK_ENTRIES_BITS + SIZE_TIERS_BITS);
				#[cfg(target_arch = "x86_64")]
				assert_eq!(
					index_table.find_entry_sse2(key_prefix, 0, &chunk).0.partial_key(index_bits),
					*partial_key
				);
				assert_eq!(
					index_table.find_entry_base(key_prefix, 0, &chunk).0.partial_key(index_bits),
					*partial_key
				);
			}
		}
	}

	#[test]
	fn test_find_any_entry() {
		let table =
			IndexTable { id: TableId(18), map: RwLock::new(None), path: Default::default() };
		let mut chunk = [0u8; CHUNK_LEN];
		let mut entries = [Entry::empty(); CHUNK_ENTRIES];
		let mut keys = [0u64; CHUNK_ENTRIES];
		let mut rng = rand::prelude::SmallRng::from_seed(Default::default());
		for i in 0..CHUNK_ENTRIES {
			keys[i] = rng.gen();
			let partial_key = Entry::extract_key(keys[i], 18);
			let e = Entry::new(Address::new(0, 0), partial_key, 18);
			entries[i] = e;
			IndexTable::write_entry(&e, i, &mut chunk);
		}

		for target in 0..CHUNK_ENTRIES {
			for start_pos in 0..CHUNK_ENTRIES {
				let (e, i) = table.find_entry_base(keys[target], start_pos, &chunk);
				if start_pos <= target {
					assert_eq!((e.as_u64(), i), (entries[target].as_u64(), target));
				} else {
					assert_eq!((e.as_u64(), i), (Entry::empty().as_u64(), 0));
				}
				#[cfg(target_arch = "x86_64")]
				{
					let (e, i) = table.find_entry_sse2(keys[target], start_pos, &chunk);
					if start_pos <= target {
						assert_eq!((e.as_u64(), i), (entries[target].as_u64(), target));
					} else {
						assert_eq!((e.as_u64(), i), (Entry::empty().as_u64(), 0));
					}
				}
			}
		}
	}

	#[test]
	fn test_find_entry_same_value() {
		let table =
			IndexTable { id: TableId(18), map: RwLock::new(None), path: Default::default() };
		let mut chunk = [0u8; CHUNK_LEN];
		let key = 0x4242424242424242;
		let partial_key = Entry::extract_key(key, 18);
		let entry = Entry::new(Address::new(0, 0), partial_key, 18);
		for i in 0..CHUNK_ENTRIES {
			IndexTable::write_entry(&entry, i, &mut chunk);
		}

		for start_pos in 0..CHUNK_ENTRIES {
			let (_, i) = table.find_entry_base(key, start_pos, &chunk);
			assert_eq!(i, start_pos);
			#[cfg(target_arch = "x86_64")]
			{
				let (_, i) = table.find_entry_sse2(key, start_pos, &chunk);
				assert_eq!(i, start_pos);
			}
		}
	}

	#[cfg(feature = "bench")]
	fn bench_find_entry_internal<
		F: Fn(&IndexTable, u64, usize, &[u8; CHUNK_LEN]) -> (Entry, usize),
	>(
		b: &mut Bencher,
		f: F,
	) {
		let table =
			IndexTable { id: TableId(18), map: RwLock::new(None), path: Default::default() };
		let mut chunk = [0u8; CHUNK_LEN];
		let mut keys = [0u64; CHUNK_ENTRIES];
		let mut rng = rand::prelude::SmallRng::from_seed(Default::default());
		for i in 0..CHUNK_ENTRIES {
			keys[i] = rng.gen();
			let partial_key = Entry::extract_key(keys[i], 18);
			let e = Entry::new(Address::new(0, 0), partial_key, 18);
			IndexTable::write_entry(&e, i, &mut chunk);
		}

		let mut index = 0;
		b.iter(|| {
			let x = f(&table, keys[index], 0, &chunk).1;
			assert_eq!(x, index);
			index = (index + 1) % CHUNK_ENTRIES;
		});
	}

	#[cfg(feature = "bench")]
	#[bench]
	fn bench_find_entry(b: &mut Bencher) {
		bench_find_entry_internal(b, IndexTable::find_entry_base)
	}

	#[cfg(feature = "bench")]
	#[cfg(target_arch = "x86_64")]
	#[bench]
	fn bench_find_entry_sse(b: &mut Bencher) {
		bench_find_entry_internal(b, IndexTable::find_entry_sse2)
	}
}
