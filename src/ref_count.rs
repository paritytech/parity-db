// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::{ColId, MIN_REF_COUNT_BITS},
	error::{try_io, Error, Result},
	file::madvise_random,
	index::{Address, PlanOutcome},
	log::{LogQuery, LogReader, LogWriter},
	parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard},
};
use std::convert::TryInto;

const CHUNK_LEN: usize = CHUNK_ENTRIES * ENTRY_BYTES;
const CHUNK_ENTRIES: usize = 1 << CHUNK_ENTRIES_BITS;
const CHUNK_ENTRIES_BITS: u8 = 5;
const META_SIZE: usize = 0;
const ENTRY_BITS: u8 = 128;
pub const ENTRY_BYTES: usize = ENTRY_BITS as usize / 8;

const EMPTY_CHUNK: Chunk = Chunk([0u8; CHUNK_LEN]);
const EMPTY_ENTRIES: [Entry; CHUNK_ENTRIES] = [Entry::empty(); CHUNK_ENTRIES];

#[allow(clippy::assertions_on_constants)]
const _: () = assert!(META_SIZE % CHUNK_LEN == 0);

#[inline]
fn chunk_to_file_index(mut chunk_index: u64, max_chunks: Option<u64>) -> (u64, usize) {
	// count meta in.
	chunk_index += (META_SIZE / CHUNK_LEN) as u64;

	if let Some(i) = max_chunks {
		(chunk_index % i, (chunk_index / i) as usize)
	} else {
		(chunk_index, 0)
	}
}

#[repr(C, align(8))]
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Chunk(pub [u8; CHUNK_LEN]);

#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Entry(u64, u64);

impl Entry {
	#[inline]
	fn new(address: Address, ref_count: u64) -> Entry {
		Entry(address.as_u64(), ref_count)
	}

	#[inline]
	pub fn address(&self) -> Address {
		Address::from_u64(self.0)
	}

	#[inline]
	pub fn ref_count(&self) -> u64 {
		self.1
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.0 == 0
	}

	pub fn as_u128(&self) -> u128 {
		self.0 as u128 | (self.1 as u128) << 64
	}

	const fn empty() -> Self {
		Entry(0, 0)
	}

	fn from_u128(e: u128) -> Self {
		Entry((e & u64::MAX as u128) as u64, (e >> 64) as u64)
	}
}

#[derive(Debug)]
pub struct RefCountTable {
	pub id: RefCountTableId,
	map: RwLock<Vec<memmap2::MmapMut>>,
	path_base: std::path::PathBuf,
	max_size: Option<u64>,
	max_chunks: Option<u64>,
}

fn total_entries(index_bits: u8) -> u64 {
	total_chunks(index_bits) * CHUNK_ENTRIES as u64
}

fn total_chunks(index_bits: u8) -> u64 {
	1u64 << index_bits
}

fn file_size(index_bits: u8, max_size: Option<u64>) -> u64 {
	let total = total_entries(index_bits) * 8 + META_SIZE as u64;
	max_size.map(|m| std::cmp::min(m, total)).unwrap_or(total)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RefCountTableId(u16);

impl RefCountTableId {
	pub fn new(col: ColId, index_bits: u8) -> RefCountTableId {
		RefCountTableId(((col as u16) << 8) | (index_bits as u16))
	}

	pub fn from_u16(id: u16) -> RefCountTableId {
		RefCountTableId(id)
	}

	pub fn col(&self) -> ColId {
		(self.0 >> 8) as ColId
	}

	pub fn index_bits(&self) -> u8 {
		(self.0 & 0xff) as u8
	}

	pub fn file_name(&self, file_index: Option<u32>) -> String {
		if let Some(i) = file_index {
			format!("refcount_{:02}_{}_{:08x}", self.col(), self.index_bits(), i)
		} else {
			format!("refcount_{:02}_{}", self.col(), self.index_bits())
		}
	}

	pub fn is_file_name(col: ColId, name: &str) -> bool {
		name.starts_with(&format!("refcount_{col:02}_"))
	}

	pub fn as_u16(&self) -> u16 {
		self.0
	}

	pub fn total_chunks(&self) -> u64 {
		total_chunks(self.index_bits())
	}

	pub fn log_index(&self) -> usize {
		self.col() as usize * (64 - MIN_REF_COUNT_BITS) as usize + self.index_bits() as usize
	}

	pub fn from_log_index(i: usize) -> Self {
		let col = i / (64 - MIN_REF_COUNT_BITS) as usize;
		let bits = i % (64 - MIN_REF_COUNT_BITS) as usize;
		RefCountTableId::new(col as ColId, bits as u8)
	}

	pub const fn max_log_indicies(num_columns: usize) -> usize {
		(64 - MIN_REF_COUNT_BITS) as usize * num_columns
	}
}

impl std::fmt::Display for RefCountTableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "rc{:02}-{:02}", self.col(), self.index_bits())
	}
}

impl RefCountTable {
	pub fn open_existing(
		path: &std::path::Path,
		id: RefCountTableId,
		max_size: Option<usize>,
	) -> Result<Option<RefCountTable>> {
		let path_base: std::path::PathBuf = path.into();
		let mut maps = Vec::new();
		let max_chunks = max_size.map(|s| (s * 1024 * 1024 / CHUNK_LEN) as u64);
		let max_size = max_chunks.map(|c| c * CHUNK_LEN as u64);
		for i in 0.. {
			let mut path = path_base.clone();
			path.push(id.file_name(max_size.is_some().then(|| i)));

			let file = match std::fs::OpenOptions::new().read(true).write(true).open(path.as_path())
			{
				Err(e) if e.kind() == std::io::ErrorKind::NotFound =>
					if i == 0 {
						return Ok(None)
					} else {
						break
					},
				Err(e) => return Err(Error::Io(e)),
				Ok(file) => file,
			};

			try_io!(file.set_len(file_size(id.index_bits(), max_size)));
			let mut map = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
			madvise_random(&mut map);
			log::debug!(target: "parity-db", "Opened existing refcount table {}", id);
			maps.push(map);
			if max_size.is_none() {
				break;
			}
		}
		Ok(Some(RefCountTable { id, path_base, map: RwLock::new(maps), max_chunks, max_size }))
	}

	pub fn create_new(
		path: &std::path::Path,
		id: RefCountTableId,
		max_size: Option<usize>,
	) -> RefCountTable {
		let max_chunks = max_size.map(|s| (s * 1024 * 1024 / CHUNK_LEN) as u64);
		let max_size = max_chunks.map(|c| c * CHUNK_LEN as u64);
		RefCountTable {
			id,
			path_base: path.into(),
			map: RwLock::new(Vec::new()),
			max_size,
			max_chunks,
		}
	}

	fn chunk_at(index: u64, map: &memmap2::MmapMut) -> Result<&Chunk> {
		let offset = index as usize * CHUNK_LEN;
		let ptr = unsafe { &*(map[offset..offset + CHUNK_LEN].as_ptr() as *const Chunk) };
		Ok(try_io!(Ok(ptr)))
	}

	fn find_entry(&self, address: u64, chunk: &Chunk) -> Option<(Entry, usize)> {
		self.find_entry_base(address, chunk)
	}

	fn find_entry_base(&self, address: u64, chunk: &Chunk) -> Option<(Entry, usize)> {
		for i in 0..CHUNK_ENTRIES {
			let entry = Self::read_entry(chunk, i);
			if entry.address().as_u64() == address && !entry.is_empty() {
				return Some((entry, i))
			}
		}
		None
	}

	pub fn get(&self, address: Address, log: &impl LogQuery) -> Result<Option<(u64, usize)>> {
		log::trace!(target: "parity-db", "{}: Querying ref count {}", self.id, address);
		let chunk_index = self.chunk_index(address);

		if let Some(entry) = log.ref_count(self.id, chunk_index, |chunk| {
			log::trace!(target: "parity-db", "{}: Querying ref count overlay at {}", self.id, chunk_index);
			self.find_entry(address.as_u64(), chunk)
		}) {
			return Ok(entry.map(|(e, sub_index)| (e.ref_count(), sub_index)))
		}

		let (chunk_index, file_index) = chunk_to_file_index(chunk_index, self.max_chunks);
		if let Some(map) = self.map.read().get(file_index) {
			log::trace!(target: "parity-db", "{}: Querying ref count chunk at {}", self.id, chunk_index);
			let chunk = Self::chunk_at(chunk_index, map)?;
			return Ok(self
				.find_entry(address.as_u64(), chunk)
				.map(|(e, sub_index)| (e.ref_count(), sub_index)))
		}
		Ok(None)
	}

	pub fn entries(&self, chunk_index: u64, log: &impl LogQuery) -> Result<[Entry; CHUNK_ENTRIES]> {
		if let Some(entry) =
			log.ref_count(self.id, chunk_index, |chunk| *Self::transmute_chunk(chunk))
		{
			return Ok(entry)
		}
		let (chunk_index, file_index) = chunk_to_file_index(chunk_index, self.max_chunks);
		if let Some(map) = self.map.read().get(file_index) {
			let chunk = Self::chunk_at(chunk_index, map)?;
			return Ok(*Self::transmute_chunk(chunk))
		}
		Ok(EMPTY_ENTRIES)
	}

	pub fn table_entries(&self, chunk_index: u64) -> Result<[Entry; CHUNK_ENTRIES]> {
		let (chunk_index, file_index) = chunk_to_file_index(chunk_index, self.max_chunks);
		if let Some(map) = self.map.read().get(file_index) {
			let chunk = Self::chunk_at(chunk_index, map)?;
			return Ok(*Self::transmute_chunk(chunk))
		}
		Ok(EMPTY_ENTRIES)
	}

	#[inline(always)]
	fn transmute_chunk(chunk: &Chunk) -> &[Entry; CHUNK_ENTRIES] {
		unsafe { std::mem::transmute(chunk) }
	}

	#[inline(always)]
	fn write_entry(entry: &Entry, at: usize, chunk: &mut Chunk) {
		chunk.0[at * ENTRY_BYTES..at * ENTRY_BYTES + ENTRY_BYTES]
			.copy_from_slice(&entry.as_u128().to_le_bytes());
	}

	#[inline(always)]
	fn read_entry(chunk: &Chunk, at: usize) -> Entry {
		Entry::from_u128(u128::from_le_bytes(
			chunk.0[at * ENTRY_BYTES..at * ENTRY_BYTES + ENTRY_BYTES].try_into().unwrap(),
		))
	}

	#[inline(always)]
	fn chunk_index(&self, address: Address) -> u64 {
		use std::hash::Hasher;
		let mut hasher = siphasher::sip::SipHasher::new();
		hasher.write_u64(address.as_u64());
		let hash = hasher.finish();
		hash >> (64 - self.id.index_bits())
	}

	fn plan_insert_chunk(
		&self,
		address: Address,
		ref_count: u64,
		mut chunk: Chunk,
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let chunk_index = self.chunk_index(address);
		let new_entry = Entry::new(address, ref_count);
		if let Some(i) = sub_index {
			let entry = Self::read_entry(&chunk, i);
			assert_eq!(entry.address(), new_entry.address());
			Self::write_entry(&new_entry, i, &mut chunk);
			log::trace!(target: "parity-db", "{}: Replaced ref count at {}.{}: {}", self.id, chunk_index, i, new_entry.address());
			log.insert_ref_count(self.id, chunk_index, i as u8, chunk);
			return Ok(PlanOutcome::Written)
		}
		for i in 0..CHUNK_ENTRIES {
			let entry = Self::read_entry(&chunk, i);
			if entry.is_empty() {
				Self::write_entry(&new_entry, i, &mut chunk);
				log::trace!(target: "parity-db", "{}: Inserted ref count at {}.{}: {}", self.id, chunk_index, i, new_entry.address());
				log.insert_ref_count(self.id, chunk_index, i as u8, chunk);
				return Ok(PlanOutcome::Written)
			}
		}
		log::debug!(target: "parity-db", "{}: Ref count chunk full at {}", self.id, chunk_index);
		Ok(PlanOutcome::NeedReindex)
	}

	pub fn write_insert_plan(
		&self,
		address: Address,
		ref_count: u64,
		sub_index: Option<usize>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Inserting ref count {} -> {}", self.id, address, ref_count);
		let chunk_index = self.chunk_index(address);

		if let Some(chunk) = log.ref_count(self.id, chunk_index, |chunk| chunk.clone()) {
			return self.plan_insert_chunk(address, ref_count, chunk, sub_index, log)
		}

		let (chunk_index, file_index) = chunk_to_file_index(chunk_index, self.max_chunks);
		if let Some(map) = self.map.read().get(file_index) {
			let chunk = Self::chunk_at(chunk_index, map)?.clone();
			return self.plan_insert_chunk(address, ref_count, chunk, sub_index, log)
		}

		let chunk = EMPTY_CHUNK.clone();
		self.plan_insert_chunk(address, ref_count, chunk, sub_index, log)
	}

	fn plan_remove_chunk(
		&self,
		address: Address,
		mut chunk: Chunk,
		sub_index: usize,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let chunk_index = self.chunk_index(address);

		let i = sub_index;
		let entry = Self::read_entry(&chunk, i);
		if !entry.is_empty() && entry.address() == address {
			let new_entry = Entry::empty();
			Self::write_entry(&new_entry, i, &mut chunk);
			log.insert_ref_count(self.id, chunk_index, i as u8, chunk);
			log::trace!(target: "parity-db", "{}: Removed ref count at {}.{}", self.id, chunk_index, i);
			return Ok(PlanOutcome::Written)
		}
		assert!(false);
		Ok(PlanOutcome::Skipped)
	}

	pub fn write_remove_plan(
		&self,
		address: Address,
		sub_index: usize,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		log::trace!(target: "parity-db", "{}: Removing ref count {}", self.id, address);
		let chunk_index = self.chunk_index(address);

		if let Some(chunk) = log.ref_count(self.id, chunk_index, |chunk| chunk.clone()) {
			return self.plan_remove_chunk(address, chunk, sub_index, log)
		}

		let (chunk_index, file_index) = chunk_to_file_index(chunk_index, self.max_chunks);
		if let Some(map) = self.map.read().get(file_index) {
			let chunk = Self::chunk_at(chunk_index, map)?.clone();
			return self.plan_remove_chunk(address, chunk, sub_index, log)
		}

		assert!(false);
		Ok(PlanOutcome::Skipped)
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut map = self.map.upgradable_read();
		let (chunk_index, file_index) = chunk_to_file_index(index, self.max_chunks);
		let map_len = map.len();
		if map_len <= file_index {
			let mut wmap = RwLockUpgradableReadGuard::upgrade(map);
			for i in map_len..file_index + 1 {
				let mut path = self.path_base.clone();
				path.push(self.id.file_name(self.max_size.is_some().then(|| i as u32)));
				let file = try_io!(std::fs::OpenOptions::new()
					.write(true)
					.read(true)
					.create_new(true)
					.open(path.as_path()));
				log::debug!(target: "parity-db", "Created new ref count {}", self.id);
				try_io!(file.set_len(file_size(self.id.index_bits(), self.max_size)));
				let mut mmap = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
				madvise_random(&mut mmap);
				wmap.push(mmap);
			}
			map = RwLockWriteGuard::downgrade_to_upgradable(wmap);
		}

		let map = map.get(file_index).unwrap();
		let offset = chunk_index as usize * CHUNK_LEN;
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
		log::trace!(target: "parity-db", "{}: Enacted ref count chunk {}", self.id, index);
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index >= self.id.total_chunks() {
			return Err(Error::Corruption("Bad index".into()))
		}
		let mut mask_buf = [0u8; 8];
		let mut entry_buf = [0u8; ENTRY_BYTES];
		log.read(&mut mask_buf)?;
		let mut mask = u64::from_le_bytes(mask_buf);
		while mask != 0 {
			let i = mask.trailing_zeros();
			mask &= !(1 << i);
			log.read(&mut entry_buf[..])?;
		}
		log::trace!(target: "parity-db", "{}: Validated ref count chunk {}", self.id, index);
		Ok(())
	}

	pub fn skip_plan(log: &mut LogReader) -> Result<()> {
		let mut mask_buf = [0u8; 8];
		let mut entry_buf = [0u8; ENTRY_BYTES];
		log.read(&mut mask_buf)?;
		let mut mask = u64::from_le_bytes(mask_buf);
		while mask != 0 {
			let i = mask.trailing_zeros();
			mask &= !(1 << i);
			log.read(&mut entry_buf[..])?;
		}
		Ok(())
	}

	pub fn drop_file(self) -> Result<()> {
		drop(self.map);
		for i in 0.. {
			let mut path = self.path_base.clone();
			path.push(self.id.file_name(self.max_size.is_some().then(|| i)));
			match std::fs::remove_file(path.as_path()) {
				Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
				Err(e) => return Err(Error::Io(e)),
				Ok(()) => (),
			};
			if self.max_size.is_none() {
				break;
			}
		}
		log::debug!(target: "parity-db", "{}: Dropped ref count table", self.id);
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		// Flush everything except stats.
		let mut start = META_SIZE;
		let maps = self.map.read();
		for map in maps.iter() {
			try_io!(map.flush_range(start, map.len() - start)); // TODO not flush range when start 0
			start = 0;
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use rand::{Rng, SeedableRng};
	use siphasher::sip128::Hasher128;

	#[cfg(feature = "bench")]
	use test::Bencher;
	#[cfg(feature = "bench")]
	extern crate test;

	#[test]
	fn test_entries() {
		let mut chunk = RefCountTable::transmute_chunk(&EMPTY_CHUNK).clone();
		let mut chunk2 = EMPTY_CHUNK;
		for (i, chunk_entry) in chunk.iter_mut().enumerate().take(CHUNK_ENTRIES) {
			use std::hash::Hash;
			let mut hasher = siphasher::sip128::SipHasher::new();
			i.hash(&mut hasher);
			let hash = hasher.finish128().as_u128();
			let entry = Entry::from_u128(hash);
			RefCountTable::write_entry(&entry, i, &mut chunk2);
			assert_eq!(entry.as_u128(), RefCountTable::read_entry(&chunk2, i).as_u128());
			*chunk_entry = entry;
		}

		assert!(RefCountTable::transmute_chunk(&chunk2) == &chunk);
	}

	#[test]
	fn test_find_any_entry() {
		let table = RefCountTable {
			id: RefCountTableId(18),
			map: RwLock::new(Vec::new()),
			path_base: Default::default(),
			max_size: None,
			max_chunks: None,
		};
		let mut chunk = Chunk([0u8; CHUNK_LEN]);
		let mut entries = [Entry::empty(); CHUNK_ENTRIES];
		let mut addresses = [0u64; CHUNK_ENTRIES];
		let mut rng = rand::prelude::SmallRng::from_seed(Default::default());
		for i in 0..CHUNK_ENTRIES {
			addresses[i] = rng.gen();
			let ref_count = rng.gen();
			let e = Entry::new(Address::from_u64(addresses[i]), ref_count);
			entries[i] = e;
			RefCountTable::write_entry(&e, i, &mut chunk);
		}

		for target in 0..CHUNK_ENTRIES {
			let result = table.find_entry_base(addresses[target], &chunk);
			assert!(result.is_some());
			if let Some((e, i)) = result {
				assert_eq!((e.as_u128(), i), (entries[target].as_u128(), target));
			}
		}
	}

	#[test]
	fn test_find_entry_zero() {
		let table = RefCountTable {
			id: RefCountTableId(16),
			map: RwLock::new(Vec::new()),
			path_base: Default::default(),
			max_size: None,
			max_chunks: None,
		};
		let mut chunk = Chunk([0u8; CHUNK_LEN]);
		let address = Address::new(1, 1);
		let entry = Entry::new(address, 0);

		// Write at index 1. Index 0 contains an empty entry.
		RefCountTable::write_entry(&entry, 1, &mut chunk);

		let result = table.find_entry_base(address.as_u64(), &chunk);
		assert!(result.is_some());
		if let Some((_e, i)) = result {
			assert_eq!(i, 1);
		}
	}

	#[cfg(feature = "bench")]
	#[bench]
	fn bench_find_entry(b: &mut Bencher) {
		let table = RefCountTable {
			id: RefCountTableId(18),
			map: RwLock::new(None),
			path: Default::default(),
		};
		let mut chunk = Chunk([0u8; CHUNK_LEN]);
		let mut addresses = [0u64; CHUNK_ENTRIES];
		let mut rng = rand::prelude::SmallRng::from_seed(Default::default());
		for i in 0..CHUNK_ENTRIES {
			addresses[i] = rng.gen();
			let ref_count = rng.gen();
			let e = Entry::new(Address::from_u64(addresses[i]), ref_count);
			RefCountTable::write_entry(&e, i, &mut chunk);
		}

		let mut index = 0;
		b.iter(|| {
			let result = RefCountTable::find_entry_base(&table, addresses[index], &chunk);
			assert!(result.is_some());
			if let Some((_e, i)) = result {
				assert_eq!(i, index);
			}
			index = (index + 1) % CHUNK_ENTRIES;
		});
	}
}
