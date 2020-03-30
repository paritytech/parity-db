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

use std::sync::Arc;
use std::io::{Seek, Read, Write};
use parking_lot::{RwLock, Mutex, Condvar};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use kvdb::{DBOp, DBTransaction};

const KEY_LEN: usize = 32;
const MAX_AHEAD: usize = 32;
const META_SIZE: isize = 8 * 1024; // typical SSD page size
const MAX_REBALANCE_BATCH: u32 = 65536;

pub type Key=[u8; KEY_LEN];
pub type Value = Vec<u8>;

type ColId = u8;

pub type Result<T> = std::result::Result<T, Error>;
const EMPTY: Key = [0u8; KEY_LEN];

fn hash(key: &[u8]) -> Key {
	let mut k = Key::default();
	k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &[], key).as_bytes());
	//log::trace!(target: "parity-db", "HASH {} = {}", hex(&key), hex(&k));
	k
}

/// Simple wrapper to display hex representation of bytes.
pub struct HexDisplay<'a>(&'a [u8]);

impl<'a> HexDisplay<'a> {
	pub fn from<R: std::convert::AsRef<[u8]>>(d: &'a R) -> Self { HexDisplay(d.as_ref()) }
}

impl<'a> std::fmt::Display for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			f.write_fmt(format_args!("{:02x}", byte))?;
		}
		Ok(())
	}
}

impl<'a> std::fmt::Debug for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			f.write_fmt(format_args!("{:02x}", byte))?;
		}
		Ok(())
	}
}

fn hex<'a, R: std::convert::AsRef<[u8]>>(r: &'a R) -> HexDisplay<'a> {
	HexDisplay::from(r)
}

#[derive(Debug)]
pub enum Error {
	Io(std::io::Error),
	Corruption(String),
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Self {
		Error::Io(e)
	}
}

enum RebalanceProgress {
	InProgress((u64, u64)),
	Inactive,
}
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct BucketId(u32);

impl BucketId {
	fn new(col: ColId, entry_size: u16, index_bits: u8) -> BucketId {
		BucketId(((col as u32) << 24) | ((entry_size as u32) << 8) | index_bits as u32)
	}

	fn col(&self) -> ColId {
		(self.0 >> 24) as ColId
	}

	fn index_bits(&self) -> ColId {
		(self.0 & 0xff) as ColId
	}

	fn entry_size(&self) -> u16 {
		((self.0 >> 8) & 0xffff) as u16
	}
	fn file_name(&self) -> String {
		format!("c{:02}_{}_{}", self.col(), self.index_bits(), self.entry_size())
	}

	fn total_entries(&self) -> u64 {
		(1u64 << self.index_bits()) + MAX_AHEAD as u64
	}

	fn value_size(&self) -> u16 {
		self.entry_size() - KEY_LEN as u16 - 2
	}

	fn file_size(&self) -> u64 {
		self.total_entries() * self.entry_size() as u64 + META_SIZE as u64
	}

	fn as_u32(&self) -> u32 {
		self.0
	}
}

impl std::fmt::Display for BucketId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "c{:02}_{}_{}", self.col(), self.index_bits(), self.entry_size())
	}
}

struct Bucket {
	id: BucketId,
	map: memmap::MmapMut,
	entries: std::sync::atomic::AtomicU64,
}

impl Bucket {
	fn open_existing(path: &std::path::Path, id: BucketId) -> Result<Option<Bucket>> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = match std::fs::OpenOptions::new().read(true).write(true).open(path) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
			Ok(file) => file,
		};

		log::debug!(target: "parity-db", "Opened existing bucket {}", id);
		Ok(Some(Self::from_file(file, id)?))
	}

	fn create_new(path: &std::path::Path, id: BucketId) -> Result<Bucket> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = std::fs::OpenOptions::new().write(true).read(true).create_new(true).open(path)?;
		log::debug!(target: "parity-db", "Created new bucket {}", id);
		Self::from_file(file, id)
	}

	fn from_file(file: std::fs::File, id: BucketId) -> Result<Bucket> {
		//TODO: check for potential overflows on 32-bit platforms
		file.set_len(id.file_size())?;
		let map = unsafe { memmap::MmapMut::map_mut(&file)? };
		Ok(Bucket {
			id,
			map,
			entries: std::sync::atomic::AtomicU64::new(0),
		})
	}

	fn key_at(&self, index: u64) -> Key {
		let offset = META_SIZE + index as isize * self.id.entry_size() as isize;
		let key = unsafe { *((self.map.as_ptr().offset(offset)) as *const Key) };
		key
	}

	fn value_at(&self, index: u64) -> &[u8] {
		unsafe {
			let mut ptr = self.map.as_ptr();
			let offset = META_SIZE + index as isize * self.id.entry_size() as isize + KEY_LEN as isize;
			ptr = ptr.offset(offset);
			let len = u16::from_le(std::ptr::read(ptr as *const u16));
			ptr = ptr.offset(2);
			std::slice::from_raw_parts(ptr, len as usize)
		}
	}

	fn key_index(&self, key: &Key) -> u64 {
		//Interpret first 64 bits of the key as LE integer
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		key >> (64 - self.id.index_bits())
	}

	fn get(&self, key: &Key, log: &Log) -> Option<Value> {
		let mut index = self.key_index(key);
		for _ in 0 .. MAX_AHEAD {
			let target_key = self.logged_key(index, log);
			if target_key == EMPTY {
				return None;
			}
			match target_key.cmp(key) {
				std::cmp::Ordering::Less => (),
				std::cmp::Ordering::Greater => {
					return None;
				},
				std::cmp::Ordering::Equal => return Some(self.logged_value(index, log).to_vec()),
			}
			index += 1;
		}
		None
	}

	fn planned_value<'a>(&'a self, index: u64, log: &'a LogWriter) -> &'a[u8] {
		log.overlay_at(self.id, index)
			.map(|buf| &buf[KEY_LEN+2..])
			.unwrap_or_else(|| self.value_at(index))

	}

	fn planned_key(&self, index: u64, log: &LogWriter) -> Key {
		log.overlay_at(self.id, index)
			.map(|buf| buf[..KEY_LEN].try_into().unwrap())
			.unwrap_or_else(|| self.key_at(index))
	}

	fn logged_value<'a>(&'a self, index: u64, log: &'a Log) -> &'a[u8] {
		log.overlay_at(self.id, index)
			.map(|buf| &buf[KEY_LEN+2..])
			.unwrap_or_else(|| self.value_at(index))

	}

	fn logged_key(&self, index: u64, log: &Log) -> Key {
		log.overlay_at(self.id, index)
			.map(|buf| buf[..KEY_LEN].try_into().unwrap())
			.unwrap_or_else(|| self.key_at(index))
	}

	fn write_plan(&self, key: &Key, value: Option<&[u8]>, log: &mut LogWriter, overwrite: bool) -> Result<PlanOutcome> {
		let plan_insertion = |key: &Key, data: &[u8]| -> Vec<u8>  {
			let mut buf =Vec::with_capacity(KEY_LEN + 2 + data.len());
			buf.extend_from_slice(key);
			if *key != EMPTY {
				let size: u16 = data.len() as u16;
				buf.extend_from_slice(&size.to_le_bytes());
				buf.extend_from_slice(data);
			}
			buf
		};

		match value {
			Some(v) => {
				// Find a slot to insert at
				let start_index = self.key_index(key);
				for index in start_index .. start_index + MAX_AHEAD as u64 {
					let target_key = self.planned_key(index, log);
					if target_key == EMPTY {
						log::trace!(
							target: "parity-db",
							"{}: Inserting into empty slot {}: {}",
							self.id,
							index,
							hex(key),
						);
						// Insert new here
						log.insert(self.id, index, plan_insertion(key, v))?;
						self.entries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
						return Ok(PlanOutcome::Written);
					}
					match target_key.cmp(key) {
						std::cmp::Ordering::Less => continue,
						std::cmp::Ordering::Greater => {
							// Evict and insert here
							// Scan ahead for empty space but not too far from start_index.
							// This guarantess each entry is no further than MAX_AHEAD from its
							// designated location.
							let mut relocation_end = index + 1;
							while relocation_end < start_index + MAX_AHEAD as u64
								&& self.planned_key(relocation_end, log) != EMPTY
							{
								relocation_end += 1
							}
							if relocation_end == start_index + MAX_AHEAD as u64 {
								// No empty space ahead.
								log::trace!(
									target: "parity-db",
									"{}: No empty spot for eviction, designated {}, inserting {}, checked up to {}: {}",
									self.id,
									start_index,
									index,
									relocation_end,
									hex(key),
								);
								return Ok(PlanOutcome::NeedRebalance);
							}

							log::trace!(
								target: "parity-db",
								"{}: Inserting into occupied slot {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							// Relocate following entries walking backwards
							for i in (index..relocation_end).rev() {
								// TODO: 0-copy here
								let pk = self.planned_key(i, log);
								let insertion = plan_insertion(&pk, &self.planned_value(i, log));
								log.insert(self.id, i + 1, insertion)?;
								log::trace!(
									target: "parity-db",
									"{}: Relocating occupied slot {}->{}, designated {}: {}",
									self.id,
									i,
									i + 1,
									self.key_index(&pk),
									hex(&pk),
								);
							}

							log.insert(self.id, index, plan_insertion(key, v))?;
							self.entries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
							return Ok(PlanOutcome::Written);
						}
						std::cmp::Ordering::Equal if overwrite => {
							// Replace here
							log::trace!(
								target: "parity-db",
								"{}: Replacing {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							log.insert(self.id, index, plan_insertion(key, v))?;
							return Ok(PlanOutcome::Written);
						}
						std::cmp::Ordering::Equal => {
							return Ok(PlanOutcome::Skipped);
						}
					}
				}
				// No empty space
				log::trace!(
					target: "parity-db",
					"{}: No empty spot for insertion, designated {}, checked up to {}: {}",
					self.id,
					start_index,
					start_index + MAX_AHEAD as u64,
					hex(key),
				);
				return Ok(PlanOutcome::NeedRebalance);
			},
			None =>  {
				// Delete
				let start_index = self.key_index(key);
				for index in start_index .. start_index + MAX_AHEAD as u64 {
					let target_key = self.planned_key(index, log);
					if target_key == EMPTY {
						// Does not exist
						return Ok(PlanOutcome::Skipped);
					}
					match target_key.cmp(key) {
						std::cmp::Ordering::Less => continue,
						std::cmp::Ordering::Greater => return Ok(PlanOutcome::Skipped),
						std::cmp::Ordering::Equal => {
							log::trace!(
								target: "parity-db",
								"{}: Deleting {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							let max_entry = self.id.total_entries();
							// Deleting here
							log.insert(self.id, index, plan_insertion(&EMPTY, &[]))?;
							let mut relocation_end = index + 1;
							while relocation_end < max_entry {
								let target_key = self.planned_key(relocation_end, log);
								if target_key == EMPTY || self.key_index(&target_key) >= relocation_end {
									break;
								}
								relocation_end += 1;
							}
							self.entries.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
							// Relocate following entries
							for i in (index + 1)..relocation_end {
								let insertion = {
									let k = self.planned_key(i, log);
									let v = self.planned_value(i, log);
									log::trace!(
										target: "parity-db",
										"{}: Relocating freed slot {}->{}, designated {}: {}",
										self.id,
										i,
										i - 1,
										self.key_index(&k),
										hex(&k),
									);
									plan_insertion(&k, v)
								};
								log.insert(self.id, i - 1, insertion)?;
							}
							log.insert(self.id, relocation_end - 1, plan_insertion(&EMPTY, &[]))?;
							return Ok(PlanOutcome::Written);
						}
					}
				}
				return Ok(PlanOutcome::Skipped);
			}
		}
	}

	fn enact_plan(&mut self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut offset = META_SIZE as isize + (index * self.id.entry_size() as u64) as isize;
		let mut key = Key::default();
		log.read(&mut key)?;
		unsafe {
			std::ptr::copy_nonoverlapping(key.as_ptr(), self.map.as_mut_ptr().offset(offset), KEY_LEN);
			if key != EMPTY {
				offset += KEY_LEN as isize;
				let mut s = [0u8; 2];
				log.read(&mut s)?;
				let size = u16::from_le_bytes(s);
				std::ptr::copy_nonoverlapping(s.as_ptr(), self.map.as_mut_ptr().offset(offset), 2);
				offset += 2;
				log.read(&mut self.map[offset as usize..(offset as usize + size as usize)])?;
			}
		}
		log::trace!(target: "parity-db", "{}: Enacted {}", self.id, index);
		Ok(())
	}

	fn rebalance_from(&mut self, source: &Bucket, start_index: u64, log: &mut LogWriter) -> Result<u64> {
		let mut source_index = start_index;
		let mut count = 0;
		log::trace!(target: "parity-db", "{}: Starting rebalance at {}", self.id, source_index);
		while source_index < source.id.total_entries() && count < MAX_REBALANCE_BATCH {
			let source_key = source.planned_key(source_index, log);
			if source_key != EMPTY {
				// TODO: remove allocation here
				// Alternatively make sure record that triggered rebalance is flushed before doing rebalance
				let v = source.planned_value(source_index, log).to_vec();
				self.write_plan(&source_key, Some(&v), log, false)?;
				count += 1;
			}
			source_index += 1;
		}
		log::trace!(target: "parity-db", "{}: End rebalance batch {} ({})", self.id, source_index, count);
		Ok(source_index)
	}

	fn entries(&self) -> u64 {
		self.entries.load(std::sync::atomic::Ordering::Relaxed)
	}

	fn drop(self, path: &std::path::Path) -> Result<()> {
		let mut path: std::path::PathBuf = path.into();
		path.push(self.id.file_name());
		std::mem::drop(self.map);
		std::fs::remove_file(path.as_path())?;
		log::debug!(target: "parity-db", "{}: Dropped bucket", self.id);
		Ok(())
	}
}

struct Shard {
	bucket: Bucket,
	rebalancing: VecDeque<Bucket>,
	rebalance_progress: u64,
}

struct Column {
	// Ordered by value size.
	shards: Vec<Shard>,
	blobs: HashMap<Key, Value>,
	path: std::path::PathBuf,
	histogram: std::collections::BTreeMap<u64, u64>,
}

impl Column {
	fn get(&self, key: &Key, log: &Log) -> Option<Value> {
		for s in &self.shards {
			if let Some(v) = s.bucket.get(key, log) {
				return Some(v);
			}
			for r in &s.rebalancing {
				if let Some(v) = r.get(key, log) {
					return Some(v);
				}
			}
		}
		self.blobs.get(key).cloned()
	}

	fn open(index: ColId, path: &std::path::Path) -> Result<Column> {
		Ok(Column {
			shards: vec![
				Self::open_shard(path, index, 128)?,
				Self::open_shard(path, index, 256)?,
				Self::open_shard(path, index, 512)?,
				Self::open_shard(path, index, 1024)?,
				Self::open_shard(path, index, 4096)?,
			],
			blobs: HashMap::new(),
			path: path.into(),
			histogram: Default::default(),
		})
	}

	fn open_shard(path: &std::path::Path, col: ColId, entry_size: u16) -> Result<Shard> {
		let mut rebalancing = VecDeque::new();
		let mut top = None;
		for bits in (10 .. 65).rev() {
			let id = BucketId::new(col, entry_size, bits);
			if let Some(bucket) = Bucket::open_existing(path, id)? {
				if top.is_none() {
					top = Some(bucket);
				} else {
					rebalancing.push_front(bucket);
				}
			}
		}
		let bucket = match top {
			Some(bucket) => bucket,
			None => Bucket::create_new(path, BucketId::new(col, entry_size,  10))?,
		};
		Ok(Shard {
			bucket,
			rebalancing,
			rebalance_progress: 0,
		})
	}

	fn write_plan(&mut self, key: &Key, value: Option<Value>, log: &mut LogWriter) -> Result<()> {
		match value {
			Some(value) => {
				*self.histogram.entry(value.len() as u64).or_default() += 1;
				// TODO: delete from other shards?
				let target_shard = self.shards.iter()
					.position(|s|value.len() <= s.bucket.id.value_size() as usize);
				match target_shard {
					Some(target_shard) => {
						for i in 0 .. self.shards.len() {
							if i == target_shard {
								let s = &mut self.shards[i];
								match s.bucket.write_plan(key, Some(&value), log, true)? {
									PlanOutcome::NeedRebalance => {
										log::info!(
											target: "parity-db",
											"Started rebalance {} at {}/{} full",
											s.bucket.id,
											s.bucket.entries(),
											s.bucket.id.total_entries(),
										);
										// Start rebalance
										let new_bucket_id = BucketId::new(
											s.bucket.id.col(),
											s.bucket.id.entry_size(),
											s.bucket.id.index_bits() + 1
										);
										let new_bucket = Bucket::create_new(self.path.as_path(), new_bucket_id)?;
										let old_bucket = std::mem::replace(&mut s.bucket, new_bucket);
										s.rebalancing.push_back(old_bucket);
										s.bucket.write_plan(key, Some(&value), log, true)?;
									}
									_ => {
									}
								}
							} else {
								match self.shards[i].bucket.write_plan(key, None, log, true)? {
									PlanOutcome::Written => {
										log::debug!(
											target: "parity-db",
											"Replaced to a different shard {}->{}: {}",
											self.shards[i].bucket.id,
											self.shards[target_shard].bucket.id,
											hex(key),
										);
									}
									_ => {},
								}
							}
						}
					}
					None => {
						log::trace!(
							target: "parity-db",
							"Inserted blob {} ({} bytes)",
							hex(key),
							value.len(),
						);
						self.blobs.insert(*key, value);
					}
				}
			},
			None => {
				log::trace!(
					target: "parity-db",
					"Removed blob {}",
					hex(key),
				);
				if self.blobs.remove(key).is_some() {
						return Ok(());
				}
				// Delete from all shards
				for s in self.shards.iter_mut() {
					match s.bucket.write_plan(key, None, log, true)? {
						PlanOutcome::Written => {
							break;
						}
						PlanOutcome::Skipped {} | PlanOutcome::NeedRebalance => {},
					}
				}
			}
		}
		Ok(())
	}

	fn enact_plan(&mut self, id: BucketId, index: u64, log: &mut LogReader) -> Result<()> {
		// TODO: handle the case when bucket file does not exist
		let shard = self.shards.iter_mut().find(|s| s.bucket.id.entry_size() == id.entry_size());
		let shard = match shard {
			Some(s) => s,
			None => {
				log::warn!(
					target: "parity-db",
					"Missing bucket {}",
					id,
				);
				return Err(Error::Corruption("Missing bucket".into()));
			}
		};
		if shard.bucket.id == id {
			shard.bucket.enact_plan(index, log)?;
		} else {
			if let Some(bucket) = shard.rebalancing.iter_mut().find(|r|r.id == id) {
				bucket.enact_plan(index, log)?;
			}
			else {
				log::warn!(
					target: "parity-db",
					"Missing bucket {}",
					id,
				);
				return Err(Error::Corruption("Missing bucket".into()));
			}
		}
		Ok(())
	}

	fn rebalance(&mut self, log: &mut Log) -> Result<RebalanceProgress> {
		for s in self.shards.iter_mut() {
			if let Some(b) = s.rebalancing.front_mut() {
				if s.rebalance_progress != b.id.total_entries() {
					let mut log = log.begin_record()?;
					log::trace!(target: "parity-db", "{}: Start rebalance record {}", s.bucket.id, log.record_id);
					s.rebalance_progress = s.bucket.rebalance_from(&b, s.rebalance_progress, &mut log)?;
					if s.rebalance_progress == b.id.total_entries() {
						log::info!(target: "parity-db", "Completed rebalance {}", s.bucket.id);
						log.drop_bucket(b.id)?;
					}
					log::trace!(target: "parity-db", "{}: End rebalance record {}", s.bucket.id, log.record_id);
					log.end_record()?;
					return Ok(RebalanceProgress::InProgress((s.rebalance_progress, b.id.total_entries())))
				}
			}
		}
		Ok(RebalanceProgress::Inactive)
	}

	fn drop_bucket(&mut self, id: BucketId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		for s in self.shards.iter_mut() {
			if s.rebalancing.front_mut().map_or(false, |b| b.id == id) {
				let bucket = s.rebalancing.pop_front();
				s.rebalance_progress = 0;
				bucket.unwrap().drop(self.path.as_path())?;
			}
		}
		Ok(())
	}
}

struct InsertAction {
	bucket: BucketId,
	index: u64,
}

enum LogAction {
	BeginRecord(u64),
	Insert(InsertAction),
	DropBucket(BucketId),
	EndRecord, // TODO: crc32
}

struct LogReader<'a> {
	file: &'a mut std::io::BufReader<std::fs::File>,
	overlays: &'a mut HashMap<BucketId, LogOverlay>,
	record_id: u64,
}

impl<'a> LogReader<'a> {
	fn new(
		file: &'a mut std::io::BufReader<std::fs::File>,
		overlays: &'a mut HashMap<BucketId, LogOverlay>,
		record_id: u64,
	) -> LogReader<'a> {
		LogReader {
			file,
			overlays,
			record_id,
		}
	}

	fn next(&mut self) -> Result<LogAction> {
		let mut buf = [0u8; 8];
		self.file.read_exact(&mut buf[0..1])?;
		match buf[0] {
			1 =>  {
				self.file.read_exact(&mut buf[0..8])?;
				let record_id = u64::from_le_bytes(buf);
				self.record_id = record_id;
				Ok(LogAction::BeginRecord(record_id))
			},
			2 => {
				self.file.read_exact(&mut buf[0..4])?;
				let bucket = BucketId(u32::from_le_bytes(buf[0..4].try_into().unwrap()));
				self.file.read_exact(&mut buf[0..8])?;
				let index = u64::from_le_bytes(buf);
				if let Some(ref mut overlay) = self.overlays.get_mut(&bucket) {
					match overlay.map.entry(index) {
						std::collections::hash_map::Entry::Occupied(e) => {
							if e.get().0 == self.record_id {
								e.remove_entry();
							}
						}
						_ => {},
					}
				}
				Ok(LogAction::Insert(InsertAction { bucket, index }))
			},
			3 => {
				Ok(LogAction::EndRecord)
			},
			4 => {
				self.file.read_exact(&mut buf[0..4])?;
				let bucket = BucketId(u32::from_le_bytes(buf[0..4].try_into().unwrap()));
				Ok(LogAction::DropBucket(bucket))
			}
			_ => Err(Error::Corruption("Bad log entry type".into()))
		}
	}

	fn read(&mut self, buf: &mut [u8]) -> Result<()> {
		Ok(self.file.read_exact(buf)?)
	}
}

struct LogWriter<'a> {
	file: &'a mut std::io::BufWriter<std::fs::File>,
	overlays: &'a mut HashMap<BucketId, LogOverlay>,
	record_id: u64,
}

impl<'a> LogWriter<'a> {
	fn new(
		file: &'a mut std::io::BufWriter<std::fs::File>,
		overlays: &'a mut HashMap<BucketId, LogOverlay>,
		record_id: u64,
	) -> LogWriter<'a> {
		LogWriter {
			file,
			overlays,
			record_id,
		}
	}

	fn begin_record(&mut self, id: u64) -> Result<()> {
		self.file.write(&1u8.to_le_bytes())?;
		self.file.write(&id.to_le_bytes())?;
		Ok(())
	}

	fn insert(&mut self, bucket: BucketId, index: u64, data: Vec<u8>) -> Result<()> {
		self.file.write(&2u8.to_le_bytes().as_ref())?;
		self.file.write(&bucket.as_u32().to_le_bytes())?;
		self.file.write(&index.to_le_bytes())?;
		self.file.write(&data)?;
		self.overlays.entry(bucket).or_default().map.insert(index, (self.record_id, data));
		Ok(())
	}

	fn end_record(&mut self) -> Result<()> {
		self.file.write(&3u8.to_le_bytes())?;
		self.file.flush()?;
		Ok(())
	}

	fn drop_bucket(&mut self, id: BucketId) -> Result<()> {
		self.file.write(&4u8.to_le_bytes())?;
		self.file.write(&id.as_u32().to_le_bytes())?;
		Ok(())
	}

	fn overlay_at(&self, bucket: BucketId, index: u64) -> Option<&[u8]> {
		self.overlays.get(&bucket).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
	}
}

enum PlanOutcome {
	Written,
	NeedRebalance,
	Skipped,
}

#[derive(Default)]
struct LogOverlay {
	map: HashMap<u64, (u64, Vec<u8>)>, // index -> (record_id, entry)
}

struct Log {
	//records: Vec<LogRecord>,
	overlays: HashMap<BucketId, LogOverlay>,
	appending: std::io::BufWriter<std::fs::File>,
	flushing: std::io::BufReader<std::fs::File>,
	record_id: u64,
	appending_empty: bool,
	dirty: bool,
}

impl Log {
	fn open(path: &std::path::Path) -> Result<Log> {
		let mut path = std::path::PathBuf::from(path);
		path.push("log0");
		let (file0, id0) = Self::open_or_create_log_file(path.as_path())?;
		path.pop();
		path.push("log1");
		let (file1, id1) = Self::open_or_create_log_file(path.as_path())?;
		let record_id = if id0.is_some() || id1.is_some() { 0 } else { 1 };
		log::debug!(target: "parity-db", "Opened log ({:?} {:?})", id0, id1);
		let (flushing, mut appending) = match (id0, id1) {
			(None, None) => (file0, file1),
			(Some(_), None) => (file0, file1),
			(Some(id0), Some(id1)) if id0 < id1 => (file0, file1),
			(Some(id0), Some(id1)) if id0 > id1 => (file1, file0),
			_ => panic!(),
		};
		appending.seek(std::io::SeekFrom::End(0))?;

		let overlays = HashMap::new();

		Ok(Log {
			overlays,
			appending: std::io::BufWriter::new(appending),
			flushing: std::io::BufReader::new(flushing),
			record_id,
			appending_empty: false,
			dirty: true,
		})
	}

	fn open_or_create_log_file(path: &std::path::Path) -> Result<(std::fs::File, Option<u64>)> {
		let mut file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(path)?;
		if file.metadata()?.len() == 0 {
			return Ok((file, None));
		}
		// read first record id
		let mut buf = [0; 8];
		file.read_exact(&mut buf)?;
		file.seek(std::io::SeekFrom::Start(0))?;
		let id = u64::from_le_bytes(buf);
		Ok((file, Some(id)))
	}

	fn begin_record<'a>(&'a mut self) -> Result<LogWriter<'a>> {
		if self.record_id == 0 {
			panic!("Still flushing");
		}
		let mut writer = LogWriter::new(
			&mut self.appending,
			&mut self.overlays,
			self.record_id
		);
		writer.begin_record(self.record_id)?;
		self.record_id += 1;
		self.appending_empty = false;
		self.dirty = true;
		Ok(writer)
	}

	fn flush_one<'a>(&'a mut self) -> Result<Option<LogReader<'a>>> {
		if !self.dirty {
			return Ok(None);
		}
		let mut reader = LogReader::new(
			&mut self.flushing,
			&mut self.overlays,
			0
		);
		match reader.next() {
			Ok(LogAction::BeginRecord(id)) => {
				if self.record_id <= id {
					self.record_id = id + 1;
				}
				let reader = LogReader::new(
					&mut self.flushing,
					&mut self.overlays,
					id
				);
				return Ok(Some(reader));
			}
			Ok(_) => return Err(Error::Corruption("Bad log record structure".into())),
			Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
				if self.appending_empty {
					self.dirty = false;
					return Ok(None);
				}
				log::debug!(
					target: "parity-db",
					"Switching log files",
				);
				self.appending_empty = true;
				std::mem::drop(reader);
				// Switch files
				self.appending.flush()?;
				std::mem::swap(self.appending.get_mut(), self.flushing.get_mut());
				self.appending.get_mut().set_len(0)?;
				self.appending.seek(std::io::SeekFrom::Start(0))?;
				self.flushing.seek(std::io::SeekFrom::Start(0))?;
				//self.flushing.get_mut().sync_data()?;
				return self.flush_one();
			}
			Err(e) => return Err(e),
		};
	}

	fn overlay_at(&self, bucket: BucketId, index: u64) -> Option<&[u8]> {
		self.overlays.get(&bucket).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
	}
}

struct DbInner {
	columns: RwLock<Vec<Column>>,
	_path: std::path::PathBuf,
	shutdown: std::sync::atomic::AtomicBool,
	rebalancing: Mutex<bool>,
	rebalace_condvar: Condvar,
	log: RwLock<Log>,
}


impl DbInner {
	pub fn open(path: &std::path::Path, num_columns: u8) -> Result<DbInner> {
		std::fs::create_dir_all(path)?;
		let mut columns = Vec::with_capacity(num_columns as usize);
		for c in 0 .. num_columns {
			columns.push(Column::open(c, path)?);
		}
		Ok(DbInner {
			columns: RwLock::new(columns),
			_path: path.into(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			rebalancing: Mutex::new(false),
			rebalace_condvar: Condvar::new(),
			log: RwLock::new(Log::open(path)?),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Option<Value> {
		let columns = self.columns.read();
		let log = self.log.read();
		columns[col as usize].get(&hash(key), &log)
	}

	fn signal_worker(&self) {
		let mut active = self.rebalancing.lock();
		*active = true;
		log::trace!("Starting rebalance");
		self.rebalace_condvar.notify_one();
	}

	fn commit(&self, tx: DBTransaction) -> Result<()> {
		// TODO: take read lock on columns for writing log.
		{
			let mut columns = self.columns.write();
			let mut log = self.log.write();
			let mut log = log.begin_record()?;
			log::debug!(
				target: "parity-db",
				"Starting commit {}, {} ops",
				log.record_id,
				tx.ops.len(),
			);
			for op in tx.ops {
				let (c, key, value) = match op {
					DBOp::Insert { col: c, key, value } => {
						(c, hash(&key), Some(value))
					}
					DBOp::Delete { col: c, key } => {
						(c, hash(&key), None)
					}
				};

				columns[c as usize].write_plan(&key, value, &mut log)?;
			}
			log.end_record()?;
			log::debug!(
				target: "parity-db",
				"Completed commit {}",
				log.record_id,
			);
		}
		self.signal_worker();
		Ok(())
	}

	fn enact_logs(&self) -> Result<bool> {
		let mut columns = self.columns.write();
		let mut log = self.log.write();
		if let Some(mut reader) = log.flush_one()? {
			log::debug!(
				target: "parity-db",
				"Enacting log {}",
				reader.record_id,
			);
			loop {
				match reader.next()? {
					LogAction::BeginRecord(_) => {
						return Err(Error::Corruption("Bad log record".into()));
					},
					LogAction::EndRecord => {
						break;
					},
					LogAction::Insert(insertion) => {
						columns[insertion.bucket.col() as usize].enact_plan(insertion.bucket, insertion.index, &mut reader)?;

					},
					LogAction::DropBucket(id) => {
						columns[id.col() as usize].drop_bucket(id)?;

					}
				}
			}
			log::debug!(
				target: "parity-db",
				"Finished log {}",
				reader.record_id,
			);
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn flush_all_logs(&self) -> Result<()> {
		while self.enact_logs()? { };
		Ok(())
	}

	fn process_rebalance(&self) -> Result<bool> {
		// Process any pending rebalances
		let mut columns = self.columns.write();
		let mut log = self.log.write();
		for c in columns.iter_mut() {
			match c.rebalance(&mut log)? {
				RebalanceProgress::InProgress((p, t)) => {
					log::debug!(
						target: "parity-db",
						"Continue rebalance {}/{}",
						p,
						t,
					);
					return Ok(true);
				},
				RebalanceProgress::Inactive => {},
			}
		}
		return Ok(false);
	}

	fn shutdown(&self) {
		self.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
		self.signal_worker();
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	balance_thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl Db {
	pub fn open(path: &std::path::Path, columns: u8) -> Result<Db> {
		let db = Arc::new(DbInner::open(path, columns)?);
		let worker_db = db.clone();
		let worker = std::thread::spawn(move ||
			Self::db_worker(worker_db).map_err(|e| { log::warn!("DB ERROR: {:?}", e); e })
		);
		db.flush_all_logs()?;
		Ok(Db {
			inner: db,
			balance_thread: Some(worker),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		Ok(self.inner.get(col, key))
	}

	pub fn commit(&self, tx: DBTransaction) -> Result<()> {
		self.inner.commit(tx)
	}

	pub fn num_columns(&self) -> u8 {
		self.inner.columns.read().len() as u8
	}

	fn db_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut active = db.rebalancing.lock();
				if !*active {
					db.rebalace_condvar.wait(&mut active);
				}
				*active = false;
			}

			// Flush log
			more_work = db.enact_logs()?;
			more_work = more_work || db.process_rebalance()?;
		}

		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.balance_thread.take().map(|t| t.join());
		for (i, c) in self.inner.columns.read().iter().enumerate() {
			println!("HISTOGRAM Column {}, {} blobs", i, c.blobs.len());
			for (s, count) in &c.histogram {
				println!("{}:{}", s, count);
			}
		}
	}
}

