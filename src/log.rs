// Copyright 2015-2020 Parity Technologies (UK) Ltd. This file is part of Parity.

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

use crate::{
	column::ColId,
	error::{Error, Result},
	index::{Chunk as IndexChunk, TableId as IndexTableId, ENTRY_BYTES},
	options::Options,
	table::TableId as ValueTableId,
};
use parking_lot::{Condvar, MappedRwLockWriteGuard, Mutex, RwLock, RwLockWriteGuard};
use std::{
	collections::{HashMap, VecDeque},
	convert::TryInto,
	io::{Read, Seek, Write},
	sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

const MAX_LOG_POOL_SIZE: usize = 16;
const BEGIN_RECORD: u8 = 1;
const INSERT_INDEX: u8 = 2;
const INSERT_VALUE: u8 = 3;
const END_RECORD: u8 = 4;
const DROP_TABLE: u8 = 5;

pub struct InsertIndexAction {
	pub table: IndexTableId,
	pub index: u64,
}

pub struct InsertValueAction {
	pub table: ValueTableId,
	pub index: u64,
}

pub enum LogAction {
	BeginRecord,
	InsertIndex(InsertIndexAction),
	InsertValue(InsertValueAction),
	DropTable(IndexTableId),
	EndRecord,
}

pub trait LogQuery {
	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R>;
	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool;
}

#[derive(Default)]
pub struct LogOverlays {
	index: HashMap<IndexTableId, IndexLogOverlay>,
	value: HashMap<ValueTableId, ValueLogOverlay>,
	last_record_id: HashMap<ColId, u64>,
}

impl LogOverlays {
	pub fn last_record_id(&self, col: ColId) -> u64 {
		self.last_record_id.get(&col).cloned().unwrap_or(u64::MAX)
	}
}

impl LogQuery for RwLock<LogOverlays> {
	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		self.read().with_index(table, index, f)
	}

	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool {
		self.read().value(table, index, dest)
	}
}

impl LogQuery for LogOverlays {
	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		self.index
			.get(&table)
			.and_then(|o| o.map.get(&index).map(|(_id, _mask, data)| f(data)))
	}

	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool {
		let s = self;
		if let Some(d) = s.value.get(&table).and_then(|o| o.map.get(&index).map(|(_id, data)| data))
		{
			let len = dest.len().min(d.len());
			dest[0..len].copy_from_slice(&d[0..len]);
			true
		} else {
			false
		}
	}
}

#[derive(Default)]
pub struct Cleared {
	index: Vec<(IndexTableId, u64)>,
	values: Vec<(ValueTableId, u64)>,
}

pub struct LogReader<'a> {
	file: MappedRwLockWriteGuard<'a, std::io::BufReader<std::fs::File>>,
	record_id: u64,
	read_bytes: u64,
	crc32: crc32fast::Hasher,
	validate: bool,
	cleared: Cleared,
}

impl<'a> LogReader<'a> {
	pub fn record_id(&self) -> u64 {
		self.record_id
	}

	fn new(
		file: MappedRwLockWriteGuard<'a, std::io::BufReader<std::fs::File>>,
		validate: bool,
	) -> LogReader<'a> {
		LogReader {
			cleared: Default::default(),
			file,
			record_id: 0,
			read_bytes: 0,
			crc32: crc32fast::Hasher::new(),
			validate,
		}
	}

	pub fn reset(&mut self) -> Result<()> {
		self.cleared = Default::default();
		self.file.seek(std::io::SeekFrom::Current(-(self.read_bytes as i64)))?;
		self.read_bytes = 0;
		self.record_id = 0;
		self.crc32 = crc32fast::Hasher::new();
		Ok(())
	}

	pub fn next(&mut self) -> Result<LogAction> {
		let mut read_buf = |size, buf: &mut [u8; 8]| -> Result<()> {
			self.file.read_exact(&mut buf[0..size])?;
			self.read_bytes += size as u64;
			if self.validate {
				self.crc32.update(&buf[0..size]);
			}
			Ok(())
		};

		let mut buf = [0u8; 8];
		read_buf(1, &mut buf)?;
		match buf[0] {
			BEGIN_RECORD => {
				read_buf(8, &mut buf)?;
				let record_id = u64::from_le_bytes(buf);
				self.record_id = record_id;
				Ok(LogAction::BeginRecord)
			},
			INSERT_INDEX => {
				read_buf(2, &mut buf)?;
				let table =
					IndexTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				read_buf(8, &mut buf)?;
				let index = u64::from_le_bytes(buf);
				self.cleared.index.push((table, index));
				Ok(LogAction::InsertIndex(InsertIndexAction { table, index }))
			},
			INSERT_VALUE => {
				read_buf(2, &mut buf)?;
				let table =
					ValueTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				read_buf(8, &mut buf)?;
				let index = u64::from_le_bytes(buf);
				self.cleared.values.push((table, index));
				Ok(LogAction::InsertValue(InsertValueAction { table, index }))
			},
			END_RECORD => {
				self.file.read_exact(&mut buf[0..4])?;
				self.read_bytes += 4;
				if self.validate {
					let checksum = u32::from_le_bytes(buf[0..4].try_into().unwrap());
					let expected = std::mem::take(&mut self.crc32).finalize();
					log::trace!(target: "parity-db",
						"Read end of record, checksum={:#x}, expected={:#x}",
						checksum,
						expected,
					);
					if checksum != expected {
						return Err(Error::Corruption("Log record CRC-32 mismatch".into()))
					}
				} else {
					log::trace!(target: "parity-db", "Read end of record");
				}
				Ok(LogAction::EndRecord)
			},
			DROP_TABLE => {
				read_buf(2, &mut buf)?;
				let table =
					IndexTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				Ok(LogAction::DropTable(table))
			},
			_ => Err(Error::Corruption("Bad log entry type".into())),
		}
	}

	pub fn read(&mut self, buf: &mut [u8]) -> Result<()> {
		self.file.read_exact(buf)?;
		self.read_bytes += buf.len() as u64;
		if self.validate {
			self.crc32.update(buf);
		}
		Ok(())
	}

	pub fn drain(self) -> Cleared {
		self.cleared
	}

	pub fn read_bytes(&self) -> u64 {
		self.read_bytes
	}
}

pub struct LogChange {
	local_index: HashMap<IndexTableId, IndexLogOverlay>,
	local_values: HashMap<ValueTableId, ValueLogOverlay>,
	record_id: u64,
	dropped_tables: Vec<IndexTableId>,
}

impl LogChange {
	fn new(record_id: u64) -> LogChange {
		LogChange {
			local_index: Default::default(),
			local_values: Default::default(),
			dropped_tables: Default::default(),
			record_id,
		}
	}

	pub fn local_values_changes(&self, id: ValueTableId) -> Option<&ValueLogOverlay> {
		self.local_values.get(&id)
	}

	fn flush_to_file(self, file: &mut std::io::BufWriter<std::fs::File>) -> Result<FlushedLog> {
		let mut crc32 = crc32fast::Hasher::new();
		let mut bytes: u64 = 0;

		let mut write = |buf: &[u8]| -> Result<()> {
			file.write_all(buf)?;
			crc32.update(buf);
			bytes += buf.len() as u64;
			Ok(())
		};

		write(&BEGIN_RECORD.to_le_bytes())?;
		write(&self.record_id.to_le_bytes())?;

		for (id, overlay) in self.local_index.iter() {
			for (index, (_, modified_entries_mask, chunk)) in overlay.map.iter() {
				write(INSERT_INDEX.to_le_bytes().as_ref())?;
				write(&id.as_u16().to_le_bytes())?;
				write(&index.to_le_bytes())?;
				write(&modified_entries_mask.to_le_bytes())?;
				let mut mask = *modified_entries_mask;
				while mask != 0 {
					let i = mask.trailing_zeros();
					mask &= !(1 << i);
					write(&chunk[i as usize * ENTRY_BYTES..(i as usize + 1) * ENTRY_BYTES])?;
				}
			}
		}
		for (id, overlay) in self.local_values.iter() {
			for (index, (_, value)) in overlay.map.iter() {
				write(INSERT_VALUE.to_le_bytes().as_ref())?;
				write(&id.as_u16().to_le_bytes())?;
				write(&index.to_le_bytes())?;
				write(value)?;
			}
		}
		for id in self.dropped_tables.iter() {
			log::debug!(target: "parity-db", "Finalizing drop {}", id);
			write(DROP_TABLE.to_le_bytes().as_ref())?;
			write(&id.as_u16().to_le_bytes())?;
		}
		write(&END_RECORD.to_le_bytes())?;
		let checksum: u32 = crc32.finalize();
		file.write_all(&checksum.to_le_bytes())?;
		bytes += 4;
		file.flush()?;
		Ok(FlushedLog { index: self.local_index, values: self.local_values, bytes })
	}
}

struct FlushedLog {
	index: HashMap<IndexTableId, IndexLogOverlay>,
	values: HashMap<ValueTableId, ValueLogOverlay>,
	bytes: u64,
}

pub struct LogWriter<'a> {
	overlays: &'a RwLock<LogOverlays>,
	log: LogChange,
}

impl<'a> LogWriter<'a> {
	pub fn new(overlays: &'a RwLock<LogOverlays>, record_id: u64) -> LogWriter<'a> {
		LogWriter { overlays, log: LogChange::new(record_id) }
	}

	pub fn record_id(&self) -> u64 {
		self.log.record_id
	}

	pub fn insert_index(&mut self, table: IndexTableId, index: u64, sub: u8, data: &IndexChunk) {
		match self.log.local_index.entry(table).or_default().map.entry(index) {
			std::collections::hash_map::Entry::Occupied(mut entry) => {
				*entry.get_mut() = (self.log.record_id, entry.get().1 | (1 << sub), *data);
			},
			std::collections::hash_map::Entry::Vacant(entry) => {
				entry.insert((self.log.record_id, 1 << sub, *data));
			},
		}
	}

	pub fn insert_value(&mut self, table: ValueTableId, index: u64, data: Vec<u8>) {
		self.log
			.local_values
			.entry(table)
			.or_default()
			.map
			.insert(index, (self.log.record_id, data));
	}

	pub fn drop_table(&mut self, id: IndexTableId) {
		self.log.dropped_tables.push(id);
	}

	pub fn drain(self) -> LogChange {
		self.log
	}
}

impl<'a> LogQuery for LogWriter<'a> {
	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		match self
			.log
			.local_index
			.get(&table)
			.and_then(|o| o.map.get(&index).map(|(_id, _mask, data)| data))
		{
			Some(data) => Some(f(data)),
			None => self.overlays.with_index(table, index, f),
		}
	}

	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool {
		if let Some(d) = self
			.log
			.local_values
			.get(&table)
			.and_then(|o| o.map.get(&index).map(|(_id, data)| data))
		{
			let len = dest.len().min(d.len());
			dest[0..len].copy_from_slice(&d[0..len]);
			true
		} else {
			self.overlays.value(table, index, dest)
		}
	}
}

// Identity hash.
#[derive(Default, Clone)]
pub struct IdentityHash(u64);
pub type BuildIdHash = std::hash::BuildHasherDefault<IdentityHash>;

impl std::hash::Hasher for IdentityHash {
	fn write(&mut self, _: &[u8]) {
		unreachable!()
	}
	fn write_u8(&mut self, _: u8) {
		unreachable!()
	}
	fn write_u16(&mut self, _: u16) {
		unreachable!()
	}
	fn write_u32(&mut self, _: u32) {
		unreachable!()
	}
	fn write_u64(&mut self, n: u64) {
		self.0 = n
	}
	fn write_usize(&mut self, _: usize) {
		unreachable!()
	}
	fn write_i8(&mut self, _: i8) {
		unreachable!()
	}
	fn write_i16(&mut self, _: i16) {
		unreachable!()
	}
	fn write_i32(&mut self, _: i32) {
		unreachable!()
	}
	fn write_i64(&mut self, _: i64) {
		unreachable!()
	}
	fn write_isize(&mut self, _: isize) {
		unreachable!()
	}
	fn finish(&self) -> u64 {
		self.0
	}
}

#[derive(Default)]
pub struct IndexLogOverlay {
	pub map: HashMap<u64, (u64, u64, IndexChunk)>, // index -> (record_id, modified_mask, entry)
}

// We use identity hash for value overlay/log records so that writes to value tables are in order.
#[derive(Default)]
pub struct ValueLogOverlay {
	pub map: HashMap<u64, (u64, Vec<u8>), BuildIdHash>, // index -> (record_id, entry)
}

struct Appending {
	id: u32,
	file: std::io::BufWriter<std::fs::File>,
	size: u64,
}

struct Flushing {
	id: u32,
	file: std::fs::File,
}

struct Reading {
	id: u32,
	file: std::io::BufReader<std::fs::File>,
}

#[derive(Eq, PartialEq)]
enum ReadingState {
	Reading,
	Idle,
}

pub struct Log {
	overlays: RwLock<LogOverlays>,
	appending: RwLock<Option<Appending>>,
	reading: RwLock<Option<Reading>>,
	reading_state: Mutex<ReadingState>,
	done_reading_cv: Condvar,
	flushing: Mutex<Option<Flushing>>,
	next_record_id: AtomicU64,
	dirty: AtomicBool,
	log_pool: RwLock<VecDeque<(u32, std::fs::File)>>,
	cleanup_queue: RwLock<VecDeque<(u32, std::fs::File)>>,
	replay_queue: RwLock<VecDeque<(u32, u64, std::fs::File)>>,
	path: std::path::PathBuf,
	next_log_id: AtomicU32,
	sync: bool,
}

impl Log {
	pub fn open(options: &Options) -> Result<Log> {
		let path = options.path.clone();
		let mut logs = VecDeque::new();
		let mut max_log_id = 0;
		for entry in std::fs::read_dir(&path)? {
			let entry = entry?;
			if let Some(name) = entry.file_name().as_os_str().to_str() {
				if entry.metadata()?.is_file() && name.starts_with("log") {
					if let Ok(nlog) = std::str::FromStr::from_str(&name[3..]) {
						let path = Self::log_path(&path, nlog);
						let (file, record_id) = Self::open_log_file(&path)?;
						if let Some(record_id) = record_id {
							log::debug!(target: "parity-db", "Opened log {}, record {}", nlog, record_id);
							logs.push_back((nlog, record_id, file));
							if nlog > max_log_id {
								max_log_id = nlog
							}
						} else {
							log::debug!(target: "parity-db", "Removing log {}", nlog);
							std::mem::drop(file);
							std::fs::remove_file(&path)?;
						}
					}
				}
			}
		}
		logs.make_contiguous().sort_by_key(|(_id, record_id, _)| *record_id);
		let next_log_id = if logs.is_empty() { 0 } else { max_log_id + 1 };

		Ok(Log {
			overlays: Default::default(),
			appending: RwLock::new(None),
			reading: RwLock::new(None),
			reading_state: Mutex::new(ReadingState::Idle),
			done_reading_cv: Condvar::new(),
			flushing: Mutex::new(None),
			next_record_id: AtomicU64::new(1),
			next_log_id: AtomicU32::new(next_log_id),
			dirty: AtomicBool::new(true),
			sync: options.sync_wal,
			replay_queue: RwLock::new(logs),
			cleanup_queue: RwLock::new(Default::default()),
			log_pool: RwLock::new(Default::default()),
			path,
		})
	}

	fn log_path(root: &std::path::Path, id: u32) -> std::path::PathBuf {
		let mut path: std::path::PathBuf = root.into();
		path.push(format!("log{}", id));
		path
	}

	pub fn replay_record_id(&self) -> Option<u64> {
		self.replay_queue.read().front().map(|(_id, record_id, _)| *record_id)
	}

	pub fn open_log_file(path: &std::path::Path) -> Result<(std::fs::File, Option<u64>)> {
		let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
		if file.metadata()?.len() == 0 {
			return Ok((file, None))
		}
		// read first record id
		let mut buf = [0; 9];
		file.read_exact(&mut buf)?;
		file.seek(std::io::SeekFrom::Start(0))?;
		let id = u64::from_le_bytes(buf[1..].try_into().unwrap());
		log::debug!(target: "parity-db", "Opened existing log {}, first record_id = {}", path.display(), id);
		Ok((file, Some(id)))
	}

	fn drop_log(&self, id: u32) -> Result<()> {
		log::debug!(target: "parity-db", "Drop log {}", id);
		let path = Self::log_path(&self.path, id);
		std::fs::remove_file(&path)?;
		Ok(())
	}

	pub fn clear_replay_logs(&self) -> Result<()> {
		{
			let mut reading = self.reading.write();
			let id = reading.as_ref().map(|r| r.id);
			*reading = None;
			if let Some(id) = id {
				self.drop_log(id)?;
			}
		}
		{
			let replay_logs = std::mem::take(&mut *self.replay_queue.write());
			for (id, _, file) in replay_logs {
				std::mem::drop(file);
				self.drop_log(id)?;
			}
		}
		let mut overlays = self.overlays.write();
		overlays.index.clear();
		overlays.value.clear();
		overlays.last_record_id.clear();
		*self.reading_state.lock() = ReadingState::Idle;
		self.dirty.store(false, Ordering::Relaxed);
		Ok(())
	}

	pub fn begin_record(&self) -> LogWriter<'_> {
		let id = self.next_record_id.fetch_add(1, Ordering::Relaxed);
		LogWriter::new(&self.overlays, id)
	}

	pub fn end_record(&self, log: LogChange) -> Result<u64> {
		assert!(log.record_id + 1 == self.next_record_id.load(Ordering::Relaxed));
		let record_id = log.record_id;
		let mut appending = self.appending.write();
		if appending.is_none() {
			// Find a log file in the pool or create a new one
			let (id, file) = if let Some((id, file)) = self.log_pool.write().pop_front() {
				log::debug!(target: "parity-db", "Flush: Activated pool writer {}", id);
				(id, file)
			} else {
				// find a free id
				let id = self.next_log_id.fetch_add(1, Ordering::SeqCst);
				let path = Self::log_path(&self.path, id);
				let file =
					std::fs::OpenOptions::new().create(true).read(true).write(true).open(path)?;
				log::debug!(target: "parity-db", "Flush: Activated new writer {}", id);
				(id, file)
			};
			*appending = Some(Appending { size: 0, file: std::io::BufWriter::new(file), id });
		}
		let appending = appending.as_mut().unwrap();
		let FlushedLog { index, values, bytes } = log.flush_to_file(&mut appending.file)?;
		let mut overlays = self.overlays.write();
		let mut total_index = 0;
		for (id, overlay) in index.into_iter() {
			total_index += overlay.map.len();
			overlays.index.entry(id).or_default().map.extend(overlay.map.into_iter());
		}
		let mut total_value = 0;
		for (id, overlay) in values.into_iter() {
			total_value += overlay.map.len();
			overlays.last_record_id.insert(id.col(), record_id);
			overlays.value.entry(id).or_default().map.extend(overlay.map.into_iter());
		}

		log::debug!(
			target: "parity-db",
			"Finalizing log record {} ({} index, {} value)",
			record_id,
			total_index,
			total_value,
		);
		appending.size += bytes;
		self.dirty.store(true, Ordering::Relaxed);
		Ok(bytes)
	}

	pub fn end_read(&self, cleared: Cleared, record_id: u64) {
		if record_id >= self.next_record_id.load(Ordering::Relaxed) {
			self.next_record_id.store(record_id + 1, Ordering::Relaxed);
		}
		let mut overlays = self.overlays.write();
		for (table, index) in cleared.index.into_iter() {
			if let Some(ref mut overlay) = overlays.index.get_mut(&table) {
				if let std::collections::hash_map::Entry::Occupied(e) = overlay.map.entry(index) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}
		for (table, index) in cleared.values.into_iter() {
			if let Some(ref mut overlay) = overlays.value.get_mut(&table) {
				if let std::collections::hash_map::Entry::Occupied(e) = overlay.map.entry(index) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}
		// Cleanup index overlays
		overlays.index.retain(|_, overlay| !overlay.map.is_empty());
	}

	pub fn flush_one(&self, min_size: u64) -> Result<(bool, bool, bool)> {
		// Wait for the reader to finish reading
		let mut flushing = self.flushing.lock();
		let mut read_next = false;
		let mut cleanup = false;
		if flushing.is_some() {
			let mut reading_state = self.reading_state.lock();

			while *reading_state == ReadingState::Reading {
				log::debug!(target: "parity-db", "Flush: Awaiting log reader");
				self.done_reading_cv.wait(&mut reading_state)
			}

			{
				let mut reading = self.reading.write();
				if let Some(reading) = reading.take() {
					log::debug!(target: "parity-db", "Flush: Activated log cleanup {}", reading.id);
					let file = reading.file.into_inner();
					self.cleanup_queue.write().push_back((reading.id, file));
					*reading_state = ReadingState::Idle;
					cleanup = true;
				}

				if let Some(mut flushing) = flushing.take() {
					log::debug!(target: "parity-db", "Flush: Activated log reader {}", flushing.id);
					flushing.file.seek(std::io::SeekFrom::Start(0))?;
					*reading = Some(Reading {
						id: flushing.id,
						file: std::io::BufReader::new(flushing.file),
					});
					*reading_state = ReadingState::Reading;
					read_next = true;
				}
			}
		}

		{
			// Lock writer and reset it
			let cur_size = self.appending.read().as_ref().map_or(0, |r| r.size);
			if cur_size > 0 && cur_size > min_size {
				let mut appending = self.appending.write();
				let to_flush = appending.take();
				*flushing = to_flush.map(|to_flush| Flushing {
					file: to_flush.file.into_inner().unwrap(),
					id: to_flush.id,
				});
			}
		}

		// Flush to disk
		if self.sync {
			if let Some(flushing) = flushing.as_ref() {
				log::debug!(target: "parity-db", "Flush: Flushing log to disk");
				flushing.file.sync_data()?;
				log::debug!(target: "parity-db", "Flush: Flushing log completed");
			}
		}

		Ok((flushing.is_some(), read_next, cleanup))
	}

	pub fn replay_next(&mut self) -> Result<Option<u32>> {
		let mut reading = self.reading.write();
		{
			if let Some(reading) = reading.take() {
				log::debug!(target: "parity-db", "Replay: Activated log cleanup {}", reading.id);
				let file = reading.file.into_inner();
				self.cleanup_queue.write().push_back((reading.id, file));
			}
		}
		if let Some((id, _record_id, file)) = self.replay_queue.write().pop_front() {
			log::debug!(target: "parity-db", "Replay: Activated log reader {}", id);
			*reading = Some(Reading { id, file: std::io::BufReader::new(file) });
			*self.reading_state.lock() = ReadingState::Reading;
			Ok(Some(id))
		} else {
			*self.reading_state.lock() = ReadingState::Idle;
			Ok(None)
		}
	}

	pub fn clean_logs(&self, count: usize) -> Result<bool> {
		let mut cleaned: Vec<_> = { self.cleanup_queue.write().drain(0..count).collect() };
		for (id, ref mut file) in cleaned.iter_mut() {
			log::debug!(target: "parity-db", "Cleaned: {}", id);
			file.seek(std::io::SeekFrom::Start(0))?;
			file.set_len(0)?;
		}
		// Move cleaned logs back to the pool
		let mut pool = self.log_pool.write();
		pool.extend(cleaned);
		// Sort to reuse lower IDs an prevent IDs from growing.
		pool.make_contiguous().sort_by_key(|(id, _)| *id);
		if pool.len() > MAX_LOG_POOL_SIZE {
			let removed = pool.drain(MAX_LOG_POOL_SIZE..);
			for (id, file) in removed {
				std::mem::drop(file);
				self.drop_log(id)?;
			}
		}
		Ok(!self.cleanup_queue.read().is_empty())
	}

	pub fn num_dirty_logs(&self) -> usize {
		self.cleanup_queue.read().len()
	}

	pub fn read_next(&self, validate: bool) -> Result<Option<LogReader<'_>>> {
		let mut reading_state = self.reading_state.lock();
		if *reading_state != ReadingState::Reading {
			log::trace!(target: "parity-db", "No logs to enact");
			return Ok(None)
		}

		let reading = self.reading.write();
		if reading.is_none() {
			log::trace!(target: "parity-db", "No active reader");
			return Ok(None)
		}
		let reading = RwLockWriteGuard::map(reading, |r| &mut r.as_mut().unwrap().file);
		let mut reader = LogReader::new(reading, validate);
		match reader.next() {
			Ok(LogAction::BeginRecord) => Ok(Some(reader)),
			Ok(_) => Err(Error::Corruption("Bad log record structure".into())),
			Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
				*reading_state = ReadingState::Idle;
				self.done_reading_cv.notify_one();
				log::debug!(target: "parity-db", "Read: End of log");
				Ok(None)
			},
			Err(e) => Err(e),
		}
	}

	pub fn overlays(&self) -> &RwLock<LogOverlays> {
		&self.overlays
	}

	pub fn kill_logs(&self) -> Result<()> {
		let mut log_pool = self.log_pool.write();
		for (id, file) in log_pool.drain(..) {
			std::mem::drop(file);
			self.drop_log(id)?;
		}
		if let Some(reading) = self.reading.write().take() {
			std::mem::drop(reading.file);
			self.drop_log(reading.id)?;
		}
		Ok(())
	}
}
