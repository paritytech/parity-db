// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::ColId,
	error::{try_io, Error, Result},
	file::MappedBytesGuard,
	index::{Chunk as IndexChunk, TableId as IndexTableId, ENTRY_BYTES},
	options::Options,
	parking_lot::{RwLock, RwLockWriteGuard},
	ref_count::{Chunk as RefCountChunk, RefCountTableId, ENTRY_BYTES as REF_COUNT_ENTRY_BYTES},
	table::TableId as ValueTableId,
};
use std::{
	cmp::min,
	collections::{HashMap, VecDeque},
	convert::TryInto,
	io::{ErrorKind, Read, Seek, Write},
	sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

const MAX_LOG_POOL_SIZE: usize = 16;
const BEGIN_RECORD: u8 = 1;
const INSERT_INDEX: u8 = 2;
const INSERT_VALUE: u8 = 3;
const END_RECORD: u8 = 4;
const DROP_TABLE: u8 = 5;
const INSERT_REF_COUNT: u8 = 6;
const DROP_REF_COUNT_TABLE: u8 = 7;

// Once index overly uses less than 1/10 of its capacity, it will be reclaimed.
const INDEX_OVERLAY_RECLAIM_FACTOR: usize = 10;
// Once value overly uses less than 1/10 of its capacity, it will be reclaimed.
const VALUE_OVERLAY_RECLAIM_FACTOR: usize = 10;
// Min number of value items to initiate reclaim. Each item is around 40 bytes.
const VALUE_OVERLAY_MIN_RECLAIM_ITEMS: usize = 10240;
// Once ref count overlay uses less than 1/10 of its capacity, it will be reclaimed.
const REF_COUNT_OVERLAY_RECLAIM_FACTOR: usize = 10;

#[derive(Debug)]
pub struct InsertIndexAction {
	pub table: IndexTableId,
	pub index: u64,
}

#[derive(Debug)]
pub struct InsertValueAction {
	pub table: ValueTableId,
	pub index: u64,
}

#[derive(Debug)]
pub struct InsertRefCountAction {
	pub table: RefCountTableId,
	pub index: u64,
}

#[derive(Debug)]
pub enum LogAction {
	BeginRecord,
	InsertIndex(InsertIndexAction),
	InsertValue(InsertValueAction),
	InsertRefCount(InsertRefCountAction),
	DropTable(IndexTableId),
	DropRefCountTable(RefCountTableId),
	EndRecord,
}

pub trait LogQuery {
	type ValueRef<'a>: std::ops::Deref<Target = [u8]>
	where
		Self: 'a;

	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R>;
	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool;
	fn value_ref<'a>(&'a self, table: ValueTableId, index: u64) -> Option<Self::ValueRef<'a>>;

	fn ref_count<R, F: FnOnce(&RefCountChunk) -> R>(
		&self,
		table: RefCountTableId,
		index: u64,
		f: F,
	) -> Option<R>;
}

#[derive(Debug)]
pub struct LogOverlays {
	index: Vec<IndexLogOverlay>,
	value: Vec<ValueLogOverlay>,
	ref_count: Vec<RefCountLogOverlay>,
	last_record_ids: Vec<u64>,
}

impl LogOverlays {
	pub fn last_record_id(&self, col: ColId) -> u64 {
		self.last_record_ids.get(col as usize).cloned().unwrap_or(u64::MAX)
	}

	pub fn with_columns(count: usize) -> Self {
		Self {
			index: (0..IndexTableId::max_log_indicies(count))
				.map(|_| IndexLogOverlay::default())
				.collect(),
			value: (0..ValueTableId::max_log_tables(count))
				.map(|_| ValueLogOverlay::default())
				.collect(),
			ref_count: (0..RefCountTableId::max_log_indicies(count))
				.map(|_| RefCountLogOverlay::default())
				.collect(),
			last_record_ids: (0..count).map(|_| 0).collect(),
		}
	}
}

impl LogQuery for RwLock<LogOverlays> {
	type ValueRef<'a> = MappedBytesGuard<'a>;

	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		(&*self.read()).with_index(table, index, f)
	}

	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool {
		(&*self.read()).value(table, index, dest)
	}

	#[cfg(not(feature = "loom"))]
	fn value_ref<'a>(&'a self, table: ValueTableId, index: u64) -> Option<Self::ValueRef<'a>> {
		let lock =
			parking_lot::RwLockReadGuard::try_map(self.read(), |o| o.value_ref(table, index));
		lock.ok()
	}

	#[cfg(feature = "loom")]
	fn value_ref<'a>(&'a self, table: ValueTableId, index: u64) -> Option<Self::ValueRef<'a>> {
		self.read().value_ref(table, index).map(|o| MappedBytesGuard::new(o.to_vec()))
	}

	fn ref_count<R, F: FnOnce(&RefCountChunk) -> R>(
		&self,
		table: RefCountTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		(&*self.read()).ref_count(table, index, f)
	}
}

impl LogQuery for LogOverlays {
	type ValueRef<'a> = &'a [u8];
	fn with_index<R, F: FnOnce(&IndexChunk) -> R>(
		&self,
		table: IndexTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		self.index
			.get(table.log_index())
			.and_then(|o| o.map.get(&index).map(|(_id, _mask, data)| f(data)))
	}

	fn value(&self, table: ValueTableId, index: u64, dest: &mut [u8]) -> bool {
		let s = self;
		if let Some(d) = s
			.value
			.get(table.log_index())
			.and_then(|o| o.map.get(&index).map(|(_id, data)| data))
		{
			let len = dest.len().min(d.len());
			dest[0..len].copy_from_slice(&d[0..len]);
			true
		} else {
			false
		}
	}
	fn value_ref<'a>(&'a self, table: ValueTableId, index: u64) -> Option<Self::ValueRef<'a>> {
		self.value
			.get(table.log_index())
			.and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_slice()))
	}

	fn ref_count<R, F: FnOnce(&RefCountChunk) -> R>(
		&self,
		table: RefCountTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		self.ref_count
			.get(table.log_index())
			.and_then(|o| o.map.get(&index).map(|(_id, _mask, data)| f(data)))
	}
}

#[derive(Debug, Default)]
pub struct Cleared {
	index: Vec<(IndexTableId, u64)>,
	values: Vec<(ValueTableId, u64)>,
	ref_count: Vec<(RefCountTableId, u64)>,
}

#[derive(Debug)]
pub struct LogReader<'a> {
	reading: RwLockWriteGuard<'a, Option<Reading>>,
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

	fn new(reading: RwLockWriteGuard<'a, Option<Reading>>, validate: bool) -> LogReader<'a> {
		LogReader {
			cleared: Default::default(),
			reading,
			record_id: 0,
			read_bytes: 0,
			crc32: crc32fast::Hasher::new(),
			validate,
		}
	}

	pub fn reset(&mut self) -> Result<()> {
		self.cleared = Default::default();
		try_io!(self
			.reading
			.as_mut()
			.unwrap()
			.file
			.seek(std::io::SeekFrom::Current(-(self.read_bytes as i64))));
		self.read_bytes = 0;
		self.record_id = 0;
		self.crc32 = crc32fast::Hasher::new();
		Ok(())
	}

	pub fn next(&mut self) -> Result<LogAction> {
		let mut read_buf = |size, buf: &mut [u8; 8]| -> Result<()> {
			try_io!(self.reading.as_mut().unwrap().file.read_exact(&mut buf[0..size]));
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
			INSERT_REF_COUNT => {
				read_buf(2, &mut buf)?;
				let table =
					RefCountTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				read_buf(8, &mut buf)?;
				let index = u64::from_le_bytes(buf);
				self.cleared.ref_count.push((table, index));
				Ok(LogAction::InsertRefCount(InsertRefCountAction { table, index }))
			},
			END_RECORD => {
				try_io!(self.reading.as_mut().unwrap().file.read_exact(&mut buf[0..4]));
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
			DROP_REF_COUNT_TABLE => {
				read_buf(2, &mut buf)?;
				let table =
					RefCountTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				Ok(LogAction::DropRefCountTable(table))
			},
			_ => Err(Error::Corruption("Bad log entry type".into())),
		}
	}

	pub fn read(&mut self, buf: &mut [u8]) -> Result<()> {
		try_io!(self.reading.as_mut().unwrap().file.read_exact(buf));
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

#[derive(Debug)]
pub struct LogChange {
	local_index: HashMap<IndexTableId, IndexLogOverlay>,
	local_values: HashMap<ValueTableId, ValueLogOverlayLocal>,
	local_ref_count: HashMap<RefCountTableId, RefCountLogOverlay>,
	record_id: u64,
	dropped_tables: Vec<IndexTableId>,
	dropped_ref_count_tables: Vec<RefCountTableId>,
}

impl LogChange {
	fn new(record_id: u64) -> LogChange {
		LogChange {
			local_index: Default::default(),
			local_values: Default::default(),
			local_ref_count: Default::default(),
			dropped_tables: Default::default(),
			dropped_ref_count_tables: Default::default(),
			record_id,
		}
	}

	pub fn local_values_changes(&self, id: ValueTableId) -> Option<&ValueLogOverlayLocal> {
		self.local_values.get(&id)
	}

	fn flush_to_file(self, file: &mut std::io::BufWriter<std::fs::File>) -> Result<FlushedLog> {
		let mut crc32 = crc32fast::Hasher::new();
		let mut bytes: u64 = 0;

		let mut write = |buf: &[u8]| -> Result<()> {
			try_io!(file.write_all(buf));
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
					write(&chunk.0[i as usize * ENTRY_BYTES..(i as usize + 1) * ENTRY_BYTES])?;
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
		for (id, overlay) in self.local_ref_count.iter() {
			for (index, (_, modified_entries_mask, chunk)) in overlay.map.iter() {
				write(INSERT_REF_COUNT.to_le_bytes().as_ref())?;
				write(&id.as_u16().to_le_bytes())?;
				write(&index.to_le_bytes())?;
				write(&modified_entries_mask.to_le_bytes())?;
				let mut mask = *modified_entries_mask;
				while mask != 0 {
					let i = mask.trailing_zeros();
					mask &= !(1 << i);
					write(
						&chunk.0[i as usize * REF_COUNT_ENTRY_BYTES..
							(i as usize + 1) * REF_COUNT_ENTRY_BYTES],
					)?;
				}
			}
		}
		for id in self.dropped_tables.iter() {
			log::debug!(target: "parity-db", "Finalizing drop {}", id);
			write(DROP_TABLE.to_le_bytes().as_ref())?;
			write(&id.as_u16().to_le_bytes())?;
		}
		for id in self.dropped_ref_count_tables.iter() {
			log::debug!(target: "parity-db", "Finalizing ref count drop {}", id);
			write(DROP_REF_COUNT_TABLE.to_le_bytes().as_ref())?;
			write(&id.as_u16().to_le_bytes())?;
		}
		write(&END_RECORD.to_le_bytes())?;
		let checksum: u32 = crc32.finalize();
		try_io!(file.write_all(&checksum.to_le_bytes()));
		bytes += 4;
		try_io!(file.flush());
		Ok(FlushedLog {
			index: self.local_index,
			values: self.local_values,
			ref_count: self.local_ref_count,
			bytes,
		})
	}
}

#[derive(Debug)]
struct FlushedLog {
	index: HashMap<IndexTableId, IndexLogOverlay>,
	values: HashMap<ValueTableId, ValueLogOverlayLocal>,
	ref_count: HashMap<RefCountTableId, RefCountLogOverlay>,
	bytes: u64,
}

#[derive(Debug)]
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

	pub fn insert_index(&mut self, table: IndexTableId, index: u64, sub: u8, data: IndexChunk) {
		match self.log.local_index.entry(table).or_default().map.entry(index) {
			std::collections::hash_map::Entry::Occupied(mut entry) => {
				*entry.get_mut() = (self.log.record_id, entry.get().1 | (1 << sub), data);
			},
			std::collections::hash_map::Entry::Vacant(entry) => {
				entry.insert((self.log.record_id, 1 << sub, data));
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

	pub fn insert_ref_count(
		&mut self,
		table: RefCountTableId,
		index: u64,
		sub: u8,
		data: RefCountChunk,
	) {
		match self.log.local_ref_count.entry(table).or_default().map.entry(index) {
			std::collections::hash_map::Entry::Occupied(mut entry) => {
				*entry.get_mut() = (self.log.record_id, entry.get().1 | (1 << sub), data);
			},
			std::collections::hash_map::Entry::Vacant(entry) => {
				entry.insert((self.log.record_id, 1 << sub, data));
			},
		}
	}

	pub fn drop_table(&mut self, id: IndexTableId) {
		self.log.dropped_tables.push(id);
	}

	pub fn drop_ref_count_table(&mut self, id: RefCountTableId) {
		self.log.dropped_ref_count_tables.push(id);
	}

	pub fn drain(self) -> LogChange {
		self.log
	}
}

pub enum LogWriterValueGuard<'a> {
	Local(&'a [u8]),
	Overlay(MappedBytesGuard<'a>),
}

impl std::ops::Deref for LogWriterValueGuard<'_> {
	type Target = [u8];
	fn deref(&self) -> &[u8] {
		match self {
			LogWriterValueGuard::Local(data) => data,
			LogWriterValueGuard::Overlay(data) => data.deref(),
		}
	}
}

impl<'q> LogQuery for LogWriter<'q> {
	type ValueRef<'a> = LogWriterValueGuard<'a> where Self: 'a;
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
	fn value_ref<'v>(&'v self, table: ValueTableId, index: u64) -> Option<Self::ValueRef<'v>> {
		self.log
			.local_values
			.get(&table)
			.and_then(|o| {
				o.map.get(&index).map(|(_id, data)| LogWriterValueGuard::Local(data.as_slice()))
			})
			.or_else(|| {
				self.overlays
					.value_ref(table, index)
					.map(|data| LogWriterValueGuard::Overlay(data))
			})
	}

	fn ref_count<R, F: FnOnce(&RefCountChunk) -> R>(
		&self,
		table: RefCountTableId,
		index: u64,
		f: F,
	) -> Option<R> {
		match self
			.log
			.local_ref_count
			.get(&table)
			.and_then(|o| o.map.get(&index).map(|(_id, _mask, data)| data))
		{
			Some(data) => Some(f(data)),
			None => self.overlays.ref_count(table, index, f),
		}
	}
}

// Identity hash.
#[derive(Debug, Default, Clone)]
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

#[derive(Debug, Default)]
pub struct IndexLogOverlay {
	pub map: HashMap<u64, (u64, u64, IndexChunk)>, // index -> (record_id, modified_mask, entry)
}

// We use identity hash for value overlay/log records so that writes to value tables are in order.
#[derive(Debug, Default)]
pub struct ValueLogOverlay {
	pub map: HashMap<u64, (u64, Vec<u8>)>, // index -> (record_id, entry)
}
#[derive(Debug, Default)]
pub struct ValueLogOverlayLocal {
	pub map: HashMap<u64, (u64, Vec<u8>), BuildIdHash>, // index -> (record_id, entry)
}

#[derive(Debug, Default)]
pub struct RefCountLogOverlay {
	pub map: HashMap<u64, (u64, u64, RefCountChunk)>, // index -> (record_id, modified_mask, entry)
}

#[derive(Debug)]
struct Appending {
	id: u32,
	file: std::io::BufWriter<std::fs::File>,
	size: u64,
}

#[derive(Debug)]
struct Reading {
	id: u32,
	file: std::io::BufReader<std::fs::File>,
}

#[derive(Debug)]
pub struct Log {
	overlays: RwLock<LogOverlays>,
	appending: RwLock<Option<Appending>>,
	reading: RwLock<Option<Reading>>,
	read_queue: RwLock<VecDeque<(u32, std::fs::File)>>,
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
		for entry in try_io!(std::fs::read_dir(&path)) {
			let entry = try_io!(entry);
			if let Some(name) = entry.file_name().as_os_str().to_str() {
				if try_io!(entry.metadata()).is_file() && name.starts_with("log") {
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
							drop(file);
							try_io!(std::fs::remove_file(&path));
						}
					}
				}
			}
		}
		logs.make_contiguous().sort_by_key(|(_id, record_id, _)| *record_id);
		let next_log_id = if logs.is_empty() { 0 } else { max_log_id + 1 };

		Ok(Log {
			overlays: RwLock::new(LogOverlays::with_columns(options.columns.len())),
			appending: RwLock::new(None),
			reading: RwLock::new(None),
			read_queue: RwLock::default(),
			next_record_id: AtomicU64::new(1),
			next_log_id: AtomicU32::new(next_log_id),
			dirty: AtomicBool::new(true),
			sync: options.sync_wal,
			replay_queue: RwLock::new(logs),
			cleanup_queue: RwLock::default(),
			log_pool: RwLock::default(),
			path,
		})
	}

	fn log_path(root: &std::path::Path, id: u32) -> std::path::PathBuf {
		let mut path: std::path::PathBuf = root.into();
		path.push(format!("log{id}"));
		path
	}

	pub fn replay_record_id(&self) -> Option<u64> {
		self.replay_queue.read().front().map(|(_id, record_id, _)| *record_id)
	}

	pub fn open_log_file(path: &std::path::Path) -> Result<(std::fs::File, Option<u64>)> {
		let mut file = try_io!(std::fs::OpenOptions::new().read(true).write(true).open(path));
		if try_io!(file.metadata()).len() == 0 {
			return Ok((file, None))
		}
		match Self::read_first_record_id(&mut file) {
			Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
				log::error!(target: "parity-db", "Opened existing log {}. No first record id found", path.display());
				Ok((file, None))
			},
			Err(e) => Err(e),
			Ok(id) => {
				try_io!(file.rewind());
				log::debug!(target: "parity-db", "Opened existing log {}, first record_id = {}", path.display(), id);
				Ok((file, Some(id)))
			},
		}
	}

	fn read_first_record_id(file: &mut std::fs::File) -> Result<u64> {
		let mut buf = [0; 9];
		try_io!(file.read_exact(&mut buf));
		Ok(u64::from_le_bytes(buf[1..].try_into().unwrap()))
	}

	fn drop_log(&self, id: u32) -> Result<()> {
		log::debug!(target: "parity-db", "Drop log {}", id);
		let path = Self::log_path(&self.path, id);
		try_io!(std::fs::remove_file(path));
		Ok(())
	}

	pub fn clear_replay_logs(&self) {
		if let Some(reading) = self.reading.write().take() {
			self.cleanup_queue.write().push_back((reading.id, reading.file.into_inner()));
		}
		for (id, _, file) in self.replay_queue.write().drain(0..) {
			self.cleanup_queue.write().push_back((id, file));
		}
		let mut overlays = self.overlays.write();
		for o in overlays.index.iter_mut() {
			o.map.clear();
		}
		for o in overlays.value.iter_mut() {
			o.map.clear();
		}
		for o in overlays.ref_count.iter_mut() {
			o.map.clear();
		}
		for r in overlays.last_record_ids.iter_mut() {
			*r = 0;
		}
		self.dirty.store(false, Ordering::Relaxed);
	}

	pub fn begin_record(&self) -> LogWriter<'_> {
		let id = self.next_record_id.fetch_add(1, Ordering::Relaxed);
		LogWriter::new(&self.overlays, id)
	}

	pub fn end_record(&self, log: LogChange) -> Result<u64> {
		assert_eq!(log.record_id + 1, self.next_record_id.load(Ordering::Relaxed));
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
				let file = try_io!(std::fs::OpenOptions::new()
					.create(true)
					.read(true)
					.write(true)
					.open(path));
				log::debug!(target: "parity-db", "Flush: Activated new writer {}", id);
				(id, file)
			};
			*appending = Some(Appending { size: 0, file: std::io::BufWriter::new(file), id });
		}
		let appending = appending.as_mut().unwrap();
		let FlushedLog { index, values, ref_count, bytes } =
			log.flush_to_file(&mut appending.file)?;
		let mut overlays = self.overlays.write();
		let mut total_index = 0;
		for (id, overlay) in index.into_iter() {
			total_index += overlay.map.len();
			overlays.index[id.log_index()].map.extend(overlay.map.into_iter());
		}
		let mut total_value = 0;
		for (id, overlay) in values.into_iter() {
			total_value += overlay.map.len();
			overlays.last_record_ids[id.col() as usize] = record_id;
			overlays.value[id.log_index()].map.extend(overlay.map.into_iter());
		}
		let mut total_ref_count = 0;
		for (id, overlay) in ref_count.into_iter() {
			total_ref_count += overlay.map.len();
			overlays.ref_count[id.log_index()].map.extend(overlay.map.into_iter());
		}

		log::debug!(
			target: "parity-db",
			"Finalizing log record {} ({} index, {} value, {} ref count)",
			record_id,
			total_index,
			total_value,
			total_ref_count,
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
			if let Some(ref mut overlay) = overlays.index.get_mut(table.log_index()) {
				if let std::collections::hash_map::Entry::Occupied(e) = overlay.map.entry(index) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}
		for (table, index) in cleared.values.into_iter() {
			if let Some(ref mut overlay) = overlays.value.get_mut(table.log_index()) {
				if let std::collections::hash_map::Entry::Occupied(e) = overlay.map.entry(index) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}
		for (table, index) in cleared.ref_count.into_iter() {
			if let Some(ref mut overlay) = overlays.ref_count.get_mut(table.log_index()) {
				if let std::collections::hash_map::Entry::Occupied(e) = overlay.map.entry(index) {
					if e.get().0 == record_id {
						e.remove_entry();
					}
				}
			}
		}
		// Reclaim overlay memory
		for (i, o) in overlays.index.iter_mut().enumerate() {
			if o.map.capacity() > o.map.len() * INDEX_OVERLAY_RECLAIM_FACTOR {
				log::trace!(
					"Schrinking index overlay {}: {}/{}",
					IndexTableId::from_log_index(i),
					o.map.len(),
					o.map.capacity(),
				);
				o.map.shrink_to_fit();
			}
		}
		for (i, o) in overlays.value.iter_mut().enumerate() {
			if o.map.capacity() > VALUE_OVERLAY_MIN_RECLAIM_ITEMS &&
				o.map.capacity() > o.map.len() * VALUE_OVERLAY_RECLAIM_FACTOR
			{
				log::trace!(
					"Schrinking value overlay {}: {}/{}",
					ValueTableId::from_log_index(i),
					o.map.len(),
					o.map.capacity(),
				);
				o.map.shrink_to_fit();
			}
		}
		for (i, o) in overlays.ref_count.iter_mut().enumerate() {
			if o.map.capacity() > o.map.len() * REF_COUNT_OVERLAY_RECLAIM_FACTOR {
				log::trace!(
					"Schrinking ref count overlay {}: {}/{}",
					RefCountTableId::from_log_index(i),
					o.map.len(),
					o.map.capacity(),
				);
				o.map.shrink_to_fit();
			}
		}
	}

	pub fn flush_one(&self, min_size: u64) -> Result<bool> {
		// If it exists take the writer and flush it
		let cur_size = self.appending.read().as_ref().map_or(0, |r| r.size);
		if cur_size > min_size {
			if let Some(to_flush) = self.appending.write().take() {
				let file = try_io!(to_flush.file.into_inner().map_err(|e| e.into_error()));
				if self.sync {
					log::debug!(target: "parity-db", "Flush: Flushing log to disk");
					try_io!(file.sync_data());
					log::debug!(target: "parity-db", "Flush: Flushing log completed");
				}
				self.read_queue.write().push_back((to_flush.id, file));
			}
			return Ok(true)
		}
		Ok(false)
	}

	pub fn replay_next(&self) -> Result<Option<u32>> {
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
			Ok(Some(id))
		} else {
			Ok(None)
		}
	}

	pub fn clean_logs(&self, max_count: usize) -> Result<bool> {
		let mut cleaned: Vec<_> = {
			let mut queue = self.cleanup_queue.write();
			let count = min(max_count, queue.len());
			queue.drain(0..count).collect()
		};
		for (id, ref mut file) in cleaned.iter_mut() {
			log::debug!(target: "parity-db", "Cleaned: {}", id);
			try_io!(file.rewind());
			try_io!(file.set_len(0));
			file.sync_all().map_err(Error::Io)?;
		}
		// Move cleaned logs back to the pool
		let mut pool = self.log_pool.write();
		pool.extend(cleaned);
		// Sort to reuse lower IDs an prevent IDs from growing.
		pool.make_contiguous().sort_by_key(|(id, _)| *id);
		if pool.len() > MAX_LOG_POOL_SIZE {
			let removed = pool.drain(MAX_LOG_POOL_SIZE..);
			for (id, file) in removed {
				drop(file);
				self.drop_log(id)?;
			}
		}
		Ok(!self.cleanup_queue.read().is_empty())
	}

	pub fn num_dirty_logs(&self) -> usize {
		self.cleanup_queue.read().len()
	}

	pub fn read_next(&self, validate: bool) -> Result<Option<LogReader<'_>>> {
		let mut reading = self.reading.write();
		if reading.is_none() {
			if let Some((id, mut file)) = self.read_queue.write().pop_front() {
				try_io!(file.rewind());
				*reading = Some(Reading { id, file: std::io::BufReader::new(file) });
			} else {
				log::trace!(target: "parity-db", "No active reader");
				return Ok(None)
			}
		}
		let mut reader = LogReader::new(reading, validate);
		match reader.next() {
			Ok(LogAction::BeginRecord) => Ok(Some(reader)),
			Ok(_) => Err(Error::Corruption("Bad log record structure".into())),
			Err(Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
				if let Some(reading) = reader.reading.take() {
					log::debug!(target: "parity-db", "Read: End of log {}", reading.id);
					let file = reading.file.into_inner();
					self.cleanup_queue.write().push_back((reading.id, file));
				}
				Ok(None)
			},
			Err(e) => Err(e),
		}
	}

	pub fn overlays(&self) -> &RwLock<LogOverlays> {
		&self.overlays
	}

	pub fn has_log_files_to_read(&self) -> bool {
		self.read_queue.read().len() > 0
	}

	pub fn kill_logs(&self) -> Result<()> {
		let mut log_pool = self.log_pool.write();
		for (id, file) in log_pool.drain(..) {
			drop(file);
			self.drop_log(id)?;
		}
		if let Some(reading) = self.reading.write().take() {
			drop(reading.file);
			self.drop_log(reading.id)?;
		}
		Ok(())
	}
}
