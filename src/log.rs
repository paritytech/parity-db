// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	column::ColId,
	db::Commit,
	error::{try_io, Error, Result},
	index::TableId as IndexTableId,
	options::Options,
	parking_lot::{RwLock, RwLockWriteGuard},
	table::TableId as ValueTableId,
	Operation,
};
use std::{
	cmp::min,
	collections::VecDeque,
	convert::TryInto,
	io::{ErrorKind, Read, Seek, Write},
	sync::atomic::{AtomicBool, AtomicU32, Ordering},
};

const MAX_LOG_POOL_SIZE: usize = 16;
const BEGIN_RECORD: u8 = 1;
const INSERT_INDEX: u8 = 2;
const INSERT_VALUE: u8 = 3;
const END_RECORD: u8 = 4;
const DROP_TABLE: u8 = 5;
const SET: u8 = 6;
const REFERENCE: u8 = 7;
const DEREFERENCE: u8 = 8;
const COLUMN_OPS: u8 = 9;

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
pub enum LogAction {
	BeginRecord,
	InsertIndex(InsertIndexAction),
	InsertValue(InsertValueAction),
	DropTable(IndexTableId),
	ColumnOps((ColId, u32)),
	EndRecord,
	Set,
	Reference,
	Dereference,
}

#[derive(Debug)]
pub struct LogOverlays {
	last_record_ids: Vec<u64>,
}

impl LogOverlays {
	pub fn last_record_id(&self, col: ColId) -> u64 {
		self.last_record_ids.get(col as usize).cloned().unwrap_or(u64::MAX)
	}

	pub fn with_columns(count: usize) -> Self {
		Self { last_record_ids: (0..count).map(|_| 0).collect() }
	}
}

#[derive(Debug)]
pub struct LogReader<'a> {
	reading: RwLockWriteGuard<'a, Option<Reading>>,
	record_id: u64,
	read_bytes: u64,
	crc32: crc32fast::Hasher,
	validate: bool,
}

impl<'a> LogReader<'a> {
	pub fn record_id(&self) -> u64 {
		self.record_id
	}

	fn new(reading: RwLockWriteGuard<'a, Option<Reading>>, validate: bool) -> LogReader<'a> {
		LogReader {
			reading,
			record_id: 0,
			read_bytes: 0,
			crc32: crc32fast::Hasher::new(),
			validate,
		}
	}

	pub fn reset(&mut self) -> Result<()> {
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
				Ok(LogAction::InsertIndex(InsertIndexAction { table, index }))
			},
			INSERT_VALUE => {
				read_buf(2, &mut buf)?;
				let table =
					ValueTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				read_buf(8, &mut buf)?;
				let index = u64::from_le_bytes(buf);
				Ok(LogAction::InsertValue(InsertValueAction { table, index }))
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
			COLUMN_OPS => {
				read_buf(1, &mut buf)?;
				let col = u8::from_le_bytes(buf[0..1].try_into().unwrap());
				read_buf(4, &mut buf)?;
				let count = u32::from_le_bytes(buf[0..4].try_into().unwrap());
				Ok(LogAction::ColumnOps((col, count)))
			},
			SET => Ok(LogAction::Set),
			REFERENCE => Ok(LogAction::Reference),
			DEREFERENCE => Ok(LogAction::Dereference),
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

	pub fn skip(&mut self, len: usize) -> Result<()> {
		let mut remaining = len;
		if self.validate {
			while remaining != 0 {
				let mut buf = [0u8; 4096];
				let read_to = &mut buf[0..std::cmp::min(remaining, 4096)];
				try_io!(self.reading.as_mut().unwrap().file.read_exact(read_to));
				self.crc32.update(read_to);
				remaining -= read_to.len();
			}
		} else {
			try_io!(self
				.reading
				.as_mut()
				.unwrap()
				.file
				.seek(std::io::SeekFrom::Current(len as i64)));
		}
		self.read_bytes += len as u64;
		Ok(())
	}

	pub fn read_u32(&mut self) -> Result<u32> {
		let mut buf = [0u8; 4];
		self.read(&mut buf)?;
		Ok(u32::from_le_bytes(buf))
	}

	pub fn read_bytes(&self) -> u64 {
		self.read_bytes
	}
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
		for r in overlays.last_record_ids.iter_mut() {
			*r = 0;
		}
		self.dirty.store(false, Ordering::Relaxed);
	}

	pub fn write_commit(&self, commit: &Commit) -> Result<u64> {
		let record_id = commit.id;
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
		let bytes = Self::write_commit_to_file(&mut appending.file, commit)?;
		let mut overlays = self.overlays.write();

		for (c, _) in commit.changeset.btree.iter() {
			overlays.last_record_ids[*c as usize] = commit.id;
		}

		log::debug!(
			target: "parity-db",
			"Finalizing log record {} ({} bytes)",
			record_id,
			bytes,
		);
		appending.size += bytes;
		self.dirty.store(true, Ordering::Relaxed);
		Ok(bytes)
	}

	fn write_commit_to_file(
		file: &mut std::io::BufWriter<std::fs::File>,
		commit: &Commit,
	) -> Result<u64> {
		let mut crc32 = crc32fast::Hasher::new();
		let mut bytes: u64 = 0;

		let mut write = |buf: &[u8]| -> Result<()> {
			try_io!(file.write_all(buf));
			crc32.update(buf);
			bytes += buf.len() as u64;
			Ok(())
		};

		write(&BEGIN_RECORD.to_le_bytes())?;
		write(&commit.id.to_le_bytes())?;

		for (col, changeset) in &commit.changeset.hash_table {
			write(COLUMN_OPS.to_le_bytes().as_ref())?;
			write(col.to_le_bytes().as_ref())?;
			let count = changeset.changes.len() as u32;
			write(&count.to_le_bytes())?;
			for change in &changeset.changes {
				match change {
					Operation::Set(key, value) => {
						write(SET.to_le_bytes().as_ref())?;
						write(key)?;
						let len = value.as_ref().len() as u32;
						write(&len.to_le_bytes())?;
						write(value.as_ref())?;
					},
					Operation::Reference(key) => {
						write(REFERENCE.to_le_bytes().as_ref())?;
						write(key)?;
					},
					Operation::Dereference(key) => {
						write(DEREFERENCE.to_le_bytes().as_ref())?;
						write(key)?;
					},
				}
			}
		}
		for (col, changeset) in &commit.changeset.btree {
			write(COLUMN_OPS.to_le_bytes().as_ref())?;
			write(col.to_le_bytes().as_ref())?;
			let count = changeset.changes.len() as u32;
			write(&count.to_le_bytes())?;
			for change in &changeset.changes {
				match change {
					Operation::Set(key, value) => {
						write(SET.to_le_bytes().as_ref())?;
						let len = key.as_ref().len() as u32;
						write(&len.to_le_bytes())?;
						write(key.as_ref())?;
						let len = value.as_ref().len() as u32;
						write(&len.to_le_bytes())?;
						write(value.as_ref())?;
					},
					Operation::Reference(key) => {
						write(REFERENCE.to_le_bytes().as_ref())?;
						let len = key.as_ref().len() as u32;
						write(&len.to_le_bytes())?;
						write(key.as_ref())?;
					},
					Operation::Dereference(key) => {
						write(DEREFERENCE.to_le_bytes().as_ref())?;
						let len = key.as_ref().len() as u32;
						write(&len.to_le_bytes())?;
						write(key.as_ref())?;
					},
				}
			}
		}

		write(&END_RECORD.to_le_bytes())?;
		let checksum: u32 = crc32.finalize();
		try_io!(file.write_all(&checksum.to_le_bytes()));
		bytes += 4;
		try_io!(file.flush());
		Ok(bytes)
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
