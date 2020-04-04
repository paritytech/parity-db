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

use std::collections::HashMap;
use std::io::{Read, Write, Seek};
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use crate::{
	error::{Error, Result},
	table::TableId as ValueTableId,
	index::{TableId as IndexTableId, Chunk as IndexChunk},
};

pub struct InsertIndexAction {
	pub table: IndexTableId,
	pub index: u64,
}

pub struct InsertValueAction {
	pub table: ValueTableId,
	pub index: u64,
}

pub enum LogAction {
	BeginRecord(u64),
	InsertIndex(InsertIndexAction),
	InsertValue(InsertValueAction),
	DropTable(IndexTableId),
	EndRecord, // TODO: crc32
}

pub trait LogQuery {
	fn index(&self, table: IndexTableId, index: u64) -> Option<&IndexChunk>;
	fn value(&self, table: ValueTableId, index: u64) -> Option<&[u8]>;
}

#[derive(Default)]
pub struct LogOverlays {
	index: HashMap<IndexTableId, IndexLogOverlay>,
	value: HashMap<ValueTableId, ValueLogOverlay>,
}

impl LogQuery for LogOverlays {
	fn index(&self, table: IndexTableId, index: u64) -> Option<&IndexChunk> {
		self.index.get(&table).and_then(|o| o.map.get(&index).map(|(_id, data)| data))
	}

	fn value(&self, table: ValueTableId, index: u64) -> Option<&[u8]> {
		self.value.get(&table).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
	}
}

#[derive(Default)]
pub struct Cleared {
	index: Vec<(IndexTableId, u64)>,
	values: Vec<(ValueTableId, u64)>,
}

pub struct LogReader<'a> {
	file: RwLockWriteGuard<'a, std::io::BufReader<std::fs::File>>,
	record_id: u64,
	cleared: Cleared,
}

impl<'a> LogReader<'a> {
	pub fn record_id(&self) -> u64 {
		self.record_id
	}

	fn new(
		file: RwLockWriteGuard<'a, std::io::BufReader<std::fs::File>>,
		record_id: u64,
	) -> LogReader<'a> {
		LogReader {
			cleared: Default::default(),
			file,
			record_id,
		}
	}

	pub fn next(&mut self) -> Result<LogAction> {
		let mut buf = [0u8; 8];
		self.file.read_exact(&mut buf[0..1])?;
		match buf[0] {
			1 =>  { // BeginRecord
				self.file.read_exact(&mut buf[0..8])?;
				let record_id = u64::from_le_bytes(buf);
				self.record_id = record_id;
				//log::trace!(target: "parity-db", "Read record header {}", record_id);
				Ok(LogAction::BeginRecord(record_id))
			},
			2 => { // InsertIndex
				self.file.read_exact(&mut buf[0..2])?;
				let table = IndexTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				self.file.read_exact(&mut buf[0..8])?;
				let index = u64::from_le_bytes(buf);
				//log::trace!(target: "parity-db", "Read log index {}:{}", table, index);
				self.cleared.index.push((table, index));
				Ok(LogAction::InsertIndex(InsertIndexAction { table, index }))
			},
			3 => { // InsertValue
				self.file.read_exact(&mut buf[0..2])?;
				let table = ValueTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				self.file.read_exact(&mut buf[0..8])?;
				let index = u64::from_le_bytes(buf);
				self.cleared.values.push((table, index));
				//log::trace!(target: "parity-db", "Read log value {}:{}", table, index);
				Ok(LogAction::InsertValue(InsertValueAction { table, index }))
			},
			4 => {
				log::trace!(target: "parity-db", "Read log end");
				Ok(LogAction::EndRecord)
			},
			5 => {
				self.file.read_exact(&mut buf[0..2])?;
				let table = IndexTableId::from_u16(u16::from_le_bytes(buf[0..2].try_into().unwrap()));
				//log::trace!(target: "parity-db", "Read drop table {}", table);
				Ok(LogAction::DropTable(table))
			}
			_ => Err(Error::Corruption("Bad log entry type".into()))
		}
	}

	pub fn read(&mut self, buf: &mut [u8]) -> Result<()> {
		Ok(self.file.read_exact(buf)?)
	}

	pub fn drain(self) -> Cleared {
		self.cleared
	}
}

pub struct LogChange {
	local_index: HashMap<IndexTableId, IndexLogOverlay>,
	local_values: HashMap<ValueTableId, ValueLogOverlay>,
	record_id: u64,
	dropped_tables: Vec<IndexTableId>,
}

impl LogChange {
	fn new(
		record_id: u64,
	) -> LogChange {
		LogChange {
			local_index: Default::default(),
			local_values: Default::default(),
			dropped_tables: Default::default(),
			record_id,
		}
	}

	pub fn to_file(self, file: &mut std::io::BufWriter<std::fs::File>)
		-> Result<(HashMap<IndexTableId, IndexLogOverlay>, HashMap<ValueTableId, ValueLogOverlay>)>
	{
		file.write(&1u8.to_le_bytes())?; // Begin record
		file.write(&self.record_id.to_le_bytes())?;

		for (id, overlay) in self.local_index.iter() {
			for (index, (_, chunk)) in overlay.map.iter() {
				//log::trace!(target: "parity-db", "Finalizing log index {}:{}", id, index);
				file.write(&2u8.to_le_bytes().as_ref())?;
				file.write(&id.as_u16().to_le_bytes())?;
				file.write(&index.to_le_bytes())?;
				file.write(chunk)?;
			}
		}
		for (id, overlay) in self.local_values.iter() {
			for (index, (_, value)) in overlay.map.iter() {
				//log::trace!(target: "parity-db", "Finalizing log value {}:{} ({} bytes)", id, index, value.len());
				file.write(&3u8.to_le_bytes().as_ref())?;
				file.write(&id.as_u16().to_le_bytes())?;
				file.write(&index.to_le_bytes())?;
				file.write(value)?;
			}
		}
		for id in self.dropped_tables.iter() {
			log::debug!(target: "parity-db", "Finalizing drop {}", id);
			file.write(&5u8.to_le_bytes().as_ref())?;
			file.write(&id.as_u16().to_le_bytes())?;
		}

		file.write(&4u8.to_le_bytes())?; // End record
		file.flush()?;
		Ok((self.local_index, self.local_values))
	}
}

pub struct LogWriter<'a> {
	overlays: RwLockReadGuard<'a, LogOverlays>,
	log: LogChange,
}

impl<'a> LogWriter<'a> {
	fn new(
		overlays: RwLockReadGuard<'a, LogOverlays>,
		record_id: u64,
	) -> LogWriter<'a> {
		LogWriter {
			overlays,
			log: LogChange::new(record_id),
		}
	}

	pub fn record_id(&self) -> u64 {
		self.log.record_id
	}

	pub fn insert_index(&mut self, table: IndexTableId, index: u64, data: &IndexChunk) {
		self.log.local_index.entry(table).or_default().map.insert(index, (self.log.record_id, data.clone()));
	}

	pub fn insert_value(&mut self, table: ValueTableId, index: u64, data: Vec<u8>) {
		self.log.local_values.entry(table).or_default().map.insert(index, (self.log.record_id, data.clone()));
	}

	pub fn drop_table(&mut self, id: IndexTableId) {
		self.log.dropped_tables.push(id);
	}

	pub fn drain(self) -> LogChange {
		self.log
	}
}

impl<'a> LogQuery for LogWriter<'a> {
	fn index(&self, table: IndexTableId, index: u64) -> Option<&IndexChunk> {
		self.log.local_index.get(&table).and_then(|o| o.map.get(&index).map(|(_id, data)| data))
			.or_else(|| self.overlays.index(table, index))
	}

	fn value(&self, table: ValueTableId, index: u64) -> Option<&[u8]> {
		self.log.local_values.get(&table).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
			.or_else(|| self.overlays.value(table, index))
	}
}

#[derive(Default)]
pub struct IndexLogOverlay {
	pub map: HashMap<u64, (u64, IndexChunk)>, // index -> (record_id, entry)
}


#[derive(Default)]
pub struct ValueLogOverlay {
	pub map: HashMap<u64, (u64, Vec<u8>)>, // index -> (record_id, entry)
}

struct Appending {
	file: std::io::BufWriter<std::fs::File>,
	empty: bool,
}

pub struct Log {
	overlays: RwLock<LogOverlays>,
	appending: RwLock<Appending>,
	flushing: RwLock<std::io::BufReader<std::fs::File>>,
	record_id: AtomicU64,
	dirty: AtomicBool,
}

impl Log {
	pub fn open(path: &std::path::Path) -> Result<Log> {
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

		Ok(Log {
			overlays: Default::default(),
			appending: RwLock::new(Appending {
				file: std::io::BufWriter::new(appending),
				empty: false,
			}),
			flushing: RwLock::new(std::io::BufReader::new(flushing)),
			record_id: AtomicU64::new(record_id),
			dirty: AtomicBool::new(true),
		})
	}

	pub fn open_or_create_log_file(path: &std::path::Path) -> Result<(std::fs::File, Option<u64>)> {
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

	pub fn begin_record<'a>(&'a self) -> LogWriter<'a> {
		let id = self.record_id.fetch_add(1, Ordering::AcqRel);
		if id == 0 {
			panic!("Not initalized.");
		}
		let writer = LogWriter::new(
			self.overlays.read(),
			id
		);
		writer
	}

	pub fn end_record(&self, log: LogChange) -> Result<()> {
		log::trace!(target: "parity-db", "Finalizing log record {}", log.record_id);
		assert!(log.record_id + 1 == self.record_id.load(Ordering::Acquire));
		let mut appending = self.appending.write();
		let (index, values) = log.to_file(&mut appending.file)?;
		let mut overlays = self.overlays.write();
		for (id, overlay) in index.into_iter() {
			overlays.index.entry(id).or_default().map.extend(overlay.map.into_iter());
		}
		for (id, overlay) in values.into_iter() {
			overlays.value.entry(id).or_default().map.extend(overlay.map.into_iter());
		}
		appending.empty = false;
		self.dirty.store(true, Ordering::Relaxed);
		Ok(())
	}

	pub fn end_read(&self, cleared: Cleared, record_id: u64) {
		let mut overlays = self.overlays.write();
		for (table, index) in cleared.index.into_iter() {
			if let Some(ref mut overlay) = overlays.index.get_mut(&table) {
				match overlay.map.entry(index) {
					std::collections::hash_map::Entry::Occupied(e) => {
						if e.get().0 == record_id {
							e.remove_entry();
						}
					}
					_ => {},
				}
			}
		}
		for (table, index) in cleared.values.into_iter() {
			if let Some(ref mut overlay) = overlays.value.get_mut(&table) {
				match overlay.map.entry(index) {
					std::collections::hash_map::Entry::Occupied(e) => {
						if e.get().0 == record_id {
							e.remove_entry();
						}
					}
					_ => {},
				}
			}
		}
	}

	pub fn flush_one<'a>(&'a self) -> Result<Option<LogReader<'a>>> {
		if !self.dirty.load(Ordering::Relaxed) {
			log::trace!(target: "parity-db", "No logs to enact");
			return Ok(None);
		}

		let flushing = self.flushing.write();
		let mut reader = LogReader::new(
			flushing,
			0
		);
		match reader.next() {
			Ok(LogAction::BeginRecord(_)) => {
				return Ok(Some(reader));
			}
			Ok(_) => return Err(Error::Corruption("Bad log record structure".into())),
			Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
				{
					let mut appending = self.appending.write();
					if appending.empty {
						log::trace!(target: "parity-db", "End of log");
						self.dirty.store(false, Ordering::Relaxed);
						return Ok(None);
					}
					log::debug!(
						target: "parity-db",
						"Switching log files",
					);
					appending.empty = true;
					let mut flushing = reader.file;
					// Switch files
					appending.file.flush()?;
					std::mem::swap(appending.file.get_mut(), flushing.get_mut());
					appending.file.get_mut().set_len(0)?;
					appending.file.seek(std::io::SeekFrom::Start(0))?;
					flushing.seek(std::io::SeekFrom::Start(0))?;
				}
				//self.flushing.get_mut().sync_data()?;
				return self.flush_one();
			}
			Err(e) => return Err(e),
		};
	}

	pub fn overlays<'a>(&'a self) -> RwLockReadGuard<'a, LogOverlays> {
		self.overlays.read()
	}
}

