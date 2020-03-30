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
use crate::{
	error::{Error, Result},
	bucket::{BucketId}
};

pub struct InsertAction {
	pub bucket: BucketId,
	pub index: u64,
}

pub enum LogAction {
	BeginRecord(u64),
	Insert(InsertAction),
	DropBucket(BucketId),
	EndRecord, // TODO: crc32
}

pub struct LogReader<'a> {
	file: &'a mut std::io::BufReader<std::fs::File>,
	overlays: &'a mut HashMap<BucketId, LogOverlay>,
	record_id: u64,
}

impl<'a> LogReader<'a> {
	pub fn record_id(&self) -> u64 {
		self.record_id
	}

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

	pub fn next(&mut self) -> Result<LogAction> {
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
				let bucket = BucketId::from_u32(u32::from_le_bytes(buf[0..4].try_into().unwrap()));
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
				let bucket = BucketId::from_u32(u32::from_le_bytes(buf[0..4].try_into().unwrap()));
				Ok(LogAction::DropBucket(bucket))
			}
			_ => Err(Error::Corruption("Bad log entry type".into()))
		}
	}

	pub fn read(&mut self, buf: &mut [u8]) -> Result<()> {
		Ok(self.file.read_exact(buf)?)
	}
}

pub struct LogWriter<'a> {
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

	pub fn record_id(&self) -> u64 {
		self.record_id
	}

	pub fn begin_record(&mut self, id: u64) -> Result<()> {
		self.file.write(&1u8.to_le_bytes())?;
		self.file.write(&id.to_le_bytes())?;
		Ok(())
	}

	pub fn insert(&mut self, bucket: BucketId, index: u64, data: Vec<u8>) -> Result<()> {
		self.file.write(&2u8.to_le_bytes().as_ref())?;
		self.file.write(&bucket.as_u32().to_le_bytes())?;
		self.file.write(&index.to_le_bytes())?;
		self.file.write(&data)?;
		self.overlays.entry(bucket).or_default().map.insert(index, (self.record_id, data));
		Ok(())
	}

	pub fn end_record(&mut self) -> Result<()> {
		self.file.write(&3u8.to_le_bytes())?;
		self.file.flush()?;
		Ok(())
	}

	pub fn drop_bucket(&mut self, id: BucketId) -> Result<()> {
		self.file.write(&4u8.to_le_bytes())?;
		self.file.write(&id.as_u32().to_le_bytes())?;
		Ok(())
	}

	pub fn overlay_at(&self, bucket: BucketId, index: u64) -> Option<&[u8]> {
		self.overlays.get(&bucket).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
	}
}

#[derive(Default)]
struct LogOverlay {
	map: HashMap<u64, (u64, Vec<u8>)>, // index -> (record_id, entry)
}

pub struct Log {
	//records: Vec<LogRecord>,
	overlays: HashMap<BucketId, LogOverlay>,
	appending: std::io::BufWriter<std::fs::File>,
	flushing: std::io::BufReader<std::fs::File>,
	record_id: u64,
	appending_empty: bool,
	dirty: bool,
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

	pub fn begin_record<'a>(&'a mut self) -> Result<LogWriter<'a>> {
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

	pub fn flush_one<'a>(&'a mut self) -> Result<Option<LogReader<'a>>> {
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

	pub fn overlay_at(&self, bucket: BucketId, index: u64) -> Option<&[u8]> {
		self.overlays.get(&bucket).and_then(|o| o.map.get(&index).map(|(_id, data)| data.as_ref()))
	}
}

