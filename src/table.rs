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
use std::os::unix::fs::FileExt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::{
	error::Result,
	column::ColId,
	log::{Log, LogReader, LogWriter},
	display::hex,
};

pub const KEY_LEN: usize = 32;
const MAX_ENTRY_SIZE: usize = 65534;

const OFFSET_BITS: u8 = 32;
const OFFSET_MASK: u64 = (1u64 << OFFSET_BITS) - 1;
const TOMBSTONE: &[u8] = &[0xff, 0xff];

pub type Key = [u8; KEY_LEN];
pub type Value = Vec<u8>;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TableId(u16);

impl TableId {
	pub fn new(col: ColId, size_tier: u8) -> TableId {
		TableId(((col as u16) << 8) | size_tier as u16)
	}

	pub fn from_u16(id: u16) -> TableId {
		TableId(id)
	}

	pub fn col(&self) -> ColId {
		(self.0 >> 8) as ColId
	}

	pub fn size_tier(&self) -> u8 {
		(self.0 & 0xff) as u8
	}

	pub fn file_name(&self) -> String {
		format!("table_{:02}_{}", self.col(), self.size_tier())
	}

	pub fn as_u16(&self) -> u16 {
		self.0
	}
}

impl std::fmt::Display for TableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "table {:02}_{}", self.col(), self.size_tier())
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Address(u64);

impl Address {
	pub fn new(offset: u64, size_tier: u8) -> Address {
		Address(((size_tier as u64) << OFFSET_BITS) | offset)
	}

	pub fn from_u64(a: u64) -> Address {
		Address(a)
	}

	pub fn offset(&self) -> u64 {
		self.0 & OFFSET_MASK
	}

	pub fn size_tier(&self) -> u8 {
		((self.0 >> OFFSET_BITS) & 0xff) as u8
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

pub struct ValueTable {
	pub id: TableId,
	pub entry_size: u16,
	file: std::fs::File,
	capacity: u64,
	filled: AtomicU64,
	last_removed: AtomicU64,
}

impl ValueTable {
	pub fn open(path: &std::path::Path, id: TableId, entry_size: u16) -> Result<ValueTable> {
		assert!(entry_size >= 64);
		assert!(entry_size <= MAX_ENTRY_SIZE as u16);
		// TODO: posix_fadvise
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(path.as_path())?;
		let mut file_len = file.metadata()?.len();
		if file_len == 0 {
			// Prealocate a single entry that also
			file.set_len(entry_size as u64)?;
			file_len = entry_size as u64;
		}

		let capacity = file_len / entry_size as u64;

		let mut header: [u8; 16] = Default::default();
		file.read_exact_at(&mut header, 0)?;
		let last_removed = u64::from_le_bytes(header[0..8].try_into().unwrap());
		let mut filled = u64::from_le_bytes(header[8..16].try_into().unwrap());
		if filled == 0 {
			filled = 1;
		}
		log::debug!(target: "parity-db", "Opened value table {} with {} entries", id, filled);
		Ok(ValueTable {
			id,
			entry_size,
			file,
			capacity,
			filled: AtomicU64::new(filled),
			last_removed: AtomicU64::new(last_removed),
		})
	}

	pub fn value_size(&self) -> u16 {
		self.entry_size - KEY_LEN as u16
	}

	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		Ok(self.file.read_exact_at(buf, offset)?)
	}

	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		Ok(self.file.write_all_at(buf, offset)?)
	}

	fn grow(&mut self) -> Result<()> {
		self.capacity += (256 * 1024) / self.entry_size as u64;
		self.file.set_len(self.capacity * self.entry_size as u64)?;
		Ok(())
	}

	pub fn get_from_buf(&self, key: &Key, buf: &[u8], index: u64) -> Option<Value> {
		if &buf[0..2] == TOMBSTONE {
			return None;
		}
		let size: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
		if key[2..] != buf[2..32] {
			log::warn!(
				target: "parity-db",
				"{}: Key mismatch at {}. Expected {}, got {}",
				self.id,
				index,
				hex(&key[2..]),
				hex(&buf[2..32]),
			);
			return None;
		}
		Some(buf[32..32 + size as usize].to_vec())
	}

	pub fn get(&self, key: &Key, index: u64, log: &Log) -> Result<Option<Value>> {
		if let Some(val) = log.value_overlay_at(self.id, index) {
			return Ok(self.get_from_buf(key, val, index));
		}
		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		// TODO: read actual size?
		self.read_at(&mut buf[0 .. self.entry_size as usize], index * self.entry_size as u64)?;
		Ok(self.get_from_buf(key, &buf, index))
	}

	pub fn has_key_at(&self, index: u64, key: &Key, log: &LogWriter) -> Result<bool> {
		if let Some(val) = log.value_overlay_at(self.id, index) {
			return Ok(val.len() > 32 && val[2..32] == key[2..32]);
		}
		let mut buf: [u8; 32] = unsafe { MaybeUninit::uninit().assume_init() };
		self.read_at(&mut buf, index * self.entry_size as u64)?;
		Ok(&buf[0..2] != TOMBSTONE && buf[2..32] == key[2..32])
	}

	pub fn partial_key_at(&self, index: u64, log: &LogWriter) -> Result<Key> {
		let mut buf = [0u8; 32];
		if let Some(val) = log.value_overlay_at(self.id, index) {
			buf[2..].copy_from_slice(&val[2..32]);
		} else {
			self.read_at(&mut buf[2..], index * self.entry_size as u64 + 2)?;
		}
		Ok(buf)
	}

	pub fn read_as_empty(&self, index: u64, log: &LogWriter) -> Result<u64> {
		if let Some(val) = log.value_overlay_at(self.id, index) {
			let next = u64::from_le_bytes(val[2..10].try_into().unwrap());
			return Ok(next);
		}
		let mut buf: [u8; 10] = unsafe { MaybeUninit::uninit().assume_init() };
		self.read_at(&mut buf, index * self.entry_size as u64)?;
		let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
		return Ok(next);
	}

	pub fn write_insert_plan(&self, key: &Key, value: &[u8], log: &mut LogWriter) -> Result<u64> {
		let filled = self.filled.load(Ordering::Relaxed);
		let last_removed = self.last_removed.load(Ordering::Relaxed);

		let index = if last_removed != 0 {
			let next_removed = self.read_as_empty(last_removed, log)?;
			log::trace!(
				target: "parity-db",
				"{}: Inserting into removed slot {}: {}",
				self.id,
				last_removed,
				hex(key),
			);
			self.last_removed.store(next_removed, Ordering::Relaxed);
			last_removed
		} else {
			log::trace!(
				target: "parity-db",
				"{}: Inserting into new slot {}: {}",
				self.id,
				filled + 1,
				hex(key),
			);
			self.filled.store(filled + 1, Ordering::Relaxed);
			filled + 1
		};

		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		let len = value.len() as u16;
		buf[0..2].copy_from_slice(&len.to_le_bytes());
		buf[2..32].copy_from_slice(&key[2..]);
		buf[32..32+value.len()].copy_from_slice(value);
		log.insert_value(self.id, index, buf[0..32+value.len()].to_vec());
		Ok(index)
	}

	pub fn write_replace_plan(&self, index: u64, key: &Key, value: &[u8], log: &mut LogWriter) -> Result<()> {
		log::trace!(
			target: "parity-db",
			"{}: Replacing slot {}: {}",
			self.id,
			index,
			hex(key),
		);
		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		let len = value.len() as u16;
		buf[0..2].copy_from_slice(&len.to_le_bytes());
		buf[2..32].copy_from_slice(&key[2..]);
		buf[32..32+value.len()].copy_from_slice(value);
		log.insert_value(self.id, index, buf[0..32+value.len()].to_vec());
		Ok(())
	}

	pub fn write_remove_plan(&self, index: u64, log: &mut LogWriter) -> Result<()> {
		let last_removed = self.last_removed.load(Ordering::Relaxed);
		log::trace!(
			target: "parity-db",
			"{}: Freeing slot {}",
			self.id,
			index,
		);
		let mut buf = [0u8; 10];
		&buf[0..2].copy_from_slice(TOMBSTONE);
		&buf[2..10].copy_from_slice(&last_removed.to_le_bytes());

		log.insert_value(self.id, index, buf.to_vec());
		self.last_removed.store(index, Ordering::Relaxed);
		Ok(())
	}

	pub fn enact_plan(&mut self, index: u64, log: &mut LogReader) -> Result<()> {
		while index > self.capacity {
			self.grow()?;
		}

		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		log.read(&mut buf[0..2])?;
		if &buf[0..2] == TOMBSTONE {
			log.read(&mut buf[2..10])?;
			self.write_at(&buf[0..10], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted tombstone in slot {}", self.id, index);
		} else {
			let len: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
			log.read(&mut buf[2..32+len as usize])?;
			self.write_at(&buf[0..(32 + len as usize)], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted {}: {}, {} bytes", self.id, index, hex(&buf[2..32]), len);
		}
		Ok(())
	}

	pub fn complete_plan(&mut self) -> Result<()> {
		let mut buf = [0u8; 16];
		let last_removed = self.last_removed.load(Ordering::Relaxed);
		let filled = self.filled.load(Ordering::Relaxed);
		buf[0..8].copy_from_slice(&last_removed.to_le_bytes());
		buf[8..16].copy_from_slice(&filled.to_le_bytes());
		self.write_at(&buf, 0)?;
		Ok(())
	}
}


