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

// On disk data layout for value tables.
//
// Entry 0 (metadata)
// [LAST_REMOVED: 8][FILLED: 8]
// LAST_REMOVED - 64-bit index of removed entries linked list head
// FILLED - highest index filled with live data
//
// Complete entry:
// [SIZE: 2][REFS: 4][KEY: 26][VALUE: SIZE - 30]
// SIZE: 16-bit value size. Sizes up to 0xfffd are allowed.
// This includes size of REFS and KEY
// REF: 31-bit reference counter, first bit reserved to flag an applied compression.
// KEY: lower 26 bytes of the key.
// VALUE: SIZE-30  payload bytes.
//
// Partial entry (first part):
// [MULTIPART: 2][NEXT: 8][REFS: 4][KEY: 26][VALUE]
// MULTIPART - Split entry marker. 0xfffe.
// NEXT - 64-bit index of the entry that holds the next part.
// take all available space in this entry.
// REF: 31-bit reference counter, first bit reserved to flag an applied compression.
// KEY: lower 26 bytes of the key.
// VALUE: The rest of the entry is filled with payload bytes.
//
// Partial entry (continuation):
// [MULTIPART: 2][NEXT: 8][VALUE]
// MULTIPART - Split entry marker. 0xfffe.
// NEXT - 64-bit index of the entry that holds the next part.
// VALUE: The rest of the entry is filled with payload bytes.
//
// Partial entry (last part):
// [SIZE: 2][VALUE: SIZE]
// SIZE: 16-bit size of the remaining payload.
// VALUE: SIZE payload bytes.
//
// Deleted entry
// [TOMBSTONE: 2][NEXT: 8]
// TOMBSTONE - Deleted entry marker. 0xffff
// NEXT - 64-bit index of the next deleted entry.


use std::convert::TryInto;
use std::mem::MaybeUninit;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crate::{
	error::Result,
	column::ColId,
	log::{LogQuery, LogReader, LogWriter},
	display::hex,
};

pub const KEY_LEN: usize = 32;
pub const SIZE_TIERS: usize = 16;
pub const SIZE_TIERS_BITS: u8 = 4;
const MAX_ENTRY_SIZE: usize = 0xfffd;

const TOMBSTONE: &[u8] = &[0xff, 0xff];
const MULTIPART: &[u8] = &[0xff, 0xfe];
const COMPRESSED_MASK: u32 = 0xa0_00_00_00;
// When a rc reach locked ref, it is locked in db.
const LOCKED_REF: u32 = 0x7fff_ffff;


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

pub struct ValueTable {
	pub id: TableId,
	pub entry_size: u16,
	file: std::fs::File,
	capacity: AtomicU64,
	filled: AtomicU64,
	last_removed: AtomicU64,
	dirty_header: AtomicBool,
	multipart: bool,
}

#[cfg(target_os = "macos")]
fn disable_read_ahead(file: &std::fs::File) -> Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, 0) } != 0 {
		Err(std::io::Error::last_os_error())?
	} else {
		Ok(())
	}
}

#[cfg(not(target_os = "macos"))]
fn disable_read_ahead(_file: &std::fs::File) -> Result<()> {
	Ok(())
}

impl ValueTable {
	pub fn open(path: &std::path::Path, id: TableId, entry_size: Option<u16>) -> Result<ValueTable> {
		let (multipart, entry_size) = match entry_size {
			Some(s) => (false, s),
			None => (true, 4096),
		};
		assert!(entry_size >= 64);
		assert!(entry_size <= MAX_ENTRY_SIZE as u16);
		// TODO: posix_fadvise
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let mut file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(path.as_path())?;
		disable_read_ahead(&file)?;
		let mut file_len = file.metadata()?.len();
		if file_len == 0 {
			// Prealocate a single entry that contains metadata
			file.set_len(entry_size as u64)?;
			file_len = entry_size as u64;
		}

		let capacity = file_len / entry_size as u64;
		let mut header: [u8; 16] = Default::default();
		file.read_exact(&mut header)?;
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
			capacity: AtomicU64::new(capacity),
			filled: AtomicU64::new(filled),
			last_removed: AtomicU64::new(last_removed),
			dirty_header: AtomicBool::new(false),
			multipart,
		})
	}

	pub fn value_size(&self) -> u16 {
		self.entry_size - KEY_LEN as u16
	}

	#[cfg(unix)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		Ok(self.file.read_exact_at(buf, offset)?)
	}

	#[cfg(unix)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		Ok(self.file.write_all_at(buf, offset)?)
	}

	#[cfg(windows)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.file.seek_read(buf, offset)?;
		Ok(())
	}

	#[cfg(windows)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.file.seek_write(buf, offset)?;
		Ok(())
	}

	fn grow(&self) -> Result<()> {
		let mut capacity = self.capacity.load(Ordering::Relaxed);
		capacity += (256 * 1024) / self.entry_size as u64;
		self.capacity.store(capacity, Ordering::Relaxed);
		self.file.set_len(capacity * self.entry_size as u64)?;
		Ok(())
	}

	// Return if there was content, and if it was compressed.
	pub fn for_parts<Q: LogQuery, F: FnMut(&[u8])>(
		&self,
		key: &Key,
		mut index: u64,
		log: &Q,
		mut f: F,
	) -> Result<(bool, bool)> {
		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };

		let mut part = 0;
		let mut compressed = false;
		let entry_size = self.entry_size as usize;
		loop {
			let buf = if log.value(self.id, index, &mut buf) {
				&buf
			} else {
				log::trace!(
					target: "parity-db",
					"{}: Query slot {}",
					self.id,
					index,
				);
				self.read_at(&mut buf[0..entry_size], index * self.entry_size as u64)?;
				&buf[0..entry_size]
			};

			if &buf[0..2] == TOMBSTONE {
				return Ok((false, false));
			}

			let (content_offset, content_len, next) = if &buf[0..2] == MULTIPART {
				let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
				(10, entry_size - 10, next)
			} else {
				let size: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
				(2, size as usize, 0)
			};

			if part == 0 {
				let rc = u32::from_le_bytes(buf[content_offset..content_offset + 4].try_into().unwrap());
				compressed = rc & COMPRESSED_MASK > 0;
				if key[6..] != buf[content_offset + 4..content_offset + 30] {
					log::debug!(
						target: "parity-db",
						"{}: Key mismatch at {}. Expected {}, got {}",
						self.id,
						index,
						hex(&key[6..]),
						hex(&buf[content_offset + 4..content_offset + 30]),
					);
					return Ok((false, false));
				}
				f(&buf[content_offset + 30..content_offset + content_len]);
			} else {
				f(&buf[content_offset..content_offset + content_len]);
			}
			if next == 0 {
				break;
			}
			part += 1;
			index = next;
		}
		Ok((true, compressed))
	}

	pub fn get(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(Value, bool)>> {
		let mut result = Vec::new();
		let (success, compressed) = self.for_parts(key, index, log, |buf| result.extend_from_slice(buf))?;
		if success {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn size(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(u32, bool)>> {
		let mut result = 0;
		let (success, compressed) = self.for_parts(key, index, log, |buf| result += buf.len() as u32)? ;
		if success {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn has_key_at(&self, index: u64, key: &Key, log: &LogWriter) -> Result<bool> {
		Ok(match self.partial_key_at(index, log)? {
			Some(existing_key) => &existing_key[6..] == &key [6..],
			None => false,
		})
	}

	pub fn partial_key_at<Q: LogQuery>(&self, index: u64, log: &Q) -> Result<Option<Key>> {
		let mut buf = [0u8; 40];
		let mut result = Key::default();
		let buf = if log.value(self.id, index, &mut buf) {
			&buf
		} else {
			self.read_at(&mut buf[0..40], index * self.entry_size as u64)?;
			&buf
		};
		if &buf[0..2] == TOMBSTONE {
			return Ok(None);
		}
		if &buf[0..2] == MULTIPART {
			result[6..].copy_from_slice(&buf[14..40]);
		} else {
			result[6..].copy_from_slice(&buf[6..32]);
		}
		Ok(Some(result))
	}

	pub fn read_next_free(&self, index: u64, log: &LogWriter) -> Result<u64> {
		let mut buf: [u8; 10] = unsafe { MaybeUninit::uninit().assume_init() };
		if log.value(self.id, index, &mut buf) {
			let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
			return Ok(next);
		}
		self.read_at(&mut buf, index * self.entry_size as u64)?;
		let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
		return Ok(next);
	}

	pub fn read_next_part(&self, index: u64, log: &LogWriter) -> Result<Option<u64>> {
		let mut buf: [u8; 10] = unsafe { MaybeUninit::uninit().assume_init() };
		if log.value(self.id, index, &mut buf) {
			if &buf[0..2] == MULTIPART {
				let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
				return Ok(Some(next));
			}
			return Ok(None);
		}
		self.read_at(&mut buf, index * self.entry_size as u64)?;
		if &buf[0..2] == MULTIPART {
			let next = u64::from_le_bytes(buf[2..10].try_into().unwrap());
			return Ok(Some(next));
		}
		return Ok(None);
	}

	pub fn next_free(&self, log: &mut LogWriter) -> Result<u64> {
		let filled = self.filled.load(Ordering::Relaxed);
		let last_removed = self.last_removed.load(Ordering::Relaxed);
		let index = if last_removed != 0 {
			let next_removed = self.read_next_free(last_removed, log)?;
			log::trace!(
				target: "parity-db",
				"{}: Inserting into removed slot {}",
				self.id,
				last_removed,
			);
			self.last_removed.store(next_removed, Ordering::Relaxed);
			last_removed
		} else {
			log::trace!(
				target: "parity-db",
				"{}: Inserting into new slot {}",
				self.id,
				filled,
			);
			self.filled.store(filled + 1, Ordering::Relaxed);
			filled
		};
		self.dirty_header.store(true, Ordering::Relaxed);
		Ok(index)
	}

	fn overwrite_chain(&self, key: &Key, value: &[u8], log: &mut LogWriter, at: Option<u64>, compressed: bool) -> Result<u64> {
		let mut remainder = value.len() + 30; // Prefix with key and ref counter
		let mut offset = 0;
		let mut start = 0;
		assert!(self.multipart || value.len() <= self.value_size() as usize);
		let (mut index, mut follow) = match at {
			Some(index) => (index, true),
			None => (self.next_free(log)?, false)
		};
		loop {
			if start == 0 {
				start = index;
			}

			let mut next_index = 0;
			if follow {
				// check existing link
				match self.read_next_part(index, log)? {
					Some(next) => {
						next_index = next;
					}
					None => {
						follow = false;
					}
				}
			}
			log::trace!(
				target: "parity-db",
				"{}: Writing slot {}: {}",
				self.id,
				index,
				hex(key),
			);
			let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
			let free_space = self.entry_size as usize - 2;
			let (target_offset, value_len) = if remainder > free_space {
				if !follow {
					next_index = self.next_free(log)?
				}
				buf[0..2].copy_from_slice(&MULTIPART);
				buf[2..10].copy_from_slice(&(next_index as u64).to_le_bytes());
				(10, free_space - 8)
			} else {
				buf[0..2].copy_from_slice(&(remainder as u16).to_le_bytes());
				(2, remainder)
			};
			if offset == 0 {
				// first rc.
				let rc = if compressed {
					1u32 | COMPRESSED_MASK
				} else {
					1u32
				};
				buf[target_offset..target_offset + 4].copy_from_slice(&rc.to_le_bytes());
				buf[target_offset + 4..target_offset + 30].copy_from_slice(&key[6..]);
				buf[target_offset + 30..target_offset + value_len]
					.copy_from_slice(&value[offset..offset + value_len - 30]);
				offset += value_len - 30;
			} else {
				buf[target_offset..target_offset + value_len].copy_from_slice(&value[offset..offset + value_len]);
				offset += value_len;
			}
			log.insert_value(self.id, index, buf[0..target_offset + value_len].to_vec());
			remainder -= value_len;
			index = next_index;
			if remainder == 0 {
				if index != 0 {
					// End of new entry. Clear the remaining tail and exit
					self.clear_chain(index, log)?;
				}
				break;
			}
		}

		Ok(start)
	}

	fn clear_chain(&self, mut index: u64, log: &mut LogWriter) -> Result<()> {
		loop {
			match self.read_next_part(index, log)? {
				Some(next) => {
					self.clear_slot(index, log)?;
					index = next;
				}
				None => {
					self.clear_slot(index, log)?;
					return Ok(());
				}
			}
		}
	}

	fn clear_slot(&self, index: u64, log: &mut LogWriter) -> Result<()> {
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
		self.dirty_header.store(true, Ordering::Relaxed);
		Ok(())
	}

	pub fn write_insert_plan(&self, key: &Key, value: &[u8], log: &mut LogWriter, compressed: bool) -> Result<u64> {
		self.overwrite_chain(key, value, log, None, compressed)
	}

	pub fn write_replace_plan(&self, index: u64, key: &Key, value: &[u8], log: &mut LogWriter, compressed: bool) -> Result<()> {
		self.overwrite_chain(key, value, log, Some(index), compressed)?;
		Ok(())
	}

	pub fn write_remove_plan(&self, index: u64, log: &mut LogWriter) -> Result<()> {
		if self.multipart {
			self.clear_chain(index, log)?;
		} else {
			self.clear_slot(index, log)?;
		}
		Ok(())
	}

	pub fn write_inc_ref(&self, index: u64, log: &mut LogWriter, compressed: bool) -> Result<()> {
		self.change_ref(index, 1, log, Some(compressed))?;
		Ok(())
	}

	pub fn write_dec_ref(&self, index: u64, log: &mut LogWriter) -> Result<bool> {
		if self.change_ref(index, -1, log, None)? {
			return Ok(true);
		}
		self.write_remove_plan(index, log)?;
		Ok(false)
	}

	pub fn change_ref(&self, index: u64, delta: i32, log: &mut LogWriter, compressed: Option<bool>) -> Result<bool> {
		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		let buf = if log.value(self.id, index, &mut buf) {
			&mut buf
		} else {
			self.read_at(&mut buf[0..self.entry_size as usize], index * self.entry_size as u64)?;
			&mut buf[0..self.entry_size as usize]
		};

		if &buf[0..2] == TOMBSTONE {
			return Ok(false);
		}

		let (counter_offset, size) = if &buf[0..2] == MULTIPART {
			(10, self.entry_size as usize)
		} else {
			let size: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
			(2, (size + 2) as usize)
		};

		let mut counter: u32 = u32::from_le_bytes(buf[counter_offset..counter_offset + 4].try_into().unwrap());
		debug_assert!(compressed.map(|compressed| if counter & COMPRESSED_MASK > 0 {
				compressed == true
			} else {
				compressed == false
			}).unwrap_or(true)
		);

		let compressed = counter & COMPRESSED_MASK > 0;
		counter = counter & !COMPRESSED_MASK;
		if delta > 0 {
			if counter >= LOCKED_REF - delta as u32 {
				counter = LOCKED_REF
			} else {
				counter = counter + delta as u32;
			}
		} else {
			if counter != LOCKED_REF {
				counter = counter.saturating_sub((delta * -1) as u32);
				if counter == 0 {
					return Ok(false);
				}
			}
		}
		counter = if compressed {
			counter | COMPRESSED_MASK
		} else {
			LOCKED_REF
		};

		buf[counter_offset..counter_offset + 4].copy_from_slice(&counter.to_le_bytes());
		// TODO: optimize actual buf size
		log.insert_value(self.id, index, buf[0..size].to_vec());
		return Ok(true);
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		while index >= self.capacity.load(Ordering::Relaxed) {
			self.grow()?;
		}
		if index == 0 {
			let mut header = [0u8; 16];
			log.read(&mut header)?;
			self.write_at(&header, 0)?;
			return Ok(());
		}

		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		log.read(&mut buf[0..2])?;
		if &buf[0..2] == TOMBSTONE {
			log.read(&mut buf[2..10])?;
			self.write_at(&buf[0..10], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted tombstone in slot {}", self.id, index);
		} else if &buf[0..2] == MULTIPART {
				let entry_size = self.entry_size as usize;
				log.read(&mut buf[2..entry_size])?;
				self.write_at(&buf[0..entry_size], index * (entry_size as u64))?;
				log::trace!(target: "parity-db", "{}: Enacted multipart in slot {}", self.id, index);
		} else {
			let len: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
			log.read(&mut buf[2..2+len as usize])?;
			self.write_at(&buf[0..(2 + len as usize)], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted {}: {}, {} bytes", self.id, index, hex(&buf[6..32]), len);
		}
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index == 0 {
			let mut header = [0u8; 16];
			log.read(&mut header)?;
			// TODO: sanity check last_removed and filled
			return Ok(());
		}
		let mut buf: [u8; MAX_ENTRY_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
		log.read(&mut buf[0..2])?;
		if &buf[0..2] == TOMBSTONE {
			log.read(&mut buf[2..10])?;
			log::trace!(target: "parity-db", "{}: Validated tombstone in slot {}", self.id, index);
		}
		else if &buf[0..2] == MULTIPART {
			let entry_size = self.entry_size as usize;
			log.read(&mut buf[2..entry_size])?;
			log::trace!(target: "parity-db", "{}: Validated multipart in slot {}", self.id, index);
		} else {
			// TODO: check len
			let len: u16 = u16::from_le_bytes(buf[0..2].try_into().unwrap());
			log.read(&mut buf[2..2 + len as usize])?;
			log::trace!(target: "parity-db", "{}: Validated {}: {}, {} bytes", self.id, index, hex(&buf[2..32]), len);
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		let mut header: [u8; 16] = Default::default();
		self.read_at(&mut header, 0)?;
		let last_removed = u64::from_le_bytes(header[0..8].try_into().unwrap());
		let mut filled = u64::from_le_bytes(header[8..16].try_into().unwrap());
		if filled == 0 {
			filled = 1;
		}
		self.last_removed.store(last_removed, Ordering::Relaxed);
		self.filled.store(filled, Ordering::Relaxed);
		Ok(())
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		if let Ok(true) = self.dirty_header.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
			// last_removed or filled pointers were modified. Add them to the log
			let mut buf = [0u8; 16];
			let last_removed = self.last_removed.load(Ordering::Relaxed);
			let filled = self.filled.load(Ordering::Relaxed);
			buf[0..8].copy_from_slice(&last_removed.to_le_bytes());
			buf[8..16].copy_from_slice(&filled.to_le_bytes());
			log.insert_value(self.id, 0, buf.to_vec());
		}
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		self.file.sync_data()?;
		Ok(())
	}
}

#[cfg(test)]
mod test {
	const ENTRY_SIZE: u16 = 64;
	use super::{ValueTable, TableId, Key, Value};
	use crate::{log::{Log, LogWriter, LogAction}, options::Options};

	struct TempDir(std::path::PathBuf);

	impl TempDir {
		fn new(name: &'static str) -> TempDir {
			env_logger::try_init().ok();
			let mut path = std::env::temp_dir();
			path.push("parity-db-test");
			path.push("value-table");
			path.push(name);

			if path.exists() {
				std::fs::remove_dir_all(&path).unwrap();
			}
			std::fs::create_dir_all(&path).unwrap();
			TempDir(path)
		}

		fn table(&self, size: Option<u16>) -> ValueTable {
			let id = TableId::new(0, 0);
			ValueTable::open(&self.0, id, size).unwrap()
		}

		fn log(&self) -> Log {
			let options = Options::with_columns(&self.0, 1);
			Log::open(&options).unwrap()
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			if self.0.exists() {
				std::fs::remove_dir_all(&self.0).unwrap();
			}
		}
	}

	fn write_ops<F: FnOnce(&mut LogWriter)>(table: &ValueTable, log: &Log, f: F) {
		let mut writer = log.begin_record();
		f(&mut writer);
		log.end_record(writer.drain()).unwrap();
		// Cycle through 2 log files
		let _ = log.read_next(false);
		log.flush_one(|| Ok(())).unwrap();
		let _ = log.read_next(false);
		log.flush_one(|| Ok(())).unwrap();
		let mut reader = log.read_next(false).unwrap().unwrap();
		loop {
			match reader.next().unwrap() {
				LogAction::BeginRecord(_) | LogAction::InsertIndex { .. } | LogAction::DropTable { .. } => {
					panic!("Unexpected log entry");
				},
				LogAction::EndRecord => {
					break;
				},
				LogAction::InsertValue(insertion) => {
					table.enact_plan(insertion.index, &mut reader).unwrap();
				},
			}
		}
	}

	fn key(k: u32) -> Key {
		let mut key = Key::default();
		key.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &[], &k.to_le_bytes()).as_bytes());
		key
	}

	fn value(size: usize) -> Value {
		use rand::RngCore;
		let mut result = Vec::with_capacity(size);
		result.resize(size, 0);
		rand::thread_rng().fill_bytes(&mut result);
		result
	}

	#[test]
	fn insert_simple() {
		let dir = TempDir::new("insert_simple");
		let table = dir.table(Some(ENTRY_SIZE));
		let log = dir.log();

		let key = key(1);
		let val = value(19);
		let compressed = true;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key, &val, writer, compressed).unwrap();
			assert_eq!(table.get(&key, 1, writer).unwrap(), Some((val.clone(), compressed)));
		});

		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val, compressed)));
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 2);
	}

	#[test]
	#[should_panic(expected = "assertion failed: entry_size <= MAX_ENTRY_SIZE as u16")]
	fn oversized_into_fixed_panics() {
		let dir = TempDir::new("oversized_into_fixed_panics");
		let _table = dir.table(Some(65534));
	}

	#[test]
	fn remove_simple() {
		let dir = TempDir::new("remove_simple");
		let table = dir.table(Some(ENTRY_SIZE));
		let log = dir.log();

		let key1 = key(1);
		let key2 = key(2);
		let val1 = value(11);
		let val2 = value(21);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		write_ops(&table, &log, |writer| {
			table.write_remove_plan(1, writer).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), None);
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 1);

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
	}

	#[test]
	fn replace_simple() {
		let dir = TempDir::new("replace_simple");
		let table = dir.table(Some(ENTRY_SIZE));
		let log = dir.log();

		let key1 = key(1);
		let key2 = key(2);
		let key3 = key(2);
		let val1 = value(11);
		let val2 = value(21);
		let val3 = value(31);
		let compressed = true;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key3, &val3, writer, false).unwrap();
		});

		assert_eq!(table.get(&key3, 1, log.overlays()).unwrap(), Some((val3, false)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
	}

	#[test]
	fn replace_multipart_shorter() {
		let dir = TempDir::new("replace_multipart_shorter");
		let table = dir.table(None);
		let log = dir.log();

		let key1 = key(1);
		let key2 = key(2);
		let val1 = value(20000);
		let val2 = value(30);
		let val1s = value(5000);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 7);

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key1, &val1s, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1s, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 5);
		write_ops(&table, &log, |writer| {
			assert_eq!(table.read_next_free(5, writer).unwrap(), 4);
			assert_eq!(table.read_next_free(4, writer).unwrap(), 3);
			assert_eq!(table.read_next_free(3, writer).unwrap(), 0);
		});
	}

	#[test]
	fn replace_multipart_longer() {
		let dir = TempDir::new("replace_multipart_longer");
		let table = dir.table(None);
		let log = dir.log();

		let key1 = key(1);
		let key2 = key(2);
		let val1 = value(5000);
		let val2 = value(30);
		let val1l = value(20000);
		let compressed = false;

		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key1, &val1, writer, compressed).unwrap();
			table.write_insert_plan(&key2, &val2, writer, compressed).unwrap();
		});

		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 4);

		write_ops(&table, &log, |writer| {
			table.write_replace_plan(1, &key1, &val1l, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key1, 1, log.overlays()).unwrap(), Some((val1l, compressed)));
		assert_eq!(table.last_removed.load(std::sync::atomic::Ordering::Relaxed), 0);
		assert_eq!(table.filled.load(std::sync::atomic::Ordering::Relaxed), 7);
	}

	#[test]
	fn ref_counting() {
		let dir = TempDir::new("ref_counting");
		let table = dir.table(None);
		let log = dir.log();

		let key = key(1);
		let val = value(5000);

		let compressed = true;
		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key, &val, writer, compressed).unwrap();
			table.write_inc_ref(1, writer, compressed).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val.clone(), compressed)));
		write_ops(&table, &log, |writer| {
			table.write_dec_ref(1, writer).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val, compressed)));
		write_ops(&table, &log, |writer| {
			table.write_dec_ref(1, writer).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), None);
	}
}
