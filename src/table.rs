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
// [SIZE: 2][REFS: 4][KEY: 26][VALUE]
// SIZE: 15-bit value size. Sizes up to 0x7ffc are allowed.
// This includes size of REFS and KEY.
// The first bit is reserved to indicate if compression is applied.
// REF: 32-bit reference counter (optional).
// KEY: lower 26 bytes of the key.
// VALUE: payload bytes.
//
// Partial entry (first part):
// [MULTIHEAD: 2][NEXT: 8][REFS: 4][KEY: 26][VALUE]
// MULTIHEAD - Split entry head marker. 0xfffd.
// NEXT - 64-bit index of the entry that holds the next part.
// take all available space in this entry.
// REF: 32-bit reference counter (optional).
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
// SIZE: 15-bit size of the remaining payload, also indicate
// if value is compressed.
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
use std::sync::Arc;
use parking_lot::{RwLockUpgradableReadGuard, RwLock};
use crate::{
	error::Result,
	column::ColId,
	log::{LogQuery, LogReader, LogWriter},
	display::hex,
	options::ColumnOptions as Options,
};

pub const KEY_LEN: usize = 32;
pub const SIZE_TIERS: usize = 1usize << SIZE_TIERS_BITS;
pub const SIZE_TIERS_BITS: u8 = 8;
pub const COMPRESSED_MASK: u16 = 0x80_00;
pub const MAX_ENTRY_SIZE: usize = 0x7ff8;
pub const MIN_ENTRY_SIZE: usize = 32;
const REFS_SIZE: usize = 4;
const SIZE_SIZE: usize = 2;
const PARTIAL_SIZE: usize = 26;
const INDEX_SIZE: usize = 8;
const MAX_ENTRY_BUF_SIZE: usize = 0x8000;

const TOMBSTONE: &[u8] = &[0xff, 0xff];
const MULTIPART: &[u8] = &[0xff, 0xfe];
const MULTIHEAD: &[u8] = &[0xff, 0xfd];
// When a rc reach locked ref, it is locked in db.
const LOCKED_REF: u32 = u32::MAX;


pub type Key = [u8; KEY_LEN];
pub type Value = Vec<u8>;

fn partial_key(hash: &Key) -> &[u8] {
	&hash[6..]
}

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
		format!("table_{:02}_{}", self.col(), hex(&[self.size_tier()]))
	}

	pub fn legacy_file_name(&self) -> String {
		format!("table_{:02}_{}", self.col(), self.size_tier())
	}

	pub fn is_file_name(col: ColId, name: &str) -> bool {
		name.starts_with(&format!("table_{:02}_", col))
	}

	pub fn as_u16(&self) -> u16 {
		self.0
	}
}

impl std::fmt::Display for TableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{:02}-{:02}", self.col(), hex(&[self.size_tier()]))
	}
}

pub struct ValueTable {
	pub id: TableId,
	pub entry_size: u16,
	file: RwLock<Option<std::fs::File>>,
	path: Arc<std::path::PathBuf>,
	capacity: AtomicU64,
	filled: AtomicU64,
	last_removed: AtomicU64,
	dirty_header: AtomicBool,
	dirty: AtomicBool,
	multipart: bool,
	ref_counted: bool,
	no_compression: bool, // This legacy table can't be compressed. TODO: remove this
}

#[cfg(target_os = "linux")]
fn disable_read_ahead(file: &std::fs::File) -> Result<()> {
    use std::os::unix::io::AsRawFd;
    let err = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
    if err != 0 {
        Err(std::io::Error::from_raw_os_error(err))?
    } else {
        Ok(())
    }
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

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn disable_read_ahead(_file: &std::fs::File) -> Result<()> {
	Ok(())
}

#[derive(Default, Clone, Copy)]
struct Header([u8; 16]);

impl Header {
	fn last_removed(&self) -> u64 {
		u64::from_le_bytes(self.0[0..INDEX_SIZE].try_into().unwrap())
	}
	fn set_last_removed(&mut self, last_removed: u64) {
		self.0[0..INDEX_SIZE].copy_from_slice(&last_removed.to_le_bytes());
	}
	fn filled(&self) -> u64 {
		u64::from_le_bytes(self.0[INDEX_SIZE..INDEX_SIZE * 2].try_into().unwrap())
	}
	fn set_filled(&mut self, filled: u64) {
		self.0[INDEX_SIZE..INDEX_SIZE * 2].copy_from_slice(&filled.to_le_bytes());
	}
}

struct Entry<B: AsRef<[u8]> + AsMut<[u8]>>(usize, B);
type FullEntry = Entry<[u8; MAX_ENTRY_BUF_SIZE]>;
type PartialEntry = Entry<[u8; 10]>;
type PartialKeyEntry = Entry<[u8; 40]>;

impl<B: AsRef<[u8]> + AsMut<[u8]>> Entry<B> {
	#[inline(always)]
	fn new_uninit() -> Self {
		Entry(0, unsafe { MaybeUninit::uninit().assume_init() })
	}

	fn set_offset(&mut self, offset: usize) {
		self.0 = offset;
	}

	fn offset(&self) -> usize {
		self.0
	}

	fn write_slice(&mut self, buf: &[u8]) {
		let start = self.0;
		self.0 += buf.len();
		self.1.as_mut()[start..self.0].copy_from_slice(buf);
	}
	fn read_slice(&mut self, size: usize) -> &[u8] {
		let start = self.0;
		self.0 += size;
		&self.1.as_ref()[start..self.0]
	}

	fn is_tombstone(&self) -> bool {
		&self.1.as_ref()[0..SIZE_SIZE] == TOMBSTONE
	}
	fn write_tombstone(&mut self) {
		self.write_slice(&TOMBSTONE);
	}

	fn is_multipart(&self) -> bool {
		&self.1.as_ref()[0..SIZE_SIZE] == MULTIPART
	}
	fn write_multipart(&mut self) {
		self.write_slice(&MULTIPART);
	}
	fn is_multihead(&self) -> bool {
		&self.1.as_ref()[0..SIZE_SIZE] == MULTIHEAD
	}
	fn write_multihead(&mut self) {
		self.write_slice(&MULTIHEAD);
	}

	fn read_size(&mut self, no_compression: bool) -> (u16, bool) {
		let size = u16::from_le_bytes(self.read_slice(SIZE_SIZE).try_into().unwrap());
		if !no_compression {
			let compressed = (size & COMPRESSED_MASK) > 0;
			(size & !COMPRESSED_MASK, compressed)
		} else {
			(size, false)
		}
	}
	fn skip_size(&mut self) {
		self.0 += SIZE_SIZE;
	}
	fn write_size(&mut self, mut size: u16, compressed: bool) {
		if compressed {
			size |= COMPRESSED_MASK;
		}
		self.write_slice(&size.to_le_bytes());
	}

	fn read_next(&mut self) -> u64 {
		u64::from_le_bytes(self.read_slice(INDEX_SIZE).try_into().unwrap())
	}
	fn skip_next(&mut self) {
		self.0 += INDEX_SIZE;
	}
	fn write_next(&mut self, next_index: u64) {
		self.write_slice(&next_index.to_le_bytes());
	}

	fn read_rc(&mut self) -> u32 {
		u32::from_le_bytes(self.read_slice(REFS_SIZE).try_into().unwrap())
	}
	fn skip_rc(&mut self) {
		self.0 += REFS_SIZE;
	}
	fn write_rc(&mut self, rc: u32) {
		self.write_slice(&rc.to_le_bytes());
	}

	fn read_partial(&mut self) -> &[u8] {
		self.read_slice(PARTIAL_SIZE)
	}

	fn remaining_to(&self, end: usize) -> &[u8] {
		&self.1.as_ref()[self.0..end]
	}
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> AsMut<[u8]> for Entry<B> {
	fn as_mut(&mut self) -> &mut [u8] {
		self.1.as_mut()
	}
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> AsRef<[u8]> for Entry<B> {
	fn as_ref(&self) -> &[u8] {
		self.1.as_ref()
	}
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> std::ops::Index<std::ops::Range<usize>> for Entry<B> {
	type Output = [u8];

	fn index(&self, index: std::ops::Range<usize>) -> &[u8] {
		&self.1.as_ref()[index]
	}
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> std::ops::IndexMut<std::ops::Range<usize>> for Entry<B> {
	fn index_mut(&mut self, index: std::ops::Range<usize>) -> &mut [u8] {
		&mut self.1.as_mut()[index]
	}
}

impl ValueTable {
	pub fn open(
		path: Arc<std::path::PathBuf>,
		id: TableId,
		entry_size: Option<u16>,
		options: &Options,
		db_version: u32,
	) -> Result<ValueTable> {
		let (multipart, entry_size) = match entry_size {
			Some(s) => (false, s),
			None => (true, 4096),
		};
		assert!(entry_size >= MIN_ENTRY_SIZE as u16);
		if db_version >= 4 {
			assert!(entry_size <= MAX_ENTRY_SIZE as u16);
		}

		let mut filepath: std::path::PathBuf = std::path::PathBuf::clone(&*path);
		// Check for old file name format
		filepath.push(id.legacy_file_name());
		let mut file = if db_version == 3 && std::fs::metadata(&filepath).is_ok() {
			Some(std::fs::OpenOptions::new().create(true).read(true).write(true).open(filepath.as_path())?)
		} else {
			filepath.pop();
			filepath.push(id.file_name());
			if std::fs::metadata(&filepath).is_ok() {
				Some(std::fs::OpenOptions::new().create(true).read(true).write(true).open(filepath.as_path())?)
			} else {
				None
			}
		};
		let mut filled = 1;
		let mut capacity = 1;
		let mut last_removed = 0;
		if let Some(file) = &mut file {
			disable_read_ahead(file)?;
			let mut file_len = file.metadata()?.len();
			if file_len == 0 {
				// Preallocate a single entry that contains metadata
				file.set_len(entry_size as u64)?;
				file_len = entry_size as u64;
			}

			capacity = file_len / entry_size as u64;
			let mut header = Header::default();
			file.read_exact(&mut header.0)?;
			last_removed = header.last_removed();
			filled = header.filled();
			if filled == 0 {
				filled = 1;
			}
			log::debug!(target: "parity-db", "Opened value table {} with {} entries, entry_size={}", id, filled, entry_size);
		}

		Ok(ValueTable {
			id,
			entry_size,
			path,
			file: RwLock::new(file),
			capacity: AtomicU64::new(capacity),
			filled: AtomicU64::new(filled),
			last_removed: AtomicU64::new(last_removed),
			dirty_header: AtomicBool::new(false),
			dirty: AtomicBool::new(false),
			multipart,
			ref_counted: options.ref_counted,
			no_compression: db_version <= 3,
		})
	}

	fn create_file(&self) -> Result<std::fs::File> {
		let mut path = std::path::PathBuf::clone(&*self.path);
		path.push(self.id.file_name());
		let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(path.as_path())?;
		disable_read_ahead(&file)?;
		log::debug!(target: "parity-db", "Created value table {}", self.id);
		Ok(file)
	}

	pub fn value_size(&self) -> u16 {
		self.entry_size - SIZE_SIZE as u16 - self.ref_size() as u16 - PARTIAL_SIZE as u16
	}

	#[cfg(unix)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		Ok(self.file.read().as_ref().unwrap().read_exact_at(buf, offset)?)
	}

	#[cfg(unix)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		let mut file = self.file.upgradable_read();
		if file.is_none() {
			let mut wfile = RwLockUpgradableReadGuard::upgrade(file);
			*wfile = Some(self.create_file()?);
			file = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wfile);
		}
		Ok(file.as_ref().unwrap().write_all_at(buf, offset)?)
	}

	#[cfg(windows)]
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.file.read().as_ref().unwrap().seek_read(buf, offset)?;
		Ok(())
	}

	#[cfg(windows)]
	fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		let mut file = self.file.upgradable_read();
		if file.is_none() {
			let mut wfile = RwLockUpgradableReadGuard::upgrade(file);
			*wfile = Some(self.create_file()?);
			file = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wfile);
		}
		file.as_ref().unwrap().seek_write(buf, offset)?;
		Ok(())
	}

	fn grow(&self) -> Result<()> {
		let mut capacity = self.capacity.load(Ordering::Relaxed);
		capacity += (256 * 1024) / self.entry_size as u64;
		self.capacity.store(capacity, Ordering::Relaxed);
		let mut file = self.file.upgradable_read();
		if file.is_none() {
			let mut wfile = RwLockUpgradableReadGuard::upgrade(file);
			*wfile = Some(self.create_file()?);
			file = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wfile);
		}
		file.as_ref().unwrap().set_len(capacity * self.entry_size as u64)?;
		Ok(())
	}

	// Return ref counter, partial key and if it was compressed.
	#[inline(always)]
	pub fn for_parts<Q: LogQuery, F: FnMut(&[u8])>(
		&self,
		key: Option<&Key>,
		mut index: u64,
		log: &Q,
		mut f: F,
	) -> Result<(u32, [u8; PARTIAL_SIZE], bool)> {
		let mut buf = FullEntry::new_uninit();
		let mut part = 0;
		let mut compressed = false;
		let mut rc = 1;
		let mut pk = [0u8; PARTIAL_SIZE];
		let entry_size = self.entry_size as usize;
		loop {
			let buf = if log.value(self.id, index, buf.as_mut()) {
				&mut buf
			} else {
				log::trace!(
					target: "parity-db",
					"{}: Query slot {}",
					self.id,
					index,
				);
				self.read_at(&mut buf[0..entry_size], index * self.entry_size as u64)?;
				&mut buf
			};

			buf.set_offset(0);

			if buf.is_tombstone() {
				return Ok((0, Default::default(), false));
			}

			let (entry_end, next) = if buf.is_multipart() || buf.is_multihead() {
				buf.skip_size();
				let next = buf.read_next();
				(entry_size, next)
			} else {
				let (size, read_compressed) = buf.read_size(self.no_compression);
				compressed = read_compressed;
				(buf.offset() + size as usize, 0)
			};

			if part == 0 {
				if self.ref_counted {
					rc = buf.read_rc();
				}
				let pks = buf.read_partial();
				pk.copy_from_slice(&pks);
				if key.map_or(false, |k| partial_key(k) != pk) {
					log::debug!(
						target: "parity-db",
						"{}: Key mismatch at {}. Expected {:?}, got {}, size = {}",
						self.id,
						index,
						key.map(|k| hex(partial_key(k))),
						hex(&pk),
						self.entry_size,
					);
					return Ok((0, Default::default(), false));
				}
				f(buf.remaining_to(entry_end))
			} else {
				f(buf.remaining_to(entry_end))
			}
			if next == 0 {
				break;
			}
			part += 1;
			index = next;
		}
		Ok((rc, pk, compressed))
	}

	pub fn get(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(Value, bool)>> {
		let mut result = Vec::new();
		let (rc, _, compressed) = self.for_parts(Some(key), index, log, |buf| result.extend_from_slice(buf))?;
		if rc > 0 {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn get_with_meta(&self, index: u64, log: &impl LogQuery) -> Result<Option<(Value, u32, [u8; PARTIAL_SIZE], bool)>> {
		let mut result = Vec::new();
		let (rc, pkey, compressed) = self.for_parts(None, index, log, |buf| result.extend_from_slice(buf))?;
		if rc > 0 {
			return Ok(Some((result, rc, pkey, compressed)));
		}
		Ok(None)
	}


	pub fn size(&self, key: &Key, index: u64, log: &impl LogQuery) -> Result<Option<(u32, bool)>> {
		let mut result = 0;
		let (rc, _, compressed) = self.for_parts(Some(key), index, log, |buf| result += buf.len() as u32)? ;
		if rc > 0 {
			return Ok(Some((result, compressed)));
		}
		Ok(None)
	}

	pub fn has_key_at(&self, index: u64, key: &Key, log: &LogWriter) -> Result<bool> {
		Ok(match self.partial_key_at(index, log)? {
			Some(existing_key) => &existing_key[..] == partial_key(key),
			None => false,
		})
	}

	pub fn partial_key_at<Q: LogQuery>(&self, index: u64, log: &Q) -> Result<Option<[u8; PARTIAL_SIZE]>> {
		let mut buf = PartialKeyEntry::new_uninit();
		let mut result = [0u8; PARTIAL_SIZE];
		let buf = if log.value(self.id, index, buf.as_mut()) {
			&mut buf
		} else {
			self.read_at(buf.as_mut(), index * self.entry_size as u64)?;
			&mut buf
		};
		if buf.is_tombstone() {
			return Ok(None);
		}
		buf.skip_size();
		if buf.is_multipart() || buf.is_multihead() {
			buf.skip_next();
		}
		if self.ref_counted {
			buf.skip_rc();
		}
		result[..].copy_from_slice(buf.read_partial());

		Ok(Some(result))
	}

	pub fn read_next_free(&self, index: u64, log: &LogWriter) -> Result<u64> {
		let mut buf = PartialEntry::new_uninit();
		if !log.value(self.id, index, buf.as_mut()) {
			self.read_at(buf.as_mut(), index * self.entry_size as u64)?;
		}
		buf.skip_size();
		let next = buf.read_next();
		return Ok(next);
	}

	pub fn read_next_part(&self, index: u64, log: &LogWriter) -> Result<Option<u64>> {
		let mut buf = PartialEntry::new_uninit();
		if !log.value(self.id, index, buf.as_mut()) {
			self.read_at(buf.as_mut(), index * self.entry_size as u64)?;
		}
		if buf.is_multipart() || buf.is_multihead() {
			buf.skip_size();
			let next = buf.read_next();
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
		let mut remainder = value.len() + self.ref_size() + PARTIAL_SIZE;
		let mut offset = 0;
		let mut start = 0;
		assert!(self.multipart || value.len() <= self.value_size() as usize);
		let (mut index, mut follow) = match at {
			Some(index) => (index, true),
			None => (self.next_free(log)?, false)
		};
		loop {
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
			let mut buf = FullEntry::new_uninit();
			let free_space = self.entry_size as usize - SIZE_SIZE;
			let value_len = if remainder > free_space {
				if !follow {
					next_index = self.next_free(log)?
				}
				if start == 0 {
					buf.write_multihead();
				} else {
					buf.write_multipart();
				}
				buf.write_next(next_index);
				free_space - INDEX_SIZE
			} else {
				buf.write_size(remainder as u16, compressed);
				remainder
			};
			let init_offset = buf.offset();
			if offset == 0 {
				if self.ref_counted {
					// first rc.
					buf.write_rc(1u32);
				}
				buf.write_slice(partial_key(key));
			}
			let written = buf.offset() - init_offset;
			buf.write_slice(&value[offset..offset + value_len - written]);
			offset += value_len - written;
			log.insert_value(self.id, index, buf[0..buf.offset()].to_vec());
			remainder -= value_len;
			if start == 0 {
				start = index;
			}
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

		let mut buf = PartialEntry::new_uninit();
		buf.write_tombstone();
		buf.write_next(last_removed);

		log.insert_value(self.id, index, buf[0..buf.offset()].to_vec());
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

	pub fn write_inc_ref(&self, index: u64, log: &mut LogWriter) -> Result<()> {
		self.change_ref(index, 1, log)?;
		Ok(())
	}

	pub fn write_dec_ref(&self, index: u64, log: &mut LogWriter) -> Result<bool> {
		if self.change_ref(index, -1, log)? {
			return Ok(true);
		}
		self.write_remove_plan(index, log)?;
		Ok(false)
	}

	pub fn change_ref(&self, index: u64, delta: i32, log: &mut LogWriter) -> Result<bool> {
		let mut buf = FullEntry::new_uninit();
		let buf = if log.value(self.id, index, buf.as_mut()) {
			&mut buf
		} else {
			self.read_at(&mut buf[0..self.entry_size as usize], index * self.entry_size as u64)?;
			&mut buf
		};

		if buf.is_tombstone() {
			return Ok(false);
		}

		let size = if buf.is_multipart() || buf.is_multihead() {
			buf.skip_size();
			buf.skip_next();
			self.entry_size as usize
		} else {
			let (size, _compressed) = buf.read_size(self.no_compression);
			buf.offset() + size as usize
		};

		let rc_offset = buf.offset();
		let mut counter = buf.read_rc();
		if delta > 0 {
			if counter >= LOCKED_REF - delta as u32 {
				counter = LOCKED_REF
			} else {
				counter = counter + delta as u32;
			}
		} else {
			if counter != LOCKED_REF {
				counter = counter.saturating_sub(-delta as u32);
				if counter == 0 {
					return Ok(false);
				}
			}
		}

		buf.set_offset(rc_offset);
		buf.write_rc(counter);
		// TODO: optimize actual buf size
		log.insert_value(self.id, index, buf[0..size].to_vec());
		return Ok(true);
	}

	pub fn enact_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		while index >= self.capacity.load(Ordering::Relaxed) {
			self.grow()?;
		}
		if index == 0 {
			let mut header = Header::default();
			log.read(&mut header.0)?;
			self.write_at(&header.0, 0)?;
			return Ok(());
		}

		let mut buf = FullEntry::new_uninit();
		log.read(&mut buf[0..SIZE_SIZE])?;
		if buf.is_tombstone() {
			log.read(&mut buf[SIZE_SIZE..SIZE_SIZE + INDEX_SIZE])?;
			self.write_at(&buf[0..SIZE_SIZE + INDEX_SIZE], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted tombstone in slot {}", self.id, index);
		} else if buf.is_multipart() || buf.is_multihead() {
				let entry_size = self.entry_size as usize;
				log.read(&mut buf[SIZE_SIZE..entry_size])?;
				self.write_at(&buf[0..entry_size], index * (entry_size as u64))?;
				log::trace!(target: "parity-db", "{}: Enacted multipart in slot {}", self.id, index);
		} else {
			let (len, _compressed) = buf.read_size(self.no_compression);
			log.read(&mut buf[SIZE_SIZE..SIZE_SIZE + len as usize])?;
			self.write_at(&buf[0..(SIZE_SIZE + len as usize)], index * (self.entry_size as u64))?;
			log::trace!(target: "parity-db", "{}: Enacted {}: {}, {} bytes", self.id, index, hex(&buf.1[6..32]), len);
		}
		Ok(())
	}

	pub fn validate_plan(&self, index: u64, log: &mut LogReader) -> Result<()> {
		if index == 0 {
			let mut header = Header::default();
			log.read(&mut header.0)?;
			// TODO: sanity check last_removed and filled
			return Ok(());
		}
		let mut buf = FullEntry::new_uninit();
		log.read(&mut buf[0..SIZE_SIZE])?;
		if buf.is_tombstone() {
			log.read(&mut buf[SIZE_SIZE..SIZE_SIZE + INDEX_SIZE])?;
			log::trace!(target: "parity-db", "{}: Validated tombstone in slot {}", self.id, index);
		}
		else if buf.is_multipart() || buf.is_multihead() {
			let entry_size = self.entry_size as usize;
			log.read(&mut buf[SIZE_SIZE..entry_size])?;
			log::trace!(target: "parity-db", "{}: Validated multipart in slot {}", self.id, index);
		} else {
			// TODO: check len
			let (len, _compressed) = buf.read_size(self.no_compression);
			log.read(&mut buf[SIZE_SIZE..SIZE_SIZE + len as usize])?;
			log::trace!(target: "parity-db", "{}: Validated {}: {}, {} bytes", self.id, index, hex(&buf[SIZE_SIZE..32]), len);
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		if self.file.read().is_none() {
			return Ok(());
		}
		let mut header = Header::default();
		self.read_at(&mut header.0, 0)?;
		let last_removed = header.last_removed();
		let mut filled = header.filled();
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
			let mut buf = Header::default();
			let last_removed = self.last_removed.load(Ordering::Relaxed);
			let filled = self.filled.load(Ordering::Relaxed);
			buf.set_last_removed(last_removed);
			buf.set_filled(filled);
			log.insert_value(self.id, 0, buf.0.to_vec());
		}
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Ok(true) = self.dirty.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed) {
			if let Some(file) = self.file.read().as_ref() {
				file.sync_data()?;
			}
		}
		Ok(())
	}

	fn ref_size(&self) -> usize {
		if self.ref_counted {
			REFS_SIZE
		} else {
			0
		}
	}

	pub fn iter_while(&self, log: &impl LogQuery, mut f: impl FnMut (u64, u32, Vec<u8>, bool) -> bool) -> Result<()> {
		let filled = self.filled.load(Ordering::Relaxed);
		for index in 1 .. filled {
			let mut result = Vec::new();
			let (rc, _, compressed) = self.for_parts(None, index, log, |buf| result.extend_from_slice(buf))?;
			if rc > 0 {
				if !f(index, rc, result, compressed) {
					break;
				}
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	const ENTRY_SIZE: u16 = 64;
	use super::{ValueTable, TableId, Key, Value};
	use crate::{log::{Log, LogWriter, LogAction}, options::{Options, ColumnOptions, CURRENT_VERSION}};

	struct TempDir(std::sync::Arc<std::path::PathBuf>);

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
			TempDir(std::sync::Arc::new(path))
		}

		fn table(&self, size: Option<u16>, options: &ColumnOptions) -> ValueTable {
			let id = TableId::new(0, 0);
			ValueTable::open(self.0.clone(), id, size, options, CURRENT_VERSION).unwrap()
		}

		fn log(&self) -> Log {
			let options = Options::with_columns(&*self.0, 1);
			Log::open(&options).unwrap()
		}
	}

	impl Drop for TempDir {
		fn drop(&mut self) {
			if self.0.exists() {
				std::fs::remove_dir_all(&*self.0).unwrap();
			}
		}
	}

	fn write_ops<F: FnOnce(&mut LogWriter)>(table: &ValueTable, log: &Log, f: F) {
		let mut writer = log.begin_record();
		f(&mut writer);
		let bytes_written = log.end_record(writer.drain()).unwrap();
		// Cycle through 2 log files
		let _ = log.read_next(false);
		log.flush_one(0).unwrap();
		let _ = log.read_next(false);
		log.flush_one(0).unwrap();
		let mut reader = log.read_next(false).unwrap().unwrap();
		loop {
			match reader.next().unwrap() {
				LogAction::BeginRecord | LogAction::InsertIndex { .. } | LogAction::DropTable { .. } => {
					panic!("Unexpected log entry");
				},
				LogAction::EndRecord => {
					let bytes_read = reader.read_bytes();
					assert_eq!(bytes_written, bytes_read);
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

	fn rc_options() -> ColumnOptions {
		let mut result = ColumnOptions::default();
		result.ref_counted = true;
		result
	}

	#[test]
	fn insert_simple() {
		insert_simple_inner(&Default::default());
		insert_simple_inner(&rc_options());
	}
	fn insert_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("insert_simple");
		let table = dir.table(Some(ENTRY_SIZE), options);
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
		let _table = dir.table(Some(65534), &Default::default());
	}

	#[test]
	fn remove_simple() {
		remove_simple_inner(&Default::default());
		remove_simple_inner(&rc_options());
	}
	fn remove_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("remove_simple");
		let table = dir.table(Some(ENTRY_SIZE), options);
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
		replace_simple_inner(&Default::default());
		replace_simple_inner(&rc_options());
	}
	fn replace_simple_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_simple");
		let table = dir.table(Some(ENTRY_SIZE), options);
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
		replace_multipart_shorter_inner(&Default::default());
		replace_multipart_shorter_inner(&rc_options());
	}
	fn replace_multipart_shorter_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_multipart_shorter");
		let table = dir.table(None, options);
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
		replace_multipart_longer_inner(&Default::default());
		replace_multipart_longer_inner(&rc_options());
	}
	fn replace_multipart_longer_inner(options: &ColumnOptions) {
		let dir = TempDir::new("replace_multipart_longer");
		let table = dir.table(None, options);
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
		for compressed in [false, true] {
			let dir = TempDir::new("ref_counting");
			let table = dir.table(None, &rc_options());
			let log = dir.log();

			let key = key(1);
			let val = value(5000);

			write_ops(&table, &log, |writer| {
				table.write_insert_plan(&key, &val, writer, compressed).unwrap();
				table.write_inc_ref(1, writer).unwrap();
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

	#[test]
	fn ref_underflow() {
		let dir = TempDir::new("ref_underflow");
		let table = dir.table(None, &rc_options());
		let log = dir.log();

		let key = key(1);
		let val = value(10);

		let compressed = false;
		write_ops(&table, &log, |writer| {
			table.write_insert_plan(&key, &val, writer, compressed).unwrap();
			table.write_inc_ref(1, writer).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), Some((val.clone(), compressed)));
		write_ops(&table, &log, |writer| {
			table.write_dec_ref(1, writer).unwrap();
			table.write_dec_ref(1, writer).unwrap();
			table.write_dec_ref(1, writer).unwrap();
		});
		assert_eq!(table.get(&key, 1, log.overlays()).unwrap(), None);
	}
}
