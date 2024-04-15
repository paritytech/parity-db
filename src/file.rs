// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Utilities for db file.

use crate::{
	error::{try_io, Result},
	parking_lot::RwLock,
	table::TableId,
};
use std::sync::atomic::{AtomicU64, Ordering};

trait OpenOptionsExt {
	fn disable_read_ahead(&mut self) -> &mut Self;
}

#[inline]
fn offset_to_file_index(offset: u64, max_size: Option<usize>) -> (u64, usize) {
	if let Some(m) = max_size {
		(offset % m as u64, offset as usize / m)
	} else {
		(offset, 0)
	}
}

impl OpenOptionsExt for std::fs::OpenOptions {
	#[cfg(not(windows))]
	fn disable_read_ahead(&mut self) -> &mut Self {
		// Not supported
		self
	}

	#[cfg(windows)]
	fn disable_read_ahead(&mut self) -> &mut Self {
		use std::os::windows::fs::OpenOptionsExt;
		self.custom_flags(winapi::um::winbase::FILE_FLAG_RANDOM_ACCESS)
	}
}

#[cfg(target_os = "linux")]
fn disable_read_ahead(file: &std::fs::File) -> std::io::Result<()> {
	use std::os::unix::io::AsRawFd;
	let err = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
	if err != 0 {
		Err(std::io::Error::from_raw_os_error(err))
	} else {
		Ok(())
	}
}

#[cfg(target_os = "macos")]
fn disable_read_ahead(file: &std::fs::File) -> std::io::Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, 0) } != 0 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(())
	}
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn disable_read_ahead(_file: &std::fs::File) -> std::io::Result<()> {
	Ok(())
}

#[cfg(unix)]
pub fn madvise_random(map: &mut memmap2::MmapMut) {
	unsafe {
		libc::madvise(map.as_mut_ptr() as _, map.len(), libc::MADV_RANDOM);
	}
}

#[cfg(not(unix))]
pub fn madvise_random(_map: &mut memmap2::MmapMut) {}

#[cfg(not(windows))]
fn mmap(file: &std::fs::File, len: usize) -> Result<memmap2::MmapMut> {
	// TODO pass len not 0 and limit reserve to max file size
	#[cfg(not(test))]
	const RESERVE_ADDRESS_SPACE: usize = 1024 * 1024 * 1024; // 1 Gb
														 // Use a different value for tests to work around docker limits on the test machine.
	#[cfg(test)]
	const RESERVE_ADDRESS_SPACE: usize = 64 * 1024 * 1024; // 64 Mb

	let map_len = len + RESERVE_ADDRESS_SPACE; // TODO should be max??
	let mut map = try_io!(unsafe { memmap2::MmapOptions::new().len(map_len).map_mut(file) });
	madvise_random(&mut map);
	Ok(map)
}

#[cfg(windows)]
fn mmap(file: &std::fs::File, _len: usize) -> Result<memmap2::MmapMut> {
	Ok(try_io!(unsafe { memmap2::MmapOptions::new().map_mut(file) }))
}

const GROW_SIZE_BYTES: u64 = 256 * 1024;

#[derive(Debug)]
pub struct TableFile {
	pub maps: RwLock<Vec<(memmap2::MmapMut, std::fs::File)>>,
	pub path_base: std::path::PathBuf,
	pub capacity: AtomicU64,
	pub id: TableId,
	pub max_size: Option<usize>,
}

impl TableFile {
	pub fn open(
		path_base: std::path::PathBuf,
		entry_size: u16,
		id: TableId,
		max_file_size: Option<usize>,
	) -> Result<Self> {
		let mut capacity = 0u64;
		let mut maps = Vec::new();
		for i in 0.. {
			let mut filepath = path_base.clone();
			filepath.push(id.file_name(max_file_size.is_some().then(|| i)));
			if std::fs::metadata(&filepath).is_ok() {
				let file = try_io!(std::fs::OpenOptions::new()
					.read(true)
					.write(true)
					.disable_read_ahead()
					.open(filepath.as_path()));
				try_io!(disable_read_ahead(&file));
				let len = try_io!(file.metadata()).len();
				if len == 0 {
					// Preallocate.
					capacity += GROW_SIZE_BYTES / entry_size as u64;
					try_io!(file.set_len(GROW_SIZE_BYTES));
				} else {
					capacity = len / entry_size as u64;
				}
				let map = mmap(&file, len as usize)?;
				maps.push((map, file));
			} else {
				break;
			};
			if max_file_size.is_none() {
				break;
			}
		}
		let max_size = if let Some(m) = max_file_size {
			let m = m * 1024 * 1024;
			Some((m / entry_size as usize) * entry_size as usize)
		} else {
			None
		};
		Ok(TableFile {
			path_base,
			maps: RwLock::new(maps),
			capacity: AtomicU64::new(capacity),
			id,
			max_size,
		})
	}

	fn create_file(&self, index: Option<u32>) -> Result<std::fs::File> {
		log::debug!(target: "parity-db", "Created value table {}", self.id);
		let mut path = self.path_base.clone();
		path.push(self.id.file_name(index));
		let file = try_io!(std::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.disable_read_ahead()
			.open(&path));
		try_io!(disable_read_ahead(&file));
		Ok(file)
	}

	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		let (offset, file_index) = offset_to_file_index(offset, self.max_size);
		let offset = offset as usize;
		let map = self.maps.read();
		let (map, _) = map.get(file_index).unwrap();
		buf.copy_from_slice(&map[offset..offset + buf.len()]);
		Ok(())
	}

	#[cfg(not(feature = "loom"))]
	pub fn slice_at(&self, offset: u64, len: usize) -> MappedBytesGuard {
		let (offset, file_index) = offset_to_file_index(offset, self.max_size);
		let offset = offset as usize;
		let map = self.maps.read();
		parking_lot::RwLockReadGuard::map(map, |map| {
			let (map, _) = map.get(file_index).unwrap();
			&map[offset..offset + len]
		})
	}

	#[cfg(feature = "loom")]
	pub fn slice_at(&self, offset: u64, len: usize) -> MappedBytesGuard {
		let (offset, file_index) = offset_to_file_index(offset, self.max_size);
		let offset = offset as usize;
		let map = self.map.read();
		let (map, _) = map.get(file_index).unwrap();
		MappedBytesGuard::new(map[offset..offset + len].to_vec())
	}

	pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		let map = self.maps.read();
		let (offset, file_index) = offset_to_file_index(offset, self.max_size);
		let (map, _) = map.get(file_index).unwrap();
		let offset = offset as usize;

		// Nasty mutable pointer cast. We do ensure that all chunks that are being written are
		// accessed through the overlay in other threads.
		let ptr: *mut u8 = map.as_ptr() as *mut u8;
		let data: &mut [u8] = unsafe {
			let ptr = ptr.add(offset);
			std::slice::from_raw_parts_mut(ptr, buf.len())
		};
		data.copy_from_slice(buf);
		Ok(())
	}

	pub fn grow(&self, entry_size: u16) -> Result<()> {
		let mut maps = self.maps.write();
		let num_maps = maps.len();
		let (current_len, push) = match maps.last() {
			None => (0, true),
			Some((_, file)) => {
				let len = try_io!(file.metadata()).len();
				if self.max_size == Some(len as usize) {
					(0, true)
				} else {
					(len, false)
				}
			},
		};
		let per_file_capacity = self.max_size.map(|m| m / entry_size as usize).unwrap_or(0);
		let (new_len, prev_page_capacity) = if push {
			let file =
				self.create_file(self.max_size.is_some().then(|| num_maps as u32))?;
			let new_len = self
				.max_size
				.map(|m| std::cmp::min(m as u64, GROW_SIZE_BYTES))
				.unwrap_or(GROW_SIZE_BYTES);
			try_io!(file.set_len(new_len));
			let map = mmap(&file, new_len as usize)?;
			maps.push((map, file));
			(new_len, num_maps * per_file_capacity)
		} else {
			let (map, file) = maps.last_mut().unwrap();
			let new_len = current_len + GROW_SIZE_BYTES as u64;
			let new_len =
				self.max_size.map(|m| std::cmp::min(m as u64, new_len)).unwrap_or(new_len);
			try_io!(file.set_len(new_len));
			{
				let new_map = mmap(&file, new_len as usize)?;
				let old_map = std::mem::replace(map, new_map);
				try_io!(old_map.flush());
			}
			if num_maps > 0 {
				(new_len, (num_maps - 1) * per_file_capacity)
			} else {
				(new_len, 0)
			}
		};
		let capacity = new_len / entry_size as u64 + prev_page_capacity as u64;
		self.capacity.store(capacity, Ordering::Relaxed);
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		let maps = self.maps.read();
		for (map, _) in maps.iter() {
			try_io!(map.flush());
		}
		Ok(())
	}

	pub fn remove(&self) -> Result<()> {
		let mut maps_lock = self.maps.write();
		let mut maps = std::mem::take(&mut *maps_lock);
		let maps_len = maps.len();
		maps.reverse();
		for i in 0..maps_len as u32 {
			let (map, file) = maps.pop().unwrap();
			drop(map);
			drop(file);
			let mut path = self.path_base.clone();
			path.push(self.id.file_name(self.max_size.is_some().then(|| i)));
			try_io!(std::fs::remove_file(&path));
		}
		Ok(())
	}
}

// Loom is missing support for guard projection, so we copy the data as a workaround.
#[cfg(feature = "loom")]
pub struct MappedBytesGuard<'a> {
	_phantom: std::marker::PhantomData<&'a ()>,
	data: Vec<u8>,
}

#[cfg(feature = "loom")]
impl<'a> MappedBytesGuard<'a> {
	pub fn new(data: Vec<u8>) -> Self {
		Self { _phantom: std::marker::PhantomData, data }
	}
}

#[cfg(feature = "loom")]
impl<'a> std::ops::Deref for MappedBytesGuard<'a> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.data.as_slice()
	}
}

#[cfg(not(feature = "loom"))]
pub type MappedBytesGuard<'a> = parking_lot::MappedRwLockReadGuard<'a, [u8]>;
