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
	#[cfg(not(test))]
	const RESERVE_ADDRESS_SPACE: usize = 1024 * 1024 * 1024; // 1 Gb
														 // Use a different value for tests to work around docker limits on the test machine.
	#[cfg(test)]
	const RESERVE_ADDRESS_SPACE: usize = 64 * 1024 * 1024; // 64 Mb

	let map_len = len + RESERVE_ADDRESS_SPACE;
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
	pub map: RwLock<Option<(memmap2::MmapMut, std::fs::File)>>,
	pub path: std::path::PathBuf,
	pub capacity: AtomicU64,
	pub id: TableId,
}

impl TableFile {
	pub fn open(filepath: std::path::PathBuf, entry_size: u16, id: TableId) -> Result<Self> {
		let mut capacity = 0u64;
		let map = if std::fs::metadata(&filepath).is_ok() {
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
			Some((map, file))
		} else {
			None
		};
		Ok(TableFile {
			path: filepath,
			map: RwLock::new(map),
			capacity: AtomicU64::new(capacity),
			id,
		})
	}

	fn create_file(&self) -> Result<std::fs::File> {
		log::debug!(target: "parity-db", "Created value table {}", self.id);
		let file = try_io!(std::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.disable_read_ahead()
			.open(self.path.as_path()));
		try_io!(disable_read_ahead(&file));
		Ok(file)
	}

	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		let offset = offset as usize;
		let map = self.map.read();
		let (map, _) = map.as_ref().unwrap();
		buf.copy_from_slice(&map[offset..offset + buf.len()]);
		Ok(())
	}

	pub fn slice_at(&self, offset: u64, len: usize) -> &[u8] {
		let offset = offset as usize;
		let map = self.map.read();
		let (map, _) = map.as_ref().unwrap();
		let data: &[u8] = unsafe {
			let ptr = map.as_ptr().add(offset);
			std::slice::from_raw_parts(ptr, len)
		};
		data
	}

	pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		let map = self.map.read();
		let (map, _) = map.as_ref().unwrap();
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
		let mut map_and_file = self.map.write();
		let new_len = match map_and_file.as_mut() {
			None => {
				let file = self.create_file()?;
				let len = GROW_SIZE_BYTES;
				try_io!(file.set_len(len));
				let map = mmap(&file, 0)?;
				*map_and_file = Some((map, file));
				len
			},
			Some((map, file)) => {
				let new_len = try_io!(file.metadata()).len() + GROW_SIZE_BYTES;
				try_io!(file.set_len(new_len));
				if map.len() < new_len as usize {
					let new_map = mmap(&file, new_len as usize)?;
					let old_map = std::mem::replace(map, new_map);
					try_io!(old_map.flush());
					// Leak the old mapping as there might be concurrent readers.
					std::mem::forget(old_map);
				}
				new_len
			},
		};
		let capacity = new_len / entry_size as u64;
		self.capacity.store(capacity, Ordering::Relaxed);
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Some((map, _)) = self.map.read().as_ref() {
			try_io!(map.flush());
		}
		Ok(())
	}

	pub fn remove(&self) -> Result<()> {
		let mut map = self.map.write();
		if let Some((map, file)) = map.take() {
			drop(map);
			drop(file);
			try_io!(std::fs::remove_file(&self.path));
		}
		Ok(())
	}
}
