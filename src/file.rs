// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Utilities for db file.

use crate::{
	error::{try_io, Result},
	parking_lot::RwLock,
	table::TableId,
};
use std::sync::atomic::{AtomicU64, Ordering};

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
fn madvise_random(map: &mut memmap2::MmapMut) {
	unsafe {
		libc::madvise(
			map.as_mut_ptr() as _,
			map.len(),
			libc::MADV_RANDOM,
		);
	}
}

#[cfg(not(unix))]
fn madvise_random(_id: TableId, _map: &mut memmap2::MmapMut) {}

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
				.open(filepath.as_path()));
			try_io!(disable_read_ahead(&file));
			let len = try_io!(file.metadata()).len();
			if len == 0 {
				// Preallocate.
				capacity += GROW_SIZE_BYTES / entry_size as u64;
				try_io!(file.set_len(capacity * entry_size as u64));
			} else {
				capacity = len / entry_size as u64;
			}
			let mut map = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
			madvise_random(&mut map);
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
			.open(self.path.as_path()));
		try_io!(disable_read_ahead(&file));
		Ok(file)
	}

	#[cfg(unix)]
	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		let offset = offset as usize;
		let map = self.map.read();
		let (map, _) = map.as_ref().unwrap();
		buf.copy_from_slice(&map[offset..offset + buf.len()]);
		Ok(())
	}

	#[cfg(unix)]
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

	#[cfg(windows)]
	pub fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
		let (map, _) = self.map.read().as_ref().unwrap();
		let map = map.as_mut().unwrap();
		let offset = offset as usize;
		map[offset..offset + buf.len()].copy_from_slice(buf);
		Ok(())
	}

	#[cfg(windows)]
	pub fn write_at(&self, mut buf: &[u8], mut offset: u64) -> Result<()> {
		use crate::error::Error;
		use std::{io, os::windows::fs::FileExt};

		let file = self.file.read();
		let file = file.as_ref().unwrap();

		while !buf.is_empty() {
			match file.seek_write(buf, offset) {
				Ok(0) =>
					return Err(Error::Io(io::Error::new(
						io::ErrorKind::WriteZero,
						"failed to write whole buffer",
					))),
				Ok(n) => {
					buf = &buf[n..];
					offset += n as u64;
				},
				Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
					// Try again
				},
				Err(e) => return Err(Error::Io(e)),
			}
		}

		Ok(())
	}

	pub fn grow(&self, entry_size: u16) -> Result<()> {
		let mut capacity = self.capacity.load(Ordering::Relaxed);
		capacity += GROW_SIZE_BYTES / entry_size as u64;

		self.capacity.store(capacity, Ordering::Relaxed);
		let mut map_and_file = self.map.write();
		match map_and_file.as_mut() {
			None => {
				let file = self.create_file()?;
				try_io!(file.set_len(capacity * entry_size as u64));
				let mut map = try_io!(unsafe { memmap2::MmapMut::map_mut(&file) });
				madvise_random(&mut map);
				*map_and_file = Some((map, file));
			}
			Some((map, file)) => {
				try_io!(file.set_len(capacity * entry_size as u64));
				let mut m  = try_io!(unsafe { memmap2::MmapMut::map_mut(&*file) });
				madvise_random(&mut m);
				*map = m;
			}
		}
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
