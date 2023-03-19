// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Utilities for db file.

use crate::{
	error::{try_io, Error, Result},
	parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard},
	table::TableId,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

// `File::sync_data` uses F_FULLSYNC fcntl on MacOS. It it supposed to be
// the safest way to make sure data is fully persisted. However starting from
// MacOS 11.0 it severely degrades parallel write performance, even when writing to
// other files. Regular `fsync` is good enough for our use case.
// SSDs used in modern macs seem to be able to flush data even on unexpected power loss.
// We performed some testing with power shutdowns and kernel panics on both mac hardware
// and VMs and in all cases `fsync` was enough to prevent data corruption.
#[cfg(target_os = "macos")]
fn fsync(file: &std::fs::File) -> std::io::Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(())
	}
}

#[cfg(not(target_os = "macos"))]
fn fsync(file: &std::fs::File) -> std::io::Result<()> {
	file.sync_data()
}

const GROW_SIZE_BYTES: u64 = 256 * 1024;

#[derive(Debug)]
pub struct TableFile {
	pub file: RwLock<Option<std::fs::File>>,
	pub path: std::path::PathBuf,
	pub capacity: AtomicU64,
	pub dirty: AtomicBool,
	pub id: TableId,
}

impl TableFile {
	pub fn open(filepath: std::path::PathBuf, entry_size: u16, id: TableId) -> Result<Self> {
		let mut capacity = 0u64;
		let file = if std::fs::metadata(&filepath).is_ok() {
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
			Some(file)
		} else {
			None
		};
		Ok(TableFile {
			path: filepath,
			file: RwLock::new(file),
			capacity: AtomicU64::new(capacity),
			dirty: AtomicBool::new(false),
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
		use std::os::unix::fs::FileExt;
		try_io!(self
			.file
			.read()
			.as_ref()
			.ok_or_else(|| Error::Corruption("File does not exist.".into()))?
			.read_exact_at(buf, offset));
		Ok(())
	}

	#[cfg(unix)]
	pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		try_io!(self.file.read().as_ref().unwrap().write_all_at(buf, offset));
		Ok(())
	}

	#[cfg(windows)]
	pub fn read_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
		use crate::error::Error;
		use std::{io, os::windows::fs::FileExt};

		let file = self.file.read();
		let file = file.as_ref().ok_or_else(|| Error::Corruption("File does not exist.".into()))?;

		while !buf.is_empty() {
			match file.seek_read(buf, offset) {
				Ok(0) => break,
				Ok(n) => {
					buf = &mut buf[n..];
					offset += n as u64;
				},
				Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
					// Try again
				},
				Err(e) => return Err(Error::Io(e)),
			}
		}

		if !buf.is_empty() {
			Err(Error::Io(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"failed to fill whole buffer",
			)))
		} else {
			Ok(())
		}
	}

	#[cfg(windows)]
	pub fn write_at(&self, mut buf: &[u8], mut offset: u64) -> Result<()> {
		use crate::error::Error;
		use std::{io, os::windows::fs::FileExt};

		self.dirty.store(true, Ordering::Relaxed);
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
		let mut file = self.file.upgradable_read();
		if file.is_none() {
			let mut wfile = RwLockUpgradableReadGuard::upgrade(file);
			*wfile = Some(self.create_file()?);
			file = RwLockWriteGuard::downgrade_to_upgradable(wfile);
		}
		try_io!(file.as_ref().unwrap().set_len(capacity * entry_size as u64));
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Ok(true) =
			self.dirty.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
		{
			if let Some(file) = self.file.read().as_ref() {
				try_io!(fsync(file));
			}
		}
		Ok(())
	}

	pub fn remove(&self) -> Result<()> {
		let mut file = self.file.write();
		if let Some(file) = file.take() {
			drop(file);
			try_io!(std::fs::remove_file(&self.path));
		}
		Ok(())
	}
}
