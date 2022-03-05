// Copyright 2015-2021 Parity Technologies (UK) Ltd.
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

use crate::{error::Result, table::TableId};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
/// Utilites for db file.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(target_os = "linux")]
fn disable_read_ahead(file: &std::fs::File) -> Result<()> {
	use std::os::unix::io::AsRawFd;
	let err = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
	if err != 0 {
		Err(std::io::Error::from_raw_os_error(err).into())
	} else {
		Ok(())
	}
}

#[cfg(target_os = "macos")]
fn disable_read_ahead(file: &std::fs::File) -> Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_RDAHEAD, 0) } != 0 {
		Err(std::io::Error::last_os_error().into())
	} else {
		Ok(())
	}
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn disable_read_ahead(_file: &std::fs::File) -> Result<()> {
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
fn fsync(file: &std::fs::File) -> Result<()> {
	use std::os::unix::io::AsRawFd;
	if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
		Err(std::io::Error::last_os_error().into())
	} else {
		Ok(())
	}
}

#[cfg(not(target_os = "macos"))]
fn fsync(file: &std::fs::File) -> Result<()> {
	file.sync_data()?;
	Ok(())
}

const GROW_SIZE_BYTES: u64 = 256 * 1024;

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
			let file = std::fs::OpenOptions::new()
				.create(true)
				.read(true)
				.write(true)
				.open(filepath.as_path())?;
			disable_read_ahead(&file)?;
			let len = file.metadata()?.len();
			if len == 0 {
				// Preallocate.
				capacity += GROW_SIZE_BYTES / entry_size as u64;
				file.set_len(capacity * entry_size as u64)?;
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
		let file = std::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(self.path.as_path())?;
		disable_read_ahead(&file)?;
		Ok(file)
	}

	#[cfg(unix)]
	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		Ok(self.file.read().as_ref().unwrap().read_exact_at(buf, offset)?)
	}

	#[cfg(unix)]
	pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::unix::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		self.file.read().as_ref().unwrap().write_all_at(buf, offset)?;
		Ok(())
	}

	#[cfg(windows)]
	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.file.read().as_ref().unwrap().seek_read(buf, offset)?;
		Ok(())
	}

	#[cfg(windows)]
	pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
		use std::os::windows::fs::FileExt;
		self.dirty.store(true, Ordering::Relaxed);
		self.file.read().as_ref().unwrap().seek_write(buf, offset)?;
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
			file = parking_lot::RwLockWriteGuard::downgrade_to_upgradable(wfile);
		}
		file.as_ref().unwrap().set_len(capacity * entry_size as u64)?;
		Ok(())
	}

	pub fn flush(&self) -> Result<()> {
		if let Ok(true) =
			self.dirty.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
		{
			if let Some(file) = self.file.read().as_ref() {
				fsync(file)?;
			}
		}
		Ok(())
	}
}
