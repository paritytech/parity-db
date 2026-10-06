// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Memory-mapped file wrapper.

use crate::error::{try_io, Result};
use memmap2::MmapRaw;

/// Extra address space reserved past the end of a growable file, see [`Mmap::map_growable`].
#[cfg(all(not(windows), not(test)))]
const RESERVE_ADDRESS_SPACE: usize = 1024 * 1024 * 1024; // 1 Gb
/// Use a different value for tests to work around docker limits on the test machine.
#[cfg(all(not(windows), test))]
const RESERVE_ADDRESS_SPACE: usize = 64 * 1024 * 1024; // 64 Mb

/// A writable memory mapping of a file.
#[derive(Debug)]
pub struct Mmap(MmapRaw);

impl Mmap {
	/// Map the whole file.
	pub fn map(file: &std::fs::File) -> Result<Mmap> {
		let raw = try_io!(memmap2::MmapOptions::new().map_raw(file));
		Ok(Mmap::new(raw))
	}

	/// Map a file of `len` bytes that is expected to grow. On platforms that support it, extra
	/// address space is reserved past `len` so that the mapping does not need to be recreated on
	/// every file extension.
	#[cfg(not(windows))]
	pub fn map_growable(file: &std::fs::File, len: usize) -> Result<Mmap> {
		let map_len = len + RESERVE_ADDRESS_SPACE;
		let raw = try_io!(memmap2::MmapOptions::new().len(map_len).map_raw(file));
		Ok(Mmap::new(raw))
	}

	#[cfg(windows)]
	pub fn map_growable(file: &std::fs::File, _len: usize) -> Result<Mmap> {
		Mmap::map(file)
	}

	fn new(raw: MmapRaw) -> Mmap {
		let map = Mmap(raw);
		map.advise_random();
		map
	}

	#[cfg(unix)]
	fn advise_random(&self) {
		// Best effort, failure is not critical.
		let _ = self.0.advise(memmap2::Advice::Random);
	}

	#[cfg(not(unix))]
	fn advise_random(&self) {}

	/// Length of the mapping in bytes.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Flush the whole mapping to disk.
	pub fn flush(&self) -> Result<()> {
		try_io!(self.0.flush());
		Ok(())
	}

	/// Flush `len` bytes starting at `offset` to disk.
	pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
		try_io!(self.0.flush_range(offset, len));
		Ok(())
	}

	#[inline]
	fn check_range(&self, offset: usize, len: usize) {
		let end = offset.checked_add(len).expect("mmap range overflow");
		assert!(
			end <= self.len(),
			"mmap range out of bounds: {}..{} of {}",
			offset,
			end,
			self.len()
		);
	}

	/// Returns a shared slice of `len` bytes starting at `offset`.
	///
	/// Panics if the range is outside of the mapping.
	#[inline]
	pub fn slice(&self, offset: usize, len: usize) -> &[u8] {
		self.check_range(offset, len);
		// SAFETY: The range has been checked to be within the mapping, which is valid for reads for
		// as long as `self` is alive. The pointer is derived from `as_ptr` on each call, so the
		// resulting borrow only covers the requested range.
		unsafe { std::slice::from_raw_parts(self.0.as_ptr().add(offset), len) }
	}

	/// Returns a mutable slice of `len` bytes starting at `offset`.
	///
	/// Panics if the range is outside of the mapping.
	///
	/// # Safety
	///
	/// This takes `&self` so that writes can happen while other threads hold shared references to
	/// disjoint parts of the mapping. The caller must guarantee that no other slice of the mapping
	/// overlapping `offset..offset + len` exists for as long as the returned slice is alive,
	/// whether produced by [`Mmap::slice`], [`Mmap::slice_mut`], [`Mmap::read_at`] or
	/// [`Mmap::write_at`], on this or any other thread.
	#[inline]
	#[allow(clippy::mut_from_ref)]
	pub unsafe fn slice_mut(&self, offset: usize, len: usize) -> &mut [u8] {
		self.check_range(offset, len);
		// SAFETY: The range has been checked to be within the mapping, which is valid for writes
		// for as long as `self` is alive. `MmapRaw::as_mut_ptr` yields a pointer with write
		// provenance from a shared reference, and the caller guarantees exclusivity of the range.
		std::slice::from_raw_parts_mut(self.0.as_mut_ptr().add(offset), len)
	}

	/// Copy `buf.len()` bytes starting at `offset` into `buf`.
	///
	/// Panics if the range is outside of the mapping.
	#[inline]
	pub fn read_at(&self, buf: &mut [u8], offset: usize) {
		buf.copy_from_slice(self.slice(offset, buf.len()));
	}

	/// Copy `buf` into the mapping at `offset`.
	///
	/// Panics if the range is outside of the mapping.
	///
	/// # Safety
	///
	/// Same requirements as [`Mmap::slice_mut`] for the range `offset..offset + buf.len()`.
	#[inline]
	pub unsafe fn write_at(&self, buf: &[u8], offset: usize) {
		self.slice_mut(offset, buf.len()).copy_from_slice(buf);
	}
}

#[cfg(test)]
mod test {
	use super::Mmap;

	fn temp_file(len: u64) -> (tempfile::TempDir, std::fs::File) {
		let dir = tempfile::tempdir().unwrap();
		let file = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.open(dir.path().join("map"))
			.unwrap();
		file.set_len(len).unwrap();
		(dir, file)
	}

	#[test]
	fn read_write_roundtrip() {
		let (_dir, file) = temp_file(64);
		let map = Mmap::map(&file).unwrap();
		assert_eq!(map.len(), 64);
		// Interleave reads and writes through a shared reference, as the tables do.
		let before = map.slice(0, 16);
		assert_eq!(before, &[0u8; 16]);
		unsafe { map.write_at(&[1, 2, 3, 4], 8) };
		assert_eq!(map.slice(8, 4), &[1, 2, 3, 4]);
		let mut buf = [0u8; 4];
		map.read_at(&mut buf, 8);
		assert_eq!(buf, [1, 2, 3, 4]);
		unsafe { map.slice_mut(16, 4) }.copy_from_slice(&[5, 6, 7, 8]);
		assert_eq!(
			map.slice(0, 24),
			&[0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8, 0, 0, 0, 0]
		);
		map.flush().unwrap();
		map.flush_range(8, 8).unwrap();
	}

	#[test]
	fn growable_reserves_address_space() {
		let (_dir, file) = temp_file(16);
		let map = Mmap::map_growable(&file, 16).unwrap();
		assert!(map.len() >= 16);
		unsafe { map.write_at(&[9; 16], 0) };
		assert_eq!(map.slice(0, 16), &[9; 16]);
	}

	#[test]
	#[should_panic(expected = "out of bounds")]
	fn slice_out_of_bounds_panics() {
		let (_dir, file) = temp_file(16);
		let map = Mmap::map(&file).unwrap();
		let _ = map.slice(8, 16);
	}
}
