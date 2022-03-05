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

use crate::{column::ColId, error::Result, table::SIZE_TIERS};
/// Database statistics.
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::{
	io::{Cursor, Read, Write},
	mem::MaybeUninit,
};

// store up to value of size HISTOGRAM_BUCKETS * 2 ^ HISTOGRAM_BUCKET_BITS,
// that is 32ko
const HISTOGRAM_BUCKETS: usize = 1024;
const HISTOGRAM_BUCKET_BITS: u8 = 5;

pub const TOTAL_SIZE: usize =
	4 * HISTOGRAM_BUCKETS + 8 * HISTOGRAM_BUCKETS + 8 * SIZE_TIERS + 8 * 11;

// TODO: get rid of the struct and use index meta directly.
pub struct ColumnStats {
	value_histogram: [AtomicU32; HISTOGRAM_BUCKETS],
	query_histogram: [AtomicU64; SIZE_TIERS], // Per size tier
	oversized: AtomicU64,
	oversized_bytes: AtomicU64,
	total_values: AtomicU64,
	total_bytes: AtomicU64,
	commits: AtomicU64,
	inserted_new: AtomicU64,
	inserted_overwrite: AtomicU64,
	removed_hit: AtomicU64,
	removed_miss: AtomicU64,
	queries_miss: AtomicU64,
	uncompressed_bytes: AtomicU64,
	compression_delta: [AtomicI64; HISTOGRAM_BUCKETS],
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> AtomicU32 {
	let mut buf = [0u8; 4];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicU32::new(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> AtomicU64 {
	let mut buf = [0u8; 8];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicU64::new(u64::from_le_bytes(buf))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> AtomicI64 {
	let mut buf = [0u8; 8];
	cursor.read_exact(&mut buf).expect("Incorrect stats buffer");
	AtomicI64::new(i64::from_le_bytes(buf))
}

fn write_u32(cursor: &mut Cursor<&mut [u8]>, val: &AtomicU32) {
	cursor
		.write_all(&val.load(Ordering::Relaxed).to_le_bytes())
		.expect("Incorrect stats buffer");
}

fn write_u64(cursor: &mut Cursor<&mut [u8]>, val: &AtomicU64) {
	cursor
		.write_all(&val.load(Ordering::Relaxed).to_le_bytes())
		.expect("Incorrect stats buffer");
}

fn write_i64(cursor: &mut Cursor<&mut [u8]>, val: &AtomicI64) {
	cursor
		.write_all(&val.load(Ordering::Relaxed).to_le_bytes())
		.expect("Incorrect stats buffer");
}

fn value_histogram_index(size: u32) -> Option<usize> {
	let bucket = size as usize >> HISTOGRAM_BUCKET_BITS;
	if bucket < HISTOGRAM_BUCKETS {
		Some(bucket)
	} else {
		None
	}
}

impl ColumnStats {
	#[allow(clippy::uninit_assumed_init)]
	pub fn from_slice(data: &[u8]) -> ColumnStats {
		let mut cursor = Cursor::new(data);
		let mut value_histogram: [AtomicU32; HISTOGRAM_BUCKETS] =
			unsafe { MaybeUninit::uninit().assume_init() };
		for item in value_histogram.iter_mut() {
			*item = read_u32(&mut cursor);
		}
		let mut query_histogram: [AtomicU64; SIZE_TIERS] =
			unsafe { MaybeUninit::uninit().assume_init() };
		for item in query_histogram.iter_mut() {
			*item = read_u64(&mut cursor);
		}
		let mut stats = ColumnStats {
			value_histogram,
			query_histogram,
			oversized: read_u64(&mut cursor),
			oversized_bytes: read_u64(&mut cursor),
			total_values: read_u64(&mut cursor),
			total_bytes: read_u64(&mut cursor),
			commits: read_u64(&mut cursor),
			inserted_new: read_u64(&mut cursor),
			inserted_overwrite: read_u64(&mut cursor),
			removed_hit: read_u64(&mut cursor),
			removed_miss: read_u64(&mut cursor),
			queries_miss: read_u64(&mut cursor),
			uncompressed_bytes: read_u64(&mut cursor),
			compression_delta: unsafe { MaybeUninit::uninit().assume_init() },
		};
		for item in stats.compression_delta.iter_mut() {
			*item = read_i64(&mut cursor);
		}
		stats
	}

	pub fn empty() -> ColumnStats {
		let value_histogram: [AtomicU32; HISTOGRAM_BUCKETS] =
			unsafe { std::mem::transmute([0u32; HISTOGRAM_BUCKETS]) };
		let query_histogram: [AtomicU64; SIZE_TIERS] =
			unsafe { std::mem::transmute([0u64; SIZE_TIERS]) };
		ColumnStats {
			value_histogram,
			query_histogram,
			oversized: Default::default(),
			oversized_bytes: Default::default(),
			total_values: Default::default(),
			total_bytes: Default::default(),
			commits: Default::default(),
			inserted_new: Default::default(),
			inserted_overwrite: Default::default(),
			removed_hit: Default::default(),
			removed_miss: Default::default(),
			queries_miss: Default::default(),
			uncompressed_bytes: Default::default(),
			compression_delta: unsafe { std::mem::transmute([0i64; HISTOGRAM_BUCKETS]) },
		}
	}

	pub fn to_slice(&self, data: &mut [u8]) {
		let mut cursor = Cursor::new(data);
		for item in &self.value_histogram {
			write_u32(&mut cursor, item);
		}
		for item in &self.query_histogram {
			write_u64(&mut cursor, item);
		}
		write_u64(&mut cursor, &self.oversized);
		write_u64(&mut cursor, &self.oversized_bytes);
		write_u64(&mut cursor, &self.total_values);
		write_u64(&mut cursor, &self.total_bytes);
		write_u64(&mut cursor, &self.commits);
		write_u64(&mut cursor, &self.inserted_new);
		write_u64(&mut cursor, &self.inserted_overwrite);
		write_u64(&mut cursor, &self.removed_hit);
		write_u64(&mut cursor, &self.removed_miss);
		write_u64(&mut cursor, &self.queries_miss);
		write_u64(&mut cursor, &self.uncompressed_bytes);
		for item in &self.compression_delta {
			write_i64(&mut cursor, item);
		}
	}

	fn write_stats(&self, writer: &mut impl std::io::Write, col: ColId) -> Result<()> {
		writeln!(writer, "Column {}", col)?;
		writeln!(writer, "Total values: {}", self.total_values.load(Ordering::Relaxed))?;
		writeln!(writer, "Total bytes: {}", self.total_bytes.load(Ordering::Relaxed))?;
		writeln!(writer, "Total oversized values: {}", self.oversized.load(Ordering::Relaxed))?;
		writeln!(
			writer,
			"Total oversized bytes: {}",
			self.oversized_bytes.load(Ordering::Relaxed)
		)?;
		writeln!(writer, "Total commits: {}", self.commits.load(Ordering::Relaxed))?;
		writeln!(writer, "New value insertions: {}", self.inserted_new.load(Ordering::Relaxed))?;
		writeln!(
			writer,
			"Existing value insertions: {}",
			self.inserted_overwrite.load(Ordering::Relaxed)
		)?;
		writeln!(writer, "Removals: {}", self.removed_hit.load(Ordering::Relaxed))?;
		writeln!(writer, "Missed removals: {}", self.removed_miss.load(Ordering::Relaxed))?;
		writeln!(
			writer,
			"Uncompressed bytes: {}",
			self.uncompressed_bytes.load(Ordering::Relaxed)
		)?;
		writeln!(writer, "Compression deltas:")?;
		for i in 0..HISTOGRAM_BUCKETS {
			let count = self.value_histogram[i].load(Ordering::Relaxed);
			let delta = self.compression_delta[i].load(Ordering::Relaxed);
			if count != 0 && delta != 0 {
				writeln!(
					writer,
					"    {}-{}: {}",
					i << HISTOGRAM_BUCKET_BITS,
					(((i + 1) << HISTOGRAM_BUCKET_BITS) - 1),
					delta
				)?;
			}
		}
		write!(writer, "Queries per size tier: [")?;
		for i in 0..SIZE_TIERS {
			if i == SIZE_TIERS - 1 {
				writeln!(writer, "{}]", self.query_histogram[i].load(Ordering::Relaxed))?;
			} else {
				write!(writer, "{}, ", self.query_histogram[i].load(Ordering::Relaxed))?;
			}
		}
		writeln!(writer, "Missed queries: {}", self.queries_miss.load(Ordering::Relaxed))?;
		writeln!(writer, "Value histogram:")?;
		for i in 0..HISTOGRAM_BUCKETS {
			let count = self.value_histogram[i].load(Ordering::Relaxed);
			if count != 0 {
				writeln!(
					writer,
					"    {}-{}: {}",
					i << HISTOGRAM_BUCKET_BITS,
					(((i + 1) << HISTOGRAM_BUCKET_BITS) - 1),
					count
				)?;
			}
		}
		writeln!(writer)?;
		Ok(())
	}

	pub fn write_summary(&self, writer: &mut impl std::io::Write, col: ColId) {
		let _ = self.write_stats(writer, col);
	}

	pub fn query_hit(&self, size_tier: u8) {
		self.query_histogram[size_tier as usize].fetch_add(1, Ordering::Relaxed);
	}

	pub fn query_miss(&self) {
		self.queries_miss.fetch_add(1, Ordering::Relaxed);
	}

	pub fn insert(&self, size: u32, compressed: u32) {
		if let Some(index) = value_histogram_index(size) {
			self.value_histogram[index].fetch_add(1, Ordering::Relaxed);
			self.compression_delta[index]
				.fetch_add(size as i64 - compressed as i64, Ordering::Relaxed);
		} else {
			self.oversized.fetch_add(1, Ordering::Relaxed);
			self.oversized_bytes.fetch_add(compressed as u64, Ordering::Relaxed);
		}
		self.total_values.fetch_add(1, Ordering::Relaxed);
		self.total_bytes.fetch_add(compressed as u64, Ordering::Relaxed);
		self.uncompressed_bytes.fetch_add(size as u64, Ordering::Relaxed);
	}

	pub fn remove(&self, size: u32, compressed: u32) {
		if let Some(index) = value_histogram_index(size) {
			self.value_histogram[index].fetch_sub(1, Ordering::Relaxed);
			self.compression_delta[index]
				.fetch_sub(size as i64 - compressed as i64, Ordering::Relaxed);
		} else {
			self.oversized.fetch_sub(1, Ordering::Relaxed);
			self.oversized_bytes.fetch_sub(compressed as u64, Ordering::Relaxed);
		}
		self.total_values.fetch_sub(1, Ordering::Relaxed);
		self.total_bytes.fetch_sub(compressed as u64, Ordering::Relaxed);
		self.uncompressed_bytes.fetch_sub(size as u64, Ordering::Relaxed);
	}

	pub fn insert_val(&self, size: u32, compressed: u32) {
		self.inserted_new.fetch_add(1, Ordering::Relaxed);
		self.insert(size, compressed);
	}

	pub fn remove_val(&self, size: u32, compressed: u32) {
		self.removed_hit.fetch_add(1, Ordering::Relaxed);
		self.remove(size, compressed);
	}

	pub fn remove_miss(&self) {
		self.removed_miss.fetch_add(1, Ordering::Relaxed);
	}

	pub fn replace_val(&self, old: u32, old_compressed: u32, new: u32, new_compressed: u32) {
		self.inserted_overwrite.fetch_add(1, Ordering::Relaxed);
		self.remove(old, old_compressed);
		self.insert(new, new_compressed);
	}

	pub fn commit(&self) {
		self.commits.fetch_add(1, Ordering::Relaxed);
	}
}
