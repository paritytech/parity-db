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


//! Compression utility and types.

/// Different compression type
/// allowend and their u8 representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompressionType {
	NoCompression = 0,
	Lz4 = 1,
	Snappy = 2,
}

/// Compression implementation.
pub(crate) struct Compress {
	inner: Compressor,
	pub treshold: u32,
}

impl Compress {
	pub(crate) fn new(kind: CompressionType, treshold: u32) -> Self {
		Compress {
			inner: kind.into(),
			treshold,
		}
	}
}

enum Compressor {
	NoCompression(NoCompression),
	Lz4(lz4::Lz4),
	Snappy(snappy::Snappy),
}

impl From<u8> for CompressionType {
	fn from(comp_type: u8) -> Self {
		match comp_type {
			a if a == CompressionType::NoCompression as u8 => CompressionType::NoCompression,
			a if a == CompressionType::Lz4 as u8 => CompressionType::Lz4,
			a if a == CompressionType::Snappy as u8 => CompressionType::Snappy,
			_ => panic!("Unkwown compression."),
		}
	}
}

impl From<CompressionType> for Compressor {
	fn from(comp_type: CompressionType) -> Self {
		match comp_type {
			CompressionType::NoCompression => Compressor::NoCompression(NoCompression),
			CompressionType::Lz4 => Compressor::Lz4(lz4::Lz4::new()),
			CompressionType::Snappy => Compressor::Snappy(snappy::Snappy::new()),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

impl From<&Compress> for CompressionType {
	fn from(compression: &Compress) -> Self {
		match compression.inner {
			Compressor::NoCompression(_) => CompressionType::NoCompression,
			Compressor::Lz4(_) => CompressionType::Lz4,
			Compressor::Snappy(_) => CompressionType::Snappy,
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

impl Compress {
	pub(crate) fn compress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.inner {
			Compressor::NoCompression(inner) => inner.compress(buf),
			Compressor::Lz4(inner) => inner.compress(buf),
			Compressor::Snappy(inner) => inner.compress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}

	pub(crate) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.inner {
			Compressor::NoCompression(inner) => inner.decompress(buf),
			Compressor::Lz4(inner) => inner.decompress(buf),
			Compressor::Snappy(inner) => inner.decompress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

struct NoCompression;

impl NoCompression {
	fn compress(&self, buf: &[u8]) -> Vec<u8> {
		buf.to_vec()
	}

	fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		buf.to_vec()
	}
}

mod lz4 {
	pub(super) struct Lz4;

	impl Lz4 {
		pub(super) fn new() -> Self {
			Lz4
		}

		pub(super) fn compress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::compress(buf, Some(lz4::block::CompressionMode::DEFAULT), true)
				.unwrap()
		}

		pub(super) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::decompress(buf, None)
				.unwrap()
		}
	}
}

mod snappy {
    use std::io::{Read, Write};

	pub(super) struct Snappy;

	impl Snappy {
		pub(super) fn new() -> Self {
			Snappy
		}

		pub(super) fn compress(&self, value: &[u8]) -> Vec<u8> {
			let mut buf = Vec::with_capacity(value.len() << 3);
			{
				let mut encoder = snap::write::FrameEncoder::new(&mut buf);
				encoder.write(value).expect("Expect in memory write to succeed.");
			}
			buf
		}

		pub(super) fn decompress(&self, value: &[u8]) -> Vec<u8> {
			let mut buf = Vec::with_capacity(value.len());
			let mut decoder = snap::read::FrameDecoder::new(value);
			decoder.read_to_end(&mut buf).expect("Compression corrupted.");
			buf
		}
	}
}

#[cfg(test)]
mod tests {
    use super::*;

	#[test]
	fn test_compression_interfaces() {
		let original = vec![42;100];
		let types = vec![
			CompressionType::NoCompression,
			CompressionType::Snappy,
			CompressionType::Lz4
		];

		for compression_type in types {
			let compress = Compress::new(compression_type, 0);
			let v = compress.compress(&original[..]);
			assert!(v.len() <= 100);
			let round_tripped = compress.decompress( &v[..]);
			assert_eq!(original, round_tripped);
		}
	}
}
