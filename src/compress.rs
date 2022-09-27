// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Compression utility and types.

use crate::error::Result;

/// Different compression type
/// allowed and their u8 representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompressionType {
	NoCompression = 0,
	Lz4 = 1,
	Snappy = 2,
}

/// Compression implementation.
#[derive(Debug)]
pub struct Compress {
	inner: Compressor,
	pub threshold: u32,
}

impl Compress {
	pub fn new(kind: CompressionType, threshold: u32) -> Self {
		Compress { inner: kind.into(), threshold }
	}
}

pub const NO_COMPRESSION: Compress =
	Compress { inner: Compressor::NoCompression(NoCompression), threshold: u32::MAX };

#[derive(Debug)]
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
			_ => panic!("Unknown compression."),
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
	pub fn compress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.inner {
			Compressor::NoCompression(inner) => inner.compress(buf),
			Compressor::Lz4(inner) => inner.compress(buf),
			Compressor::Snappy(inner) => inner.compress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}

	pub fn decompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
		Ok(match &self.inner {
			Compressor::NoCompression(inner) => inner.decompress(buf)?,
			Compressor::Lz4(inner) => inner.decompress(buf)?,
			Compressor::Snappy(inner) => inner.decompress(buf)?,
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		})
	}
}

#[derive(Debug)]
struct NoCompression;

impl NoCompression {
	fn compress(&self, buf: &[u8]) -> Vec<u8> {
		buf.to_vec()
	}

	fn decompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
		Ok(buf.to_vec())
	}
}

mod lz4 {
	use crate::error::{Error, Result};

	#[derive(Debug)]
	pub(super) struct Lz4;

	impl Lz4 {
		pub(super) fn new() -> Self {
			Lz4
		}

		pub(super) fn compress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::compress(buf, Some(lz4::block::CompressionMode::DEFAULT), true).unwrap()
		}

		pub(super) fn decompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
			lz4::block::decompress(buf, None).map_err(|_| Error::Compression)
		}
	}
}

mod snappy {
	use crate::error::{Error, Result};
	use std::io::{Read, Write};

	#[derive(Debug)]
	pub(super) struct Snappy;

	impl Snappy {
		pub(super) fn new() -> Self {
			Snappy
		}

		pub(super) fn compress(&self, value: &[u8]) -> Vec<u8> {
			let mut buf = Vec::with_capacity(value.len() << 3);
			{
				let mut encoder = snap::write::FrameEncoder::new(&mut buf);
				encoder.write_all(value).expect("Expect in memory write to succeed.");
			}
			buf
		}

		pub(super) fn decompress(&self, value: &[u8]) -> Result<Vec<u8>> {
			let mut buf = Vec::with_capacity(value.len());
			let mut decoder = snap::read::FrameDecoder::new(value);
			decoder.read_to_end(&mut buf).map_err(|_| Error::Compression)?;
			Ok(buf)
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compression_interfaces() {
		let original = vec![42; 100];
		let types =
			vec![CompressionType::NoCompression, CompressionType::Snappy, CompressionType::Lz4];

		for compression_type in types {
			let compress = Compress::new(compression_type, 0);
			let v = compress.compress(&original[..]);
			assert!(v.len() <= 100);
			let round_tripped = compress.decompress(&v[..]).unwrap();
			assert_eq!(original, round_tripped);
		}
	}
}
