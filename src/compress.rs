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
pub enum CompressType {
	NoCompression = 0,
	Lz4 = 1,
	Lz4High = 2,
	Lz4Low = 3,
}

/// Compression implementation.
pub(crate) struct Compress(Compressor);

enum Compressor {
	NoCompression(NoCompression),
	Lz4(lz4::Lz4),
	Lz4High(lz4::Lz4High),
	Lz4Low(lz4::Lz4Low),
}

impl From<u8> for CompressType {
	fn from(comp_type: u8) -> Self {
		match comp_type {
			a if a == CompressType::NoCompression as u8 => CompressType::NoCompression,
			a if a == CompressType::Lz4 as u8 => CompressType::Lz4,
			a if a == CompressType::Lz4High as u8 => CompressType::Lz4High,
			a if a == CompressType::Lz4Low as u8 => CompressType::Lz4Low,
			_ => panic!("Unkwown compression."),
		}
	}
}

impl From<CompressType> for Compress {
	fn from(comp_type: CompressType) -> Self {
		Compress(match comp_type {
			CompressType::NoCompression => Compressor::NoCompression(NoCompression),
			CompressType::Lz4 => Compressor::Lz4(lz4::Lz4::new()),
			CompressType::Lz4High => Compressor::Lz4High(lz4::Lz4High::new()),
			CompressType::Lz4Low => Compressor::Lz4Low(lz4::Lz4Low::new()),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		})
	}
}

impl From<&Compress> for CompressType {
	fn from(compression: &Compress) -> Self {
		match compression.0 {
			Compressor::NoCompression(_) => CompressType::NoCompression,
			Compressor::Lz4(_) => CompressType::Lz4,
			Compressor::Lz4High(_) => CompressType::Lz4High,
			Compressor::Lz4Low(_) => CompressType::Lz4Low,
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}
}

impl Compress {
	// TODO maybe fn compress(&'a self, buf: &[u8]) -> &'a [u8] {
	pub(crate) fn compress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.0 {
			Compressor::NoCompression(inner) => inner.compress(buf),
			Compressor::Lz4(inner) => inner.compress(buf),
			Compressor::Lz4High(inner) => inner.compress(buf),
			Compressor::Lz4Low(inner) => inner.compress(buf),
			#[allow(unreachable_patterns)]
			_ => unimplemented!("Missing compression implementation."),
		}
	}

	pub(crate) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		match &self.0 {
			Compressor::NoCompression(inner) => inner.decompress(buf),
			Compressor::Lz4(inner) => inner.decompress(buf),
			Compressor::Lz4High(inner) => inner.decompress(buf),
			Compressor::Lz4Low(inner) => inner.decompress(buf),
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
	pub(super) type Lz4 = Lz4Inner<DefaultMode>;
	pub(super) type Lz4High = Lz4Inner<HighMode>;
	pub(super) type Lz4Low = Lz4Inner<LowMode>;

	pub(super) trait Lz4Mode {
		const MODE: lz4::block::CompressionMode;
	}

	pub(super) struct DefaultMode;
	pub(super) struct LowMode;
	pub(super) struct HighMode;

	pub(super) struct Lz4Inner<M>(std::marker::PhantomData<M>);

	impl Lz4Mode for DefaultMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::DEFAULT;
	}
	impl Lz4Mode for HighMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::HIGHCOMPRESSION(9);
	}
	impl Lz4Mode for LowMode {
		const MODE: lz4::block::CompressionMode = lz4::block::CompressionMode::FAST(1);
	}

	impl<M: Lz4Mode> Lz4Inner<M> {
		pub(super) fn new() -> Self {
			Lz4Inner(Default::default())
		}

		pub(super) fn compress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::compress(buf, Some(M::MODE), true)
				.unwrap()
		}

		pub(super) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
			lz4::block::decompress(buf, None)
				.unwrap()
		}
	}
}
