// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

/// Simple wrapper to display hex representation of bytes.
pub struct HexDisplay<'a>(&'a [u8]);

impl<'a> HexDisplay<'a> {
	pub fn from<R: AsRef<[u8]> + ?Sized>(d: &'a R) -> Self {
		HexDisplay(d.as_ref())
	}
}

impl<'a> std::fmt::Display for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			write!(f, "{:02x}", byte)?;
		}
		Ok(())
	}
}

impl<'a> std::fmt::Debug for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			write!(f, "{:02x}", byte)?;
		}
		Ok(())
	}
}

pub fn hex<R: AsRef<[u8]> + ?Sized>(r: &R) -> HexDisplay<'_> {
	HexDisplay::from(r)
}
