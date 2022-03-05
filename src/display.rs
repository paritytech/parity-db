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

/// Simple wrapper to display hex representation of bytes.
pub struct HexDisplay<'a>(&'a [u8]);

impl<'a> HexDisplay<'a> {
	pub fn from<R: std::convert::AsRef<[u8]> + ?Sized>(d: &'a R) -> Self {
		HexDisplay(d.as_ref())
	}
}

impl<'a> std::fmt::Display for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			f.write_fmt(format_args!("{:02x}", byte))?;
		}
		Ok(())
	}
}

impl<'a> std::fmt::Debug for HexDisplay<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		for byte in self.0 {
			f.write_fmt(format_args!("{:02x}", byte))?;
		}
		Ok(())
	}
}

pub fn hex<R: std::convert::AsRef<[u8]> + ?Sized>(r: &R) -> HexDisplay<'_> {
	HexDisplay::from(r)
}
