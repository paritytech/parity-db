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

use std::{fmt, sync::Arc};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
	Io(std::io::Error),
	Corruption(String),
	InvalidConfiguration(String),
	InvalidInput(String),
	InvalidValueData,
	Background(Arc<Error>),
	Locked(std::io::Error),
	Migration(String),
	Compression,
	DatabaseNotFound,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Io(e) => write!(f, "IO Error: {}", e),
			Error::Corruption(e) => write!(f, "Corruption: {}", e),
			Error::InvalidConfiguration(e) => write!(f, "Invalid configuration: {}", e),
			Error::InvalidInput(e) => write!(f, "Invalid input: {}", e),
			Error::InvalidValueData => write!(f, "Invalid data in value table"),
			Error::Background(e) => write!(f, "Background worker error: {}", e),
			Error::Locked(e) => write!(f, "Database file is in use. ({})", e),
			Error::Migration(e) => write!(f, "Migration error: {}", e),
			Error::Compression => write!(f, "Compression error"),
			Error::DatabaseNotFound => write!(f, "Database does not exist"),
		}
	}
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Self {
		Error::Io(e)
	}
}

impl From<std::io::ErrorKind> for Error {
	fn from(e: std::io::ErrorKind) -> Self {
		let e: std::io::Error = e.into();
		e.into()
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::Io(e) => Some(e),
			Error::Background(e) => e.source(),
			Error::Locked(e) => Some(e),
			_ => None,
		}
	}
}
