// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::column::ColId;
#[cfg(feature = "instrumentation")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, io, sync::Arc};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
	Io(io::Error),
	Corruption(String),
	InvalidConfiguration(String),
	IncompatibleColumnConfig { id: ColId, reason: String },
	InvalidInput(String),
	InvalidValueData,
	Background(Arc<Error>),
	Locked(io::Error),
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
			Error::IncompatibleColumnConfig { id, reason } =>
				write!(f, "Invalid column {} configuration : {}", id, reason),
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

#[cfg(feature = "instrumentation")]
pub static IO_COUNTER_BEFORE_ERROR: AtomicUsize = AtomicUsize::new(usize::MAX);

#[cfg(feature = "instrumentation")]
pub fn set_number_of_allowed_io_operations(val: usize) {
	IO_COUNTER_BEFORE_ERROR.store(val, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
macro_rules! try_io {
	($e:expr) => {{
		if crate::error::IO_COUNTER_BEFORE_ERROR
			.fetch_update(
				::std::sync::atomic::Ordering::SeqCst,
				::std::sync::atomic::Ordering::SeqCst,
				|v| Some(v.saturating_sub(1)),
			)
			.unwrap() == 0
		{
			Err(crate::error::Error::Io(::std::io::Error::new(
				::std::io::ErrorKind::Other,
				"Instrumented failure",
			)))?
		} else {
			$e.map_err(crate::error::Error::Io)?
		}
	}};
}

#[cfg(not(feature = "instrumentation"))]
macro_rules! try_io {
	($e:expr) => {{
		$e.map_err(crate::error::Error::Io)?
	}};
}

pub(crate) use try_io;
