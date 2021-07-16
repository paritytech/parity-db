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

/// Database migration.

use std::path::Path;
use crate::{options::Options, db::Db, Error, Result, column::ColId};

const COMMIT_SIZE: usize = 1024;

pub fn migrate(from: &Path, to: &Options) -> Result<()> {
	let mut path: std::path::PathBuf = from.into();
	path.push("metadata");
	let (source_cols, _) = Options::load_metadata(&path)?;
	let source_cols = source_cols.ok_or_else(|| Error::Migration("Error loading source metadata".into()))?;
	if source_cols.len() != to.columns.len() {
		return Err(Error::Migration("Source and dest columns mismatch".into()));
	}

	let mut source_options = Options::with_columns(from, source_cols.len() as u8);
	source_options.columns = source_cols;

	let source = Db::open(&source_options)?;
	let dest = Db::open(to)?;

	let mut ncommits: u64 = 0;
	let mut commit = Vec::with_capacity(COMMIT_SIZE);
	for c in 0 .. source_options.columns.len() as ColId {
		log::info!("Migrating col {}", c);
		let mut last_index = 0;
		source.iter_column_while(c, |index, key, rc, mut value| {
			//TODO: more efficient ref migration
			if rc > 1 {
				log::info!("Migrating rc {}", rc);
			}
			for _ in 0 .. rc {
				let value = std::mem::take(&mut value);
				commit.push((c, key.clone(), Some(value)));
				if commit.len() == COMMIT_SIZE {
					ncommits += 1;
					if let Err(e) = dest.commit_raw(std::mem::take(&mut commit)) {
						log::warn!("Migration error: {:?}", e);
						return false;
					}
					commit.reserve(COMMIT_SIZE);
				}
			}
			if index - last_index >= 10000 {
				log::info!("Migrating {} #{}, commit {}", c, index, ncommits);
				last_index = index;
			}
			true
		})?;
	}
	dest.commit_raw(commit)?;
	Ok(())
}
