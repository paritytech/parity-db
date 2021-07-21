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

use structopt::StructOpt;
use super::*;

/// Stress subcommand.

pub(super) struct BenchAdapter(crate::Db);

impl db_bench::Db for BenchAdapter {
	type Options = crate::options::Options;

	fn open(path: &std::path::Path) -> Self {
		BenchAdapter(crate::Db::with_columns(path, 1).unwrap())
	}

	fn with_options(options: &Self::Options) -> Self {
		BenchAdapter(crate::Db::open(options, true).unwrap())
	}

	fn get(&self, key: &db_bench::Key) -> Option<db_bench::Value> {
		self.0.get(0, key).unwrap()
	}

	fn commit<I: IntoIterator<Item=(db_bench::Key, Option<db_bench::Value>)>>(&self, tx: I) {
		self.0.commit(tx.into_iter().map(|(k, v)| (0, k, v))).unwrap()
	}
}

/// Stress tests (warning erase db first).
#[derive(Debug, StructOpt)]
pub struct Stress {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Number of reading threads [default: 4].
	#[structopt(long)]
	pub readers: Option<usize>,

	/// Number of writing threads [default: 1].
	#[structopt(long)]
	pub writers: Option<usize>,

	/// Start commit index.
	#[structopt(long)]
	pub start_commit: Option<usize>,


	/// Total number of inserted commits.
	#[structopt(long)]
	pub commits: Option<usize>,

	/// Random seed used for key generation.
	#[structopt(long)]
	pub seed: Option<u64>,

	/// Open an existing database.
	#[structopt(long)]
	pub append: bool,

	/// Do not apply pruning.
	#[structopt(long)]
	pub archive: bool,

	/// Do only check (requires append flag).
	#[structopt(long)]
	pub check_only: bool,
}

impl Stress {
	pub(super) fn get_args(&self) -> db_bench::Args {
		db_bench::Args {
			readers: self.readers.unwrap_or(4),
			writers: self.writers.unwrap_or(1),
			commits: self.commits.unwrap_or(100_000),
			seed: self.seed.clone(),
			append: self.append,
			archive: self.archive,
			check_only: self.check_only,
			start_commit: self.start_commit.unwrap_or(0),
		}
	}
}
