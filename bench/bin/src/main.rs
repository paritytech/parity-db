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

/*
#[cfg_attr(all(any(target_os = "linux", target_os = "macos"), feature = "jemalloc")  global_allocator)]
#[cfg(all(any(target_os = "linux", target_os = "macos"), feature = "jemalloc"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
*/

struct BenchAdapter(parity_db::Db);

impl db_bench::Db for BenchAdapter {
	fn open(path: &std::path::Path) -> Self {
		BenchAdapter(parity_db::Db::open(path, 1).unwrap())
	}

	fn get(&self, key: &db_bench::Key) -> Option<db_bench::Value> {
		self.0.get(0, key).unwrap()
	}

	fn commit<I: IntoIterator<Item=(db_bench::Key, Option<db_bench::Value>)>>(&self, tx: I) {
		self.0.commit(tx.into_iter().map(|(k, v)| (0, k, v))).unwrap()
	}
}

fn main() {
	db_bench::run::<BenchAdapter>();
}

