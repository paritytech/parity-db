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

mod sizes;
mod db;

pub use db::{Key, Value, Db};

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Arc, }, thread};
use rand::{SeedableRng, RngCore};

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static QUERIES: AtomicUsize = AtomicUsize::new(0);

const USAGE: &str = "
Usage: stress [--readers=<#>][--writers=<#>] [--entries=<l>]

Options:
	--readers=<#>      Number of reading threads [default: 4].
	--writers=<#>      Number of writing threads [default: 1].
	--entries=<n>      Total numbet of inserted entries.
";

#[derive(Clone)]
struct Args {
	readers: usize,
	entries: usize,
	writers: usize,
}

impl Default for Args {
	fn default() -> Args {
		Args {
			readers: 4,
			writers: 1,
			entries: 1000000,
		}
	}
}

struct SizePool {
	distribution: std::collections::BTreeMap<u32, u32>,
	total: u32,
}

impl SizePool {
	fn from_histogram(h: &[(u32, u32)]) -> SizePool {
		let mut distribution = std::collections::BTreeMap::default();
		let mut total = 0;
		for (size, count) in h {
			total += count;
			distribution.insert(total, *size);
		}
		SizePool { distribution, total }
	}

	fn value(&self, seed: u64) -> Vec<u8> {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let sr = (rng.next_u64() % self.total as u64) as u32;
		let mut range = self.distribution.range((std::ops::Bound::Included(sr), std::ops::Bound::Unbounded));
		let size = *range.next().unwrap().1 as usize;
		let mut v = Vec::new();
		v.resize(size, 0);
		rng.fill_bytes(&mut v);
		v
	}

	fn key(&self, seed: u64) -> db::Key {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut key = db::Key::default();
		rng.fill_bytes(&mut key);
		key
	}
}

fn parse<'a, I, T>(mut iter: I) -> T
where
	I: Iterator<Item = &'a str>,
	T: std::str::FromStr,
	<T as std::str::FromStr>::Err: std::fmt::Debug,
{
	iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
	fn parse() -> Args {
		let mut args = Args::default();
		for raw_arg in std::env::args().skip(1) {
			let mut splits = raw_arg[2..].split('=');
			match splits.next().unwrap() {
				"readers" => args.readers = parse(&mut splits),
				"writers" => args.writers = parse(&mut splits),
				"entries" => args.entries = parse(&mut splits),
				other => panic!("unknown option: {}, {}", other, USAGE),
			}
		}
		args
	}
}

fn informant(shutdown: Arc<AtomicBool>, total: usize) {
	let mut last = 0;
	let mut last_time = std::time::Instant::now();
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
		let commits = COMMITS.load(Ordering::Acquire);
		let now = std::time::Instant::now();
		println!("{}/{} commits, {} cps", commits, total,  ((commits - last) as f64) / (now - last_time).as_secs_f64());
		last = commits;
		last_time = now;
	}
}

fn writer<D: Db>(db: Arc<D>, shutdown: Arc<AtomicBool>) {
	let mut key: u64 = 1;
	let commit_size = 100;
	let pool = SizePool::from_histogram(&sizes::C1);
	let mut commit = Vec::with_capacity(commit_size);
	while !shutdown.load(Ordering::Relaxed) {

		for _ in 0 .. commit_size {
			commit.push((pool.key(key), Some(pool.value(key))));
			key += 1;
		}

		db.commit(commit.drain(..));
		//thread::sleep(std::time::Duration::from_millis(5));

		COMMITS.fetch_add(1, Ordering::Release);
		commit.clear();
	}
}

fn reader<D: Db>(_db: Arc<D>, shutdown: Arc<AtomicBool>) {
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_millis(500));
	}
}

pub fn run<D: Db>() {
	env_logger::try_init().unwrap();
	let args = Args::parse();
	let shutdown = Arc::new(AtomicBool::new(false));
	let path: std::path::PathBuf = "./test_db".into();
	let db = Arc::new(Db::open(path.as_path())) as Arc<D>;
	let start = std::time::Instant::now();

	let mut threads = Vec::new();

	{
		let entries = args.entries;
		let shutdown = shutdown.clone();
		threads.push(thread::spawn(move || informant(shutdown, entries)));
	}

	for i in 0 .. args.readers {
		let db = db.clone();
		let shutdown = shutdown.clone();

		threads.push(
			thread::Builder::new()
			.name(format!("reader {}", i))
			.spawn(move || reader(db, shutdown))
			.unwrap()
		);
	}

	for i in 0 .. args.writers {
		let db = db.clone();
		let shutdown = shutdown.clone();

		threads.push(
			thread::Builder::new()
			.name(format!("writer {}", i))
			.spawn(move || writer(db, shutdown))
			.unwrap()
		);
	}

	while COMMITS.load(Ordering::Relaxed) < args.entries {
		thread::sleep(std::time::Duration::from_millis(50));
	}
	shutdown.store(true, Ordering::SeqCst);

	for t in threads.into_iter() {
		t.join().unwrap();
	}

	let commits = COMMITS.load(Ordering::SeqCst);
	let elapsed = start.elapsed().as_secs_f64();

	println!(
		"Completed {} commits in {} seconds. {} cps",
		commits,
		elapsed,
		commits as f64  / elapsed
	);
}

