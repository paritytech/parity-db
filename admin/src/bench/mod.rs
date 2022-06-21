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

use super::*;
/// Stress subcommand.
use structopt::StructOpt;

mod sizes;

pub use parity_db::{CompressionType, Db, Key, Value};

use rand::{RngCore, SeedableRng};
use std::{
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	thread,
};

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static NEXT_COMMIT: AtomicUsize = AtomicUsize::new(0);
static QUERIES_HIT: AtomicUsize = AtomicUsize::new(0);
static QUERIES_MISS: AtomicUsize = AtomicUsize::new(0);
static ITERATIONS: AtomicUsize = AtomicUsize::new(0);

const COMMIT_SIZE: usize = 100;

const KEY_RESTART: Key = [1u8; 32];

// Out of `COMMIT_SIZE` values `COMMIT_PRUNE_SIZE` will be deleted in a later commit.
// The rest will be queried during the final check.
const COMMIT_PRUNE_SIZE: usize = 90;
const COMMIT_PRUNE_WINDOW: usize = 2000;

/// Stress tests (warning erase db first).
#[derive(Debug, StructOpt)]
pub struct Stress {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Number of reading threads [default: 0].
	#[structopt(long)]
	pub readers: Option<usize>,

	/// Number of iterating threads [default: 0].
	#[structopt(long)]
	pub iter: Option<usize>,

	/// Number of writing threads [default: 1].
	#[structopt(long)]
	pub writers: Option<usize>,

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

	/// Do not check after writing.
	#[structopt(long)]
	pub no_check: bool,

	/// Enable compression.
	#[structopt(long)]
	pub compress: bool,

	/// Use btree index.
	#[structopt(long)]
	pub ordered: bool,
}

#[derive(Clone)]
pub struct Args {
	pub readers: usize,
	pub iter: usize,
	pub commits: usize,
	pub writers: usize,
	pub seed: Option<u64>,
	pub archive: bool,
	pub append: bool,
	pub no_check: bool,
	pub compress: bool,
	pub ordered: bool,
}

impl Stress {
	pub(super) fn get_args(&self) -> Args {
		Args {
			readers: self.readers.unwrap_or(0),
			iter: self.iter.unwrap_or(0),
			writers: self.writers.unwrap_or(1),
			commits: self.commits.unwrap_or(100_000),
			seed: self.seed,
			append: self.append,
			archive: self.archive,
			no_check: self.no_check,
			compress: self.compress,
			ordered: self.ordered,
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

	fn value(&self, seed: u64, compressable: bool) -> Vec<u8> {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let sr = (rng.next_u64() % self.total as u64) as u32;
		let mut range = self
			.distribution
			.range((std::ops::Bound::Included(sr), std::ops::Bound::Unbounded));
		let size = *range.next().unwrap().1 as usize;
		let mut v = Vec::new();
		v.resize(size, 0);
		let fill = if !compressable { size } else { size / 2 };
		rng.fill_bytes(&mut v[..fill]);
		v
	}

	fn key(&self, seed: u64) -> Key {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut key = Key::default();
		rng.fill_bytes(&mut key);
		key
	}
}

fn informant(shutdown: Arc<AtomicBool>, total: usize, start: usize) {
	let mut last = start;
	let mut last_time = std::time::Instant::now();
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
		let commits = COMMITS.load(Ordering::Acquire);
		let now = std::time::Instant::now();
		println!(
			"{}/{} commits, {} cps",
			commits - start,
			total,
			((commits - last) as f64) / (now - last_time).as_secs_f64()
		);
		last = commits;
		last_time = now;
	}
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	pool: Arc<SizePool>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
) {
	let offset = args.seed.unwrap_or(0);
	// Note that multiple worker will run on same range concurrently.
	let commit_size = COMMIT_SIZE;
	let mut commit = Vec::with_capacity(commit_size);

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break
		}

		let mut key = n as u64 * COMMIT_SIZE as u64 + offset;
		for _ in 0..commit_size {
			commit.push((pool.key(key), Some(pool.value(key, args.compress))));
			key += 1;
		}
		if !args.archive && n >= COMMIT_PRUNE_WINDOW {
			let prune_start = (n - COMMIT_PRUNE_WINDOW) * COMMIT_SIZE + offset as usize;
			for p in prune_start..prune_start + COMMIT_PRUNE_SIZE {
				commit.push((pool.key(p as u64), None));
			}
		}
		commit.push((KEY_RESTART, Some((n as u64).to_be_bytes().to_vec())));

		db.commit(commit.drain(..).map(|(k, v)| (0, k, v))).unwrap();
		COMMITS.fetch_add(1, Ordering::Relaxed);
		commit.clear();
	}
}

fn reader(db: Arc<Db>, pool: Arc<SizePool>, seed: u64, index: u64, shutdown: Arc<AtomicBool>) {
	// Query random keys while writing
	let mut rng = rand::rngs::SmallRng::seed_from_u64(seed + index);
	while !shutdown.load(Ordering::Relaxed) {
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue
		}
		let num_keys = commits * COMMIT_SIZE as u64;
		let key = pool.key(rng.next_u64() % num_keys + seed);
		match db.get(0, &key).unwrap() {
			Some(_) => {
				QUERIES_HIT.fetch_add(1, Ordering::SeqCst);
			},
			None => {
				QUERIES_MISS.fetch_add(1, Ordering::SeqCst);
			},
		}
	}
}

fn iter(db: Arc<Db>, shutdown: Arc<AtomicBool>) {
	loop {
		let mut iter = db.iter(0).unwrap();
		while iter.next().unwrap().is_some() {
			if shutdown.load(Ordering::Relaxed) {
				return
			}
		}
		ITERATIONS.fetch_add(1, Ordering::SeqCst);
	}
}

pub fn run_internal(args: Args, db: Db) {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let pool = Arc::new(SizePool::from_histogram(sizes::KUSAMA_STATE_DISTRIBUTION));
	let db = Arc::new(db);
	let start = std::time::Instant::now();

	let mut threads = Vec::new();

	let start_commit = if let Some(start) = db.get(0, &KEY_RESTART).unwrap() {
		let mut buf = [0u8; 8];
		buf.copy_from_slice(&start[0..8]);
		u64::from_be_bytes(buf) as usize + 1
	} else {
		0
	};

	COMMITS.store(start_commit as usize, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit as usize, Ordering::SeqCst);

	{
		let commits = args.commits;
		let start = start_commit;
		let shutdown = shutdown.clone();
		threads.push(thread::spawn(move || informant(shutdown, commits, start)));
	}

	for i in 0..args.readers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let offset = args.seed.unwrap_or(0);
		let pool = pool.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("reader {}", i))
				.spawn(move || reader(db, pool, offset, i as u64, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.iter {
		let db = db.clone();
		let shutdown = shutdown.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("iter {}", i))
				.spawn(move || iter(db, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.writers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let pool = pool.clone();
		let args = args.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("writer {}", i))
				.spawn(move || writer(db, args, pool, shutdown, start_commit))
				.unwrap(),
		);
	}

	while COMMITS.load(Ordering::Relaxed) < start_commit + args.commits {
		thread::sleep(std::time::Duration::from_millis(50));
	}
	shutdown.store(true, Ordering::SeqCst);

	for t in threads.into_iter() {
		t.join().unwrap();
	}

	let commits = COMMITS.load(Ordering::SeqCst);
	let commits = commits - start_commit;
	let elapsed = start.elapsed().as_secs_f64();

	let hits = QUERIES_HIT.load(Ordering::SeqCst);
	let misses = QUERIES_MISS.load(Ordering::SeqCst);
	let iterations = ITERATIONS.load(Ordering::SeqCst);

	println!(
		"Completed {} commits in {} seconds. {} cps. {} hits, {} misses, {} iterations, {} qps",
		commits,
		elapsed,
		commits as f64 / elapsed,
		hits,
		misses,
		iterations,
		(hits + misses) as f64 / elapsed,
	);

	if args.no_check {
		return
	}

	// Verify content
	let start = std::time::Instant::now();
	let pruned_per_commit = if args.archive { 0u64 } else { COMMIT_PRUNE_SIZE as u64 };
	let mut queries = 0;
	for nc in start_commit as u64..(start_commit + commits) as u64 {
		let counter = nc - start_commit as u64;
		if counter % 10000 == 0 {
			println!("Query {}/{}", counter, commits);
		}
		let commits = (start_commit + commits) as u64;
		let prune_window: u64 = COMMIT_PRUNE_WINDOW as u64;
		let offset = args.seed.unwrap_or(0);
		let start = if !args.archive && commits > prune_window && nc < commits - prune_window {
			let end = nc * COMMIT_SIZE as u64 + pruned_per_commit + offset;
			for key in (nc * COMMIT_SIZE as u64) + offset..end {
				let k = pool.key(key);
				let db_val = db.get(0, &k).unwrap();
				queries += 1;
				assert_eq!(None, db_val);
			}
			end
		} else {
			nc * COMMIT_SIZE as u64 + offset
		};
		for key in start..(nc + 1) * (COMMIT_SIZE as u64) + offset {
			let k = pool.key(key);
			let val = pool.value(key, args.compress);
			let db_val = db.get(0, &k).unwrap();
			queries += 1;
			assert_eq!(Some(val), db_val);
		}
	}

	let elapsed = start.elapsed().as_secs_f64();
	println!(
		"Completed {} queries in {} seconds. {} qps",
		queries,
		elapsed,
		queries as f64 / elapsed
	);
}
