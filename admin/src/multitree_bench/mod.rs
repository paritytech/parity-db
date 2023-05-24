// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use super::*;

mod data;

pub use parity_db::{CompressionType, Db, Key, Value};
use parity_db::{NewNode, NodeRef, Operation};

use rand::{RngCore, SeedableRng};
use std::{
	collections::BTreeMap,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	thread,
};

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static NEXT_COMMIT: AtomicUsize = AtomicUsize::new(0);

/// Stress tests (warning erase db first).
#[derive(Debug, clap::Parser)]
pub struct MultiTreeStress {
	#[clap(flatten)]
	pub shared: Shared,

	/// Number of reading threads [default: 0].
	#[clap(long)]
	pub readers: Option<usize>,

	/// Number of iterating threads [default: 0].
	#[clap(long)]
	pub iter: Option<usize>,

	/// Number of writing threads [default: 1].
	#[clap(long)]
	pub writers: Option<usize>,

	/// Total number of inserted commits.
	#[clap(long)]
	pub commits: Option<usize>,

	/// Random seed used for key generation.
	#[clap(long)]
	pub seed: Option<u64>,

	/// Open an existing database.
	#[clap(long)]
	pub append: bool,

	/// Do not apply pruning.
	#[clap(long)]
	pub archive: bool,

	/// Enable compression.
	#[clap(long)]
	pub compress: bool,

	/// Time (in milliseconds) between commits.
	#[clap(long)]
	pub commit_time: Option<u64>,
}

#[derive(Clone)]
pub struct Args {
	pub readers: usize,
	pub iter: usize,
	pub writers: usize,
	pub commits: usize,
	pub seed: Option<u64>,
	pub append: bool,
	pub archive: bool,
	pub compress: bool,
	pub commit_time: u64,
}

impl MultiTreeStress {
	pub(super) fn get_args(&self) -> Args {
		Args {
			readers: self.readers.unwrap_or(0),
			iter: self.iter.unwrap_or(0),
			writers: self.writers.unwrap_or(1),
			commits: self.commits.unwrap_or(100_000),
			seed: self.seed,
			append: self.append,
			archive: self.archive,
			compress: self.compress,
			commit_time: self.commit_time.unwrap_or(0),
		}
	}
}

fn sample_histogram(rnd: u64, histogram: &BTreeMap<u32, u32>, total: u32) -> u32 {
	let sr = (rnd % total as u64) as u32;
	let mut range = histogram.range((std::ops::Bound::Included(sr), std::ops::Bound::Unbounded));
	let size = *range.next().unwrap().1;
	size
}

struct ChainGenerator {
	depth_child_count_distribution: Vec<BTreeMap<u32, u32>>,
	depth_child_count_total: Vec<u32>,
	value_length_distribution: BTreeMap<u32, u32>,
	value_length_total: u32,
}

impl ChainGenerator {
	fn new(
		depth_child_count_histogram: &[(u32, [u32; 17])],
		value_length_histogram: &[(u32, u32)],
	) -> ChainGenerator {
		let mut depth_child_count_distribution = Vec::default();
		let mut depth_child_count_total = Vec::default();
		for (depth, histogram) in depth_child_count_histogram {
			assert_eq!(*depth, depth_child_count_distribution.len() as u32);

			let mut distribution = BTreeMap::default();
			let mut total = 0;
			for i in 0..histogram.len() {
				let size = i as u32;
				let count = histogram[i];
				total += count;
				if count > 0 {
					distribution.insert(total, size);
				}
			}

			depth_child_count_distribution.push(distribution);
			depth_child_count_total.push(total);
		}

		let mut value_length_distribution = BTreeMap::default();
		let mut value_length_total = 0;
		for (size, count) in value_length_histogram {
			value_length_total += count;
			if *count > 0 {
				value_length_distribution.insert(value_length_total, *size);
			}
		}

		ChainGenerator {
			depth_child_count_distribution,
			depth_child_count_total,
			value_length_distribution,
			value_length_total,
		}
	}

	fn key(&self, seed: u64) -> Key {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut key = Key::default();
		rng.fill_bytes(&mut key);
		key
	}

	/// Returns tuple of node data and child seeds
	fn generate_node(&self, seed: u64, depth: u32) -> (Vec<u8>, Vec<u64>) {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

		let num_children = if depth < self.depth_child_count_distribution.len() as u32 {
			sample_histogram(
				rng.next_u64(),
				&self.depth_child_count_distribution[depth as usize],
				self.depth_child_count_total[depth as usize],
			)
		} else {
			0
		};
		let mut children_seeds = Vec::default();
		for _i in 0..num_children {
			children_seeds.push(rng.next_u64());
		}

		let mut size = 4;
		if num_children == 0 {
			size = sample_histogram(
				rng.next_u64(),
				&self.value_length_distribution,
				self.value_length_total,
			) as usize;
		}
		let mut v = Vec::new();
		v.resize(size, 0);
		let fill = size;
		rng.fill_bytes(&mut v[..fill]);

		(v, children_seeds)
	}
}

fn informant(shutdown: Arc<AtomicBool>) {
	while !shutdown.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
	}
}

fn build_commit_tree(
	node_data: (Vec<u8>, Vec<u64>),
	depth: u32,
	chain_generator: &ChainGenerator,
) -> NodeRef {
	let mut children = Vec::default();
	for seed in node_data.1 {
		let child_data = chain_generator.generate_node(seed, depth + 1);
		let child_node = build_commit_tree(child_data, depth + 1, chain_generator);
		children.push(child_node);
	}
	let new_node = NewNode { data: node_data.0, children };
	NodeRef::New(new_node)
}

fn num_new_child_nodes(node: &NodeRef) -> u32 {
	match node {
		NodeRef::New(node) => {
			let mut num = 0;
			for child in &node.children {
				num += num_new_child_nodes(child) + 1;
			}
			num
		},
		NodeRef::Existing(_) => 0,
	}
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
) {
	let offset = args.seed.unwrap_or(0);
	let mut commit = Vec::new();

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break
		}

		let root_seed = n as u64 + offset;
		let node_data = chain_generator.generate_node(root_seed, 0);
		let root_node_ref = build_commit_tree(node_data, 0, &chain_generator);
		let num_new_nodes = num_new_child_nodes(&root_node_ref) + 1;
		println!("Tree commit num new nodes: {}", num_new_nodes);
		if let NodeRef::New(node) = root_node_ref {
			let key = chain_generator.key(root_seed);
			commit.push((0, Operation::InsertTree(key.to_vec(), node)));

			db.commit_changes(commit.drain(..)).unwrap();
			COMMITS.fetch_add(1, Ordering::Relaxed);
			commit.clear();
		}
	}
}

fn reader(
	_db: Arc<Db>,
	_args: Arc<Args>,
	_chain_generator: Arc<ChainGenerator>,
	_index: u64,
	shutdown: Arc<AtomicBool>,
) {
	// Query random keys while writing
	/* let seed = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(seed + index); */
	while !shutdown.load(Ordering::Relaxed) {}
}

fn iter(_db: Arc<Db>, shutdown: Arc<AtomicBool>) {
	while !shutdown.load(Ordering::Relaxed) {}
}

pub fn run_internal(args: Args, db: Db) {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let db = Arc::new(db);

	let mut threads = Vec::new();

	let start_commit = 0;

	let total_num_expected_tree_nodes: u32 =
		data::DEPTH_CHILD_COUNT_HISTOGRAMS.iter().map(|x| x.1.iter().sum::<u32>()).sum();
	println!("Total num expected tree nodes: {}", total_num_expected_tree_nodes);

	let chain_generator =
		ChainGenerator::new(data::DEPTH_CHILD_COUNT_HISTOGRAMS, data::VALUE_LENGTH_HISTOGRAM);
	let chain_generator = Arc::new(chain_generator);

	let start_time = std::time::Instant::now();

	COMMITS.store(start_commit, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit, Ordering::SeqCst);

	{
		let shutdown = shutdown.clone();
		threads.push(thread::spawn(move || informant(shutdown)));
	}

	for i in 0..args.readers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("reader {i}"))
				.spawn(move || reader(db, args, chain_generator, i as u64, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.iter {
		let db = db.clone();
		let shutdown = shutdown.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("iter {i}"))
				.spawn(move || iter(db, shutdown))
				.unwrap(),
		);
	}

	for i in 0..args.writers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("writer {i}"))
				.spawn(move || writer(db, args, chain_generator, shutdown, start_commit))
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
	let elapsed_time = start_time.elapsed().as_secs_f64();

	println!("Completed {} commits in {} seconds.", commits, elapsed_time);
}
