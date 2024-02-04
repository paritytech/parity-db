// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use super::*;

pub use parity_db::{Db, Key, TreeReader};
use parity_db::{NewNode, NodeRef, Operation};

use parking_lot::Mutex;

use rand::{RngCore, SeedableRng};
use std::{
	collections::VecDeque,
	sync::{
		atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
		Arc,
	},
	thread,
};
use trie_db::{
	node_db::{Hasher, NodeDB},
	NodeCodec, TrieDBMut, TrieDBMutBuilder,
};

type TrieLayout = reference_trie::GenericNoExtensionLayout<Blake3Hasher, u64>;
type TrieNodeCodec = <TrieLayout as trie_db::TrieLayout>::Codec;

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static NEXT_COMMIT: AtomicUsize = AtomicUsize::new(0);
static NUM_REMOVED: AtomicUsize = AtomicUsize::new(0);
static TARGET_NUM_REMOVED: AtomicUsize = AtomicUsize::new(0);
static QUERIES: AtomicUsize = AtomicUsize::new(0);
static ITERATIONS: AtomicUsize = AtomicUsize::new(0);

type _AccountId = u64;
type _Balance = u128;

pub const TREE_COLUMN: u8 = 0;
pub const INFO_COLUMN: u8 = 1;

const KEY_LAST_COMMIT: Key = [1u8; 32];
const KEY_NUM_REMOVED: Key = [2u8; 32];

const THREAD_PRUNING: bool = true;

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

	/// Remove all trees on shutdown and wait for the database to be empty.
	#[clap(long)]
	pub empty_on_shutdown: bool,

	/// Number of trees to keep (Older are removed). 0 means never remove. [default: 8]
	#[clap(long)]
	pub pruning: Option<u64>,

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
	pub empty_on_shutdown: bool,
	pub pruning: u64,
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
			empty_on_shutdown: self.empty_on_shutdown,
			pruning: self.pruning.unwrap_or(8),
			compress: self.compress,
			commit_time: self.commit_time.unwrap_or(0),
		}
	}
}

fn informant(
	_db: Arc<Db>,
	_shutdown: Arc<AtomicBool>,
	shutdown_final: Arc<AtomicBool>,
	chain_generator: Arc<ChainGenerator>,
	total: usize,
) -> Result<(), String> {
	let mut last = COMMITS.load(Ordering::Acquire);
	let mut last_acc = chain_generator.max_account_id.load(Ordering::Relaxed);
	let mut last_time = std::time::Instant::now();
	let mut last_ops = chain_generator.ops.load(Ordering::Relaxed);
	while !shutdown_final.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_secs(1));
		let commits = COMMITS.load(Ordering::Acquire);
		let accounts = chain_generator.max_account_id.load(Ordering::Relaxed);
		let ops = chain_generator.ops.load(Ordering::Relaxed);
		let now = std::time::Instant::now();
		println!(
			"{}/{} commits, {:.2} m accounts, {:.2} cps, {:.2} tps, {:.2} ops/acc",
			commits,
			total,
			accounts as f64 / 1_000_000f64,
			((commits - last) as f64) / (now - last_time).as_secs_f64(),
			((accounts - last_acc) as f64) / (now - last_time).as_secs_f64(),
			if accounts != last_acc {
				((ops - last_ops) as f64) / (accounts - last_acc) as f64
			} else {
				0f64
			},
		);
		last = commits;
		last_time = now;
		last_acc = accounts;
		last_ops = ops;
	}
	Ok(())
}

struct ChainGenerator {
	max_account_id: AtomicU64,
	roots: Mutex<VecDeque<[u8; 32]>>,
	ops: AtomicU64,
}
/// Concrete implementation of Hasher using Blake2b 256-bit hashes
#[derive(Debug)]
pub struct Blake3Hasher;

impl trie_db::node_db::Hasher for Blake3Hasher {
	type Out = Key;
	type StdHasher = hash256_std_hasher::Hash256StdHasher;
	const LENGTH: usize = 32;

	fn hash(x: &[u8]) -> Self::Out {
		let mut hasher = blake3::Hasher::new();
		hasher.update(x);
		hasher.finalize().into()
	}
}

struct TrieDB {
	db: Arc<Db>,
	ops: AtomicU64,
}

impl NodeDB<Blake3Hasher, Vec<u8>, u64> for TrieDB {
	fn get(
		&self,
		key: &Key,
		_prefix: trie_db::node_db::Prefix,
		location: u64,
	) -> Option<(Vec<u8>, Vec<u64>)> {
		self.ops.fetch_add(1, Ordering::Relaxed);
		if location == 0 {
			self.db.get_root(0, key).unwrap()
		} else {
			self.db.get_node(0, location).unwrap()
		}
	}
}

impl TrieDB {
	fn new(db: Arc<Db>) -> Self {
		Self { db, ops: AtomicU64::new(0) }
	}

	fn ops(&self) -> u64 {
		self.ops.load(Ordering::Relaxed)
	}
}

pub fn convert_to_db_commit(
	commit: trie_db::Changeset<Key, u64>,
	ops: &mut Vec<(u8, Operation<Vec<u8>, Vec<u8>>)>,
) {
	fn convert(node: trie_db::Changeset<Key, u64>) -> NodeRef {
		match node {
			trie_db::Changeset::Existing(node) => NodeRef::Existing(node.location),
			trie_db::Changeset::New(node) => NodeRef::New(NewNode {
				data: node.data,
				children: node.children.into_iter().map(|c| convert(c)).collect(),
			}),
		}
	}

	let root = commit.root_hash();
	match commit {
		trie_db::Changeset::Existing(node) => {
			ops.push((TREE_COLUMN, Operation::ReferenceTree(node.hash.to_vec())));
		},
		new_node @ trie_db::Changeset::New(_) => {
			if let NodeRef::New(n) = convert(new_node) {
				ops.push((TREE_COLUMN, Operation::InsertTree(root.to_vec(), n)));
			}
		},
	}
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
) -> Result<(), String> {
	let mut commit = Vec::new();

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break;
		}

		let root = chain_generator.roots.lock().back().cloned();

		let tdb = TrieDB::new(Arc::clone(&db));
		let mut trie: TrieDBMut<reference_trie::GenericNoExtensionLayout<Blake3Hasher, u64>> =
			if let Some(root) = root {
				TrieDBMutBuilder::from_existing(&tdb, root).build()
			} else {
				TrieDBMutBuilder::new(&tdb).build()
			};

		for _i in 0..10000 {
			let account_id = chain_generator.max_account_id.fetch_add(1, Ordering::SeqCst);
			let key = Blake3Hasher::hash(&account_id.to_be_bytes());
			let value = account_id.to_be_bytes();
			trie.insert(&key, &value).unwrap();
		}

		let trie_commit = trie.commit();
		let root = trie_commit.root_hash();
		let ops = tdb.ops();
		convert_to_db_commit(trie_commit, &mut commit);
		db.commit_changes(commit.drain(..)).unwrap();
		chain_generator.roots.lock().push_back(root);
		chain_generator.ops.fetch_add(ops, Ordering::SeqCst);

		COMMITS.fetch_add(1, Ordering::SeqCst);
		commit.clear();

		/*
			if args.pruning > 0 && !THREAD_PRUNING {
				try_prune(&db, &args, &chain_generator, &mut commit, &output_helper)?;
			}

			if args.commit_time > 0 {
				thread::sleep(std::time::Duration::from_millis(args.commit_time));
			}
		}*/
	}

	Ok(())
}

fn try_prune(
	db: &Db,
	args: &Args,
	chain_generator: &ChainGenerator,
	commit: &mut Vec<(u8, Operation<Vec<u8>, Vec<u8>>)>,
) -> Result<(), String> {
	let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
	let target_override = TARGET_NUM_REMOVED.load(Ordering::Relaxed);
	let commits = COMMITS.load(Ordering::Relaxed);

	let target_num_removed = if target_override > 0 {
		target_override as u64
	} else {
		if commits as u64 > args.pruning {
			commits as u64 - args.pruning
		} else {
			0
		}
	};

	if target_num_removed > num_removed as u64 {
		// Need to remove a tree
		let key = chain_generator.roots.lock().pop_front().unwrap();

		commit.push((TREE_COLUMN, Operation::DereferenceTree(key.to_vec())));
		commit.push((
			INFO_COLUMN,
			Operation::Set(
				KEY_NUM_REMOVED.to_vec(),
				((num_removed + 1) as u64).to_be_bytes().to_vec(),
			),
		));

		NUM_REMOVED.fetch_add(1, Ordering::SeqCst);
		db.commit_changes(commit.drain(..)).unwrap();
		commit.clear();
	}

	Ok(())
}

fn pruner(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	let mut commit = Vec::new();

	while !shutdown.load(Ordering::Relaxed) {
		try_prune(&db, &args, &chain_generator, &mut commit)?;
	}

	Ok(())
}

fn reader(
	_db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Query random values from random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue;
		}

		let _account_id = rng.next_u64() % chain_generator.max_account_id.load(Ordering::Relaxed);
		// TODO: query trie
	}

	Ok(())
}

fn iter(
	_db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Iterate over nodes in random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let _account_id = rng.next_u64() % chain_generator.max_account_id.load(Ordering::Relaxed);
		// TODO: query trie
	}

	Ok(())
}

pub fn run_internal(args: Args, db: Db) -> Result<(), String> {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let shutdown_final = Arc::new(AtomicBool::new(false));
	let db = Arc::new(db);

	db.commit_changes([(
		TREE_COLUMN,
		Operation::InsertTree(
			TrieNodeCodec::hashed_null_node().to_vec(),
			NewNode { data: TrieNodeCodec::empty_node().to_vec(), children: vec![] },
		),
	)])
	.unwrap();

	let mut threads = Vec::new();

	let start_commit = if let Some(start) = db.get(INFO_COLUMN, &KEY_LAST_COMMIT).unwrap() {
		let mut buf = [0u8; 8];
		buf.copy_from_slice(&start[0..8]);
		u64::from_be_bytes(buf) as usize + 1
	} else {
		0
	};

	let num_removed = if let Some(start) = db.get(INFO_COLUMN, &KEY_NUM_REMOVED).unwrap() {
		let mut buf = [0u8; 8];
		buf.copy_from_slice(&start[0..8]);
		u64::from_be_bytes(buf) as usize
	} else {
		0
	};

	let chain_generator = ChainGenerator {
		max_account_id: AtomicU64::new(0),
		roots: Mutex::new(VecDeque::new()),
		ops: AtomicU64::new(0),
	};
	let chain_generator = Arc::new(chain_generator);

	let start_time = std::time::Instant::now();

	COMMITS.store(start_commit, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit, Ordering::SeqCst);
	NUM_REMOVED.store(num_removed, Ordering::SeqCst);

	{
		let db = db.clone();
		let shutdown = shutdown.clone();
		let shutdown_final = shutdown_final.clone();
		let total = args.commits;
		let chain_generator = chain_generator.clone();

		threads.push(thread::spawn(move || {
			informant(db, shutdown, shutdown_final, chain_generator, total)
		}));
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

	let iter_start_index = args.readers;
	for i in 0..args.iter {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("iter {i}"))
				.spawn(move || {
					iter(db, args, chain_generator, (iter_start_index + i) as u64, shutdown)
				})
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

	if args.pruning > 0 && THREAD_PRUNING {
		let db = db.clone();
		let shutdown = shutdown_final.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("pruner"))
				.spawn(move || pruner(db, args, chain_generator, shutdown))
				.unwrap(),
		);
	}

	while COMMITS.load(Ordering::Relaxed) < start_commit + args.commits {
		thread::sleep(std::time::Duration::from_millis(50));
	}
	shutdown.store(true, Ordering::SeqCst);

	let commits = COMMITS.load(Ordering::SeqCst);
	let commits = commits - start_commit;
	let elapsed_time = start_time.elapsed().as_secs_f64();

	let queries = QUERIES.load(Ordering::SeqCst);
	let iterations = ITERATIONS.load(Ordering::SeqCst);

	println!(
		"Completed {} commits in {} seconds. {} cps. {} queries, {} iterations",
		commits,
		elapsed_time,
		commits as f64 / elapsed_time,
		queries,
		iterations
	);

	if args.empty_on_shutdown && args.pruning > 0 {
		// Continue removing trees until they are all gone.
		TARGET_NUM_REMOVED.store(start_commit + args.commits, Ordering::SeqCst);
		while NUM_REMOVED.load(Ordering::Relaxed) < start_commit + args.commits {
			thread::sleep(std::time::Duration::from_millis(50));
		}

		// Wait for all entries to actually be removed from Db.
		while !shutdown_final.load(Ordering::Relaxed) {
			thread::sleep(std::time::Duration::from_millis(50));
		}
	} else {
		shutdown_final.store(true, Ordering::SeqCst);
	}

	for t in threads.into_iter() {
		t.join().unwrap()?;
	}

	if args.empty_on_shutdown && args.pruning > 0 {
		let elapsed_time = start_time.elapsed().as_secs_f64();
		println!("Removed all entries. Total time: {}", elapsed_time);
	}

	Ok(())
}
