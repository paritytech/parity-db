// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use super::*;

mod data;

pub use parity_db::{CompressionType, Db, Key, TreeReader, Value};
use parity_db::{NewNode, NodeRef, Operation};

use parking_lot::{RwLock, RwLockReadGuard};

use rand::{RngCore, SeedableRng};
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	io::Write,
	ops::Deref,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	thread,
};

static COMMITS: AtomicUsize = AtomicUsize::new(0);
static NEXT_COMMIT: AtomicUsize = AtomicUsize::new(0);
static NUM_REMOVED: AtomicUsize = AtomicUsize::new(0);
static TARGET_NUM_REMOVED: AtomicUsize = AtomicUsize::new(0);
static QUERIES: AtomicUsize = AtomicUsize::new(0);
static ITERATIONS: AtomicUsize = AtomicUsize::new(0);
static EXPECTED_NUM_ENTRIES: AtomicUsize = AtomicUsize::new(0);

const TREE_COLUMN: u8 = 0;
const INFO_COLUMN: u8 = 1;

const KEY_LAST_COMMIT: Key = [1u8; 32];
const KEY_NUM_REMOVED: Key = [2u8; 32];

const THREAD_PRUNING: bool = true;
const FORCE_NO_MULTIPART_VALUES: bool = true;
const FIXED_TEXT_POSITION: bool = true;

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
			pruning: self.pruning.unwrap_or(8),
			compress: self.compress,
			commit_time: self.commit_time.unwrap_or(0),
		}
	}
}

struct OutputHelper {
	last_fixed: String,
	stdout: std::io::Stdout,
}

impl OutputHelper {
	fn new() -> OutputHelper {
		println!("");
		OutputHelper { last_fixed: "".to_string(), stdout: std::io::stdout() }
	}

	fn println(&mut self, text: String) {
		if FIXED_TEXT_POSITION {
			let overwrite = format!("{:<1$}", text, self.last_fixed.len());
			println!("\r{}", overwrite);
			print!("{}", self.last_fixed);
			self.stdout.flush().unwrap();
		} else {
			println!("{}", text);
		}
	}

	fn print_fixed(&mut self, text: String) {
		if FIXED_TEXT_POSITION {
			let overwrite = format!("{:<1$}", text, self.last_fixed.len());
			print!("\r{}", overwrite);
			self.last_fixed = text;
			self.stdout.flush().unwrap();
		} else {
			println!("							{}", text);
		}
	}

	fn println_final(&mut self, text: String) {
		if FIXED_TEXT_POSITION {
			println!("");
			println!("{}", text);
			self.stdout.flush().unwrap();
		} else {
			println!("{}", text);
		}
	}
}

struct Histogram {
	distribution: BTreeMap<u32, u32>,
	total: u32,
}

impl Histogram {
	fn new(histogram_data: &[(u32, u32)]) -> Histogram {
		let mut distribution = BTreeMap::default();
		let mut total = 0;
		for (size, count) in histogram_data {
			total += count;
			if *count > 0 {
				distribution.insert(total, *size);
			}
		}
		Histogram { distribution, total }
	}

	fn sample(&self, rnd: u64) -> u32 {
		let sr = (rnd % self.total as u64) as u32;
		let mut range = self
			.distribution
			.range((std::ops::Bound::Included(sr), std::ops::Bound::Unbounded));
		let size = *range.next().unwrap().1;
		size
	}
}

pub enum NodeSpec {
	/// Direct specification of a node. (Tree index, depth, seed).
	Direct(u64, u32, u64),
	/// Node will be a path node but haven't done the work to generate the path yet.
	UnresolvedPath(),
	/// Path to another node. (Tree index, Path) where Path is a sequence of child indices starting
	/// from the root of the tree.
	Path(u64, Vec<u32>),
}

struct ChainGenerator {
	depth_child_count_histograms: Vec<Histogram>,
	age_histogram: Histogram,
	value_length_histogram: Histogram,
	seed: u64,
	compressable: bool,
}

impl ChainGenerator {
	fn new(
		depth_child_count_histogram: &[(u32, [u32; 17])],
		age_histogram: &[(u32, u32)],
		value_length_histogram: &[(u32, u32)],
		seed: u64,
		compressable: bool,
	) -> ChainGenerator {
		let mut depth_child_count_histograms = Vec::default();
		for (depth, histogram_data) in depth_child_count_histogram {
			assert_eq!(*depth, depth_child_count_histograms.len() as u32);

			let data: Vec<(u32, u32)> =
				histogram_data.iter().enumerate().map(|(a, b)| (a as u32, *b)).collect();
			let histogram = Histogram::new(data.as_slice());

			depth_child_count_histograms.push(histogram);
		}

		let age_histogram = Histogram::new(age_histogram);

		let value_length_histogram = Histogram::new(value_length_histogram);

		ChainGenerator {
			depth_child_count_histograms,
			age_histogram,
			value_length_histogram,
			seed,
			compressable,
		}
	}

	fn root_seed(&self, tree_index: u64) -> u64 {
		use std::hash::Hasher;
		let mut hasher = siphasher::sip::SipHasher24::new();
		hasher.write_u64(self.seed);
		hasher.write_u64(tree_index);
		let seed = hasher.finish();
		seed
	}

	fn key(&self, seed: u64) -> Key {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut key = Key::default();
		rng.fill_bytes(&mut key);
		key
	}

	/// Returns tuple of node data and child specs. When only_direct_children is true node data will
	/// be empty and non direct children will be NodeSpec::UnresolvedPath.
	fn generate_node(
		&self,
		tree_index: u64,
		depth: u32,
		seed: u64,
		only_direct_children: bool,
	) -> (Vec<u8>, Vec<NodeSpec>) {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

		let num_children = if depth < self.depth_child_count_histograms.len() as u32 {
			self.depth_child_count_histograms[depth as usize].sample(rng.next_u64())
		} else {
			0
		};
		let mut children = Vec::default();

		for _i in 0..num_children {
			let age = self.age_histogram.sample(rng.next_u64()) as u64;

			let child_tree_index = if age > tree_index { tree_index } else { tree_index - age };

			if child_tree_index == tree_index {
				children.push(NodeSpec::Direct(tree_index, depth + 1, rng.next_u64()));
			} else {
				// Always generate path seed even if only_direct_children is true to ensure
				// determinism.
				let path_seed = rng.next_u64();
				if only_direct_children {
					children.push(NodeSpec::UnresolvedPath());
				} else {
					// Generate path to node in child_tree_index
					let mut path_rng = rand::rngs::SmallRng::seed_from_u64(path_seed);
					let mut path = Vec::default();
					let mut path_node: Option<NodeSpec> = None;

					let target_depth = depth + 1;

					let mut other_tree_index = child_tree_index;
					let mut other_depth = 0;
					let mut other_seed = self.root_seed(child_tree_index);
					while other_depth < target_depth {
						let (_other_node_data, other_children) =
							self.generate_node(other_tree_index, other_depth, other_seed, true);

						let mut direct_children =
							other_children.iter().enumerate().filter(|(_, x)| {
								if let NodeSpec::Direct(..) = x {
									true
								} else {
									false
								}
							});
						let num_direct_children = direct_children.clone().count();

						if num_direct_children == 0 {
							break
						}

						let child_index = path_rng.next_u64() % num_direct_children as u64;

						let child = direct_children.nth(child_index as usize).unwrap();
						if let NodeSpec::Direct(new_index, new_depth, new_seed) = child.1 {
							path.push(child.0 as u32);

							// Chose a direct node so should be same tree, one depth down.
							assert_eq!(*new_index, other_tree_index);
							assert_eq!(*new_depth, other_depth + 1);

							other_tree_index = *new_index;
							other_depth = *new_depth;
							other_seed = *new_seed;

							if other_depth == target_depth {
								path_node = Some(NodeSpec::Path(child_tree_index, path.clone()));
							}
						} else {
							break
						}
					}

					match path_node {
						Some(node) => {
							children.push(node);
						},
						None => {
							// Unable to generate a path so just create a direct node. Use path_rng
							// to ensure deterministic generation when using only_direct_children.
							// TODO: This is not returned when only_direct_children is true even
							// though it is a direct node. Is this ok? only_direct_children is only
							// used when generating a path so it is ok if it doesn't see these
							// nodes.
							children.push(NodeSpec::Direct(
								tree_index,
								depth + 1,
								path_rng.next_u64(),
							));
						},
					}
				}
			}
		}

		if only_direct_children {
			return (Vec::new(), children)
		}

		let mut size = 4;
		if num_children == 0 {
			size = self.value_length_histogram.sample(rng.next_u64()) as usize;
			if FORCE_NO_MULTIPART_VALUES {
				size = std::cmp::min(size, 32760 - 64);
			}
		}
		let mut v = Vec::new();

		v.resize(size, 0);
		let fill = if !self.compressable { size } else { size / 2 };
		rng.fill_bytes(&mut v[..fill]);

		(v, children)
	}

	fn execute_path(&self, tree_index: u64, path: Vec<u32>) -> Result<(u64, u32, u64), String> {
		let mut depth = 0;
		let mut seed = self.root_seed(tree_index);
		for child_index in path {
			let (_node_data, children) = self.generate_node(tree_index, depth, seed, true);
			let child = &children[child_index as usize];
			if let NodeSpec::Direct(child_tree_index, child_depth, child_seed) = child {
				assert_eq!(*child_tree_index, tree_index);
				assert_eq!(*child_depth, depth + 1);
				depth = *child_depth;
				seed = *child_seed;
			} else {
				return Err("Non-direct node in path".to_string())
			}
		}
		Ok((tree_index, depth, seed))
	}
}

fn informant(
	db: Arc<Db>,
	shutdown: Arc<AtomicBool>,
	shutdown_final: Arc<AtomicBool>,
	output_helper: Arc<RwLock<OutputHelper>>,
) -> Result<(), String> {
	let mut num_expected_entries = 0;
	let mut num_entries = 0;
	while !shutdown_final.load(Ordering::Relaxed) {
		if FIXED_TEXT_POSITION {
			thread::sleep(std::time::Duration::from_millis(100));
		} else {
			thread::sleep(std::time::Duration::from_secs(1));
		}

		let new_num_expected_entries = EXPECTED_NUM_ENTRIES.load(Ordering::Relaxed);
		let new_num_entries = db.get_num_column_value_entries(TREE_COLUMN).unwrap();

		if new_num_expected_entries != num_expected_entries || new_num_entries != num_entries {
			num_expected_entries = new_num_expected_entries;
			num_entries = new_num_entries;

			output_helper.write().print_fixed(format!(
				"Entries, Created: {}, ValueTables: {}",
				num_expected_entries, num_entries
			));

			if num_entries == 0 {
				if shutdown.load(Ordering::Relaxed) {
					shutdown_final.store(true, Ordering::SeqCst);
				}
			}
		}
	}
	Ok(())
}

fn find_dependent_trees(
	node_data: &(Vec<u8>, Vec<NodeSpec>),
	chain_generator: &ChainGenerator,
	trees: &mut HashSet<u64>,
) -> Result<(), String> {
	for spec in &node_data.1 {
		match spec {
			NodeSpec::Direct(child_tree_index, child_depth, child_seed) => {
				let child_data = chain_generator.generate_node(
					*child_tree_index,
					*child_depth,
					*child_seed,
					false,
				);
				find_dependent_trees(&child_data, chain_generator, trees)?;
			},
			NodeSpec::UnresolvedPath() => return Err("UnresolvedPath found".to_string()),
			NodeSpec::Path(tree_index, _path) => {
				trees.insert(*tree_index);
			},
		}
	}
	Ok(())
}

fn build_commit_tree<'s, 'd: 's>(
	node_data: (Vec<u8>, Vec<NodeSpec>),
	db: &Db,
	chain_generator: &ChainGenerator,
	tree_refs: &'s HashMap<Key, TreeReaderRef<'d>>,
	tree_guards: &mut HashMap<Key, TreeReaderGuard<'s, 'd>>,
) -> Result<NodeRef, String> {
	let mut children = Vec::default();
	for spec in node_data.1 {
		match spec {
			NodeSpec::Direct(child_tree_index, child_depth, child_seed) => {
				let child_data =
					chain_generator.generate_node(child_tree_index, child_depth, child_seed, false);
				let child_node =
					build_commit_tree(child_data, db, chain_generator, tree_refs, tree_guards)?;
				children.push(child_node);
			},
			NodeSpec::UnresolvedPath() => return Err("UnresolvedPath found".to_string()),
			NodeSpec::Path(tree_index, path) => {
				let root_seed = chain_generator.root_seed(tree_index);
				let key = chain_generator.key(root_seed);

				if let None = tree_guards.get(&key) {
					if let Some(tree_ref) = tree_refs.get(&key) {
						tree_guards.insert(key.clone(), tree_ref.read());
					}
				}

				let mut final_child_address: Option<u64> = None;
				if let Some(tree_guard) = tree_guards.get(&key) {
					if let Some((db_node_data, db_children)) = tree_guard.get_root().unwrap() {
						// Note: We don't actually have to generate any nodes here; we could just
						// traverse down the database nodes. Only generating them to verify data.
						let (gen_node_data, gen_children) =
							chain_generator.generate_node(tree_index, 0, root_seed, false);

						assert_eq!(gen_node_data, db_node_data);
						assert_eq!(gen_children.len(), db_children.len());

						let mut generated_children = gen_children;
						let mut database_children = db_children;

						for index in 0..path.len() {
							let child_index = path[index];

							let child_address = database_children[child_index as usize];

							if index == path.len() - 1 {
								final_child_address = Some(child_address);
								break
							}

							let (child_tree_index, child_depth, child_seed) =
								match &generated_children[child_index as usize] {
									NodeSpec::Direct(child_tree_index, child_depth, child_seed) =>
										(*child_tree_index, *child_depth, *child_seed),
									NodeSpec::UnresolvedPath() =>
										return Err("UnresolvedPath found".to_string()),
									NodeSpec::Path(_tree_index, _path) =>
										return Err("NodeSpec::Path found within path".to_string()),
								};

							let (gen_node_data, gen_children) = chain_generator.generate_node(
								child_tree_index,
								child_depth,
								child_seed,
								false,
							);

							match tree_guard.get_node(child_address).unwrap() {
								Some((db_node_data, db_children)) => {
									assert_eq!(gen_node_data, db_node_data);
									assert_eq!(gen_children.len(), db_children.len());

									generated_children = gen_children;
									database_children = db_children;
								},
								None => return Err("Child address not in database".to_string()),
							}
						}
					}
				}

				match final_child_address {
					Some(address) => {
						let child_node = NodeRef::Existing(address);
						children.push(child_node);
					},
					None => {
						// Not able to get the existing child address so duplicate sub-tree
						let (child_tree_index, child_depth, child_seed) =
							chain_generator.execute_path(tree_index, path)?;
						let child_data = chain_generator.generate_node(
							child_tree_index,
							child_depth,
							child_seed,
							false,
						);
						let child_node = build_commit_tree(
							child_data,
							db,
							chain_generator,
							tree_refs,
							tree_guards,
						)?;
						children.push(child_node);
					},
				}
			},
		}
	}
	let new_node = NewNode { data: node_data.0, children };
	Ok(NodeRef::New(new_node))
}

fn num_new_nodes(node: &NodeRef, num_existing: &mut u32) -> u32 {
	match node {
		NodeRef::New(node) => {
			let mut num = 1;
			for child in &node.children {
				num += num_new_nodes(child, num_existing);
			}
			num
		},
		NodeRef::Existing(_) => {
			*num_existing += 1;
			0
		},
	}
}

fn read_value(
	tree_index: u64,
	rng: &mut rand::rngs::SmallRng,
	db: &Db,
	chain_generator: &ChainGenerator,
) -> Result<(), String> {
	let mut depth = 0;
	let root_seed = chain_generator.root_seed(tree_index);

	let (gen_node_data, gen_children) =
		chain_generator.generate_node(tree_index, depth, root_seed, false);

	let key = chain_generator.key(root_seed);
	match db.get_tree(TREE_COLUMN, &key).unwrap() {
		Some(reader) => {
			let reader = reader.read();
			match reader.get_root().unwrap() {
				Some((db_node_data, db_children)) => {
					assert_eq!(gen_node_data, db_node_data);
					assert_eq!(gen_children.len(), db_children.len());

					let mut generated_children = gen_children;
					let mut database_children = db_children;

					while generated_children.len() > 0 {
						let child_index = rng.next_u64() % generated_children.len() as u64;

						let (child_tree_index, child_seed) =
							match &generated_children[child_index as usize] {
								NodeSpec::Direct(child_tree_index, _child_depth, child_seed) =>
									(*child_tree_index, *child_seed),
								NodeSpec::UnresolvedPath() =>
									return Err("UnresolvedPath found".to_string()),
								NodeSpec::Path(tree_index, path) => {
									let (child_tree_index, child_depth, child_seed) =
										chain_generator.execute_path(*tree_index, path.clone())?;
									assert_eq!(child_depth, depth + 1);
									(child_tree_index, child_seed)
								},
							};

						let child_address = database_children[child_index as usize];
						depth += 1;

						let (gen_node_data, gen_children) = chain_generator.generate_node(
							child_tree_index,
							depth,
							child_seed,
							false,
						);
						match reader.get_node(child_address).unwrap() {
							Some((db_node_data, db_children)) => {
								assert_eq!(gen_node_data, db_node_data);
								assert_eq!(gen_children.len(), db_children.len());

								generated_children = gen_children;
								database_children = db_children;
							},
							None => return Err("Child address not in database".to_string()),
						}
					}

					QUERIES.fetch_add(1, Ordering::SeqCst);
				},
				None => {
					// Is this expected?
					let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
					if tree_index >= num_removed as u64 {
						return Err("Tree root not in database".to_string())
					}
				},
			}
		},
		None => {
			// Is this expected?
			let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
			if tree_index >= num_removed as u64 {
				return Err("Tree not in database".to_string())
			}
		},
	}

	Ok(())
}

struct TreeReaderRef<'d> {
	reader_ref: Arc<RwLock<dyn TreeReader + 'd>>,
}

impl<'d> TreeReaderRef<'d> {
	pub fn read<'s>(&'s self) -> TreeReaderGuard<'s, 'd> {
		TreeReaderGuard { lock_guard: self.reader_ref.read() }
	}
}

struct TreeReaderGuard<'s, 'd: 's> {
	lock_guard: RwLockReadGuard<'s, dyn TreeReader + 'd>,
}

impl<'s, 'd: 's> Deref for TreeReaderGuard<'s, 'd> {
	type Target = dyn TreeReader + 'd;

	fn deref(&self) -> &Self::Target {
		self.lock_guard.deref()
	}
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
	output_helper: Arc<RwLock<OutputHelper>>,
) -> Result<(), String> {
	let seed = args.seed.unwrap_or(0);
	let mut commit = Vec::new();

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break
		}

		let tree_index = n as u64;
		let root_seed = chain_generator.root_seed(tree_index);

		let node_data = chain_generator.generate_node(tree_index, 0, root_seed, false);

		// First phase. Find all trees that this commit is dependent on.
		let mut trees: HashSet<u64> = Default::default();
		find_dependent_trees(&node_data, &chain_generator, &mut trees)?;

		let mut tree_refs: HashMap<Key, TreeReaderRef> = Default::default();
		let mut tree_guards: HashMap<Key, TreeReaderGuard> = Default::default();
		for index in trees {
			let seed = chain_generator.root_seed(index);
			let key = chain_generator.key(seed);
			match db.get_tree(TREE_COLUMN, &key).unwrap() {
				Some(reader) => {
					let reader_ref = TreeReaderRef { reader_ref: reader };
					tree_refs.insert(key, reader_ref);
				},
				None => {
					// It's fine for the tree to not be in the database. The commit will regenerate
					// the required nodes.
				},
			}
		}

		/* for (key, tree_ref) in tree_refs.iter() {
			tree_guards.insert(key.clone(), tree_ref.read());
		} */

		let root_node_ref =
			build_commit_tree(node_data, &db, &chain_generator, &tree_refs, &mut tree_guards)?;
		let mut num_existing_nodes = 0;
		let num_new_nodes = num_new_nodes(&root_node_ref, &mut num_existing_nodes);
		if let NodeRef::New(node) = root_node_ref {
			let key: [u8; 32] = chain_generator.key(root_seed);

			output_helper.write().println(format!(
				"Commit tree {}, new: {}, existing: {}",
				tree_index, num_new_nodes, num_existing_nodes
			));

			commit.push((TREE_COLUMN, Operation::InsertTree(key.to_vec(), node)));

			commit.push((
				INFO_COLUMN,
				Operation::Set(KEY_LAST_COMMIT.to_vec(), (n as u64).to_be_bytes().to_vec()),
			));

			db.commit_changes(commit.drain(..)).unwrap();
			COMMITS.fetch_add(1, Ordering::Relaxed);
			EXPECTED_NUM_ENTRIES.fetch_add(num_new_nodes as usize, Ordering::Relaxed);
			commit.clear();

			// Immediately read and check a random value from the tree
			let mut rng = rand::rngs::SmallRng::seed_from_u64(seed + n as u64);
			read_value(tree_index, &mut rng, &db, &chain_generator)?;

			if args.pruning > 0 && !THREAD_PRUNING {
				try_prune(&db, &args, &chain_generator, &mut commit, &output_helper)?;
			}

			if args.commit_time > 0 {
				thread::sleep(std::time::Duration::from_millis(args.commit_time));
			}
		}
	}

	Ok(())
}

fn try_prune(
	db: &Db,
	args: &Args,
	chain_generator: &ChainGenerator,
	commit: &mut Vec<(u8, Operation<Vec<u8>, Vec<u8>>)>,
	output_helper: &RwLock<OutputHelper>,
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
		let tree_index = num_removed as u64;
		let root_seed = chain_generator.root_seed(tree_index);
		let key = chain_generator.key(root_seed);

		output_helper.write().println(format!("Remove tree {}", tree_index));

		commit.push((TREE_COLUMN, Operation::RemoveTree(key.to_vec())));
		commit.push((
			INFO_COLUMN,
			Operation::Set(
				KEY_NUM_REMOVED.to_vec(),
				((num_removed + 1) as u64).to_be_bytes().to_vec(),
			),
		));

		NUM_REMOVED.fetch_add(1, Ordering::Relaxed);
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
	output_helper: Arc<RwLock<OutputHelper>>,
) -> Result<(), String> {
	let mut commit = Vec::new();

	while !shutdown.load(Ordering::Relaxed) {
		try_prune(&db, &args, &chain_generator, &mut commit, &output_helper)?;
	}

	Ok(())
}

fn reader(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Query random values from random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let num_removed = NUM_REMOVED.load(Ordering::Relaxed) as u64;
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue
		}

		//let tree_index = rng.next_u64() % commits;
		let tree_index = (rng.next_u64() % (commits - num_removed)) + num_removed;

		read_value(tree_index, &mut rng, &db, &chain_generator)?;
	}

	Ok(())
}

fn iter_children<'a>(
	depth: u32,
	generated_children: &mut Vec<NodeSpec>,
	database_children: &mut Vec<u64>,
	reader: &RwLockReadGuard<'a, dyn TreeReader>,
	chain_generator: &ChainGenerator,
) -> Result<(), String> {
	for i in 0..generated_children.len() {
		let child_index = i;

		let (child_tree_index, child_seed) = match &generated_children[child_index as usize] {
			NodeSpec::Direct(child_tree_index, _child_depth, child_seed) =>
				(*child_tree_index, *child_seed),
			NodeSpec::UnresolvedPath() => return Err("UnresolvedPath found".to_string()),
			NodeSpec::Path(tree_index, path) => {
				let (child_tree_index, child_depth, child_seed) =
					chain_generator.execute_path(*tree_index, path.clone())?;
				assert_eq!(child_depth, depth + 1);
				(child_tree_index, child_seed)
			},
		};

		let child_address = database_children[child_index as usize];

		let (gen_node_data, mut gen_children) =
			chain_generator.generate_node(child_tree_index, depth + 1, child_seed, false);
		match reader.get_node(child_address).unwrap() {
			Some((db_node_data, mut db_children)) => {
				assert_eq!(gen_node_data, db_node_data);
				assert_eq!(gen_children.len(), db_children.len());

				iter_children(
					depth + 1,
					&mut gen_children,
					&mut db_children,
					reader,
					chain_generator,
				)?;
			},
			None => return Err("Child address not in database".to_string()),
		}
	}

	Ok(())
}

fn iter(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Iterate over nodes in random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let num_removed = NUM_REMOVED.load(Ordering::Relaxed) as u64;
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue
		}

		//let tree_index = rng.next_u64() % commits;
		let tree_index = (rng.next_u64() % (commits - num_removed)) + num_removed;

		let root_seed = chain_generator.root_seed(tree_index);
		let depth = 0;

		let (gen_node_data, gen_children) =
			chain_generator.generate_node(tree_index, depth, root_seed, false);

		let key = chain_generator.key(root_seed);
		match db.get_tree(TREE_COLUMN, &key).unwrap() {
			Some(reader) => {
				let reader = reader.read();
				match reader.get_root().unwrap() {
					Some((db_node_data, db_children)) => {
						assert_eq!(gen_node_data, db_node_data);
						assert_eq!(gen_children.len(), db_children.len());

						// Iterate over all children recursively in depth first order.
						let mut generated_children = gen_children;
						let mut database_children = db_children;

						iter_children(
							depth,
							&mut generated_children,
							&mut database_children,
							&reader,
							&chain_generator,
						)?;

						ITERATIONS.fetch_add(1, Ordering::SeqCst);
					},
					None => {
						// Is this expected?
						let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
						if tree_index >= num_removed as u64 {
							return Err("Tree root not in database".to_string())
						}
					},
				}
			},
			None => {
				// Is this expected?
				let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
				if tree_index >= num_removed as u64 {
					return Err("Tree not in database".to_string())
				}
			},
		}
	}

	Ok(())
}

pub fn run_internal(args: Args, db: Db) -> Result<(), String> {
	let args = Arc::new(args);
	let shutdown = Arc::new(AtomicBool::new(false));
	let shutdown_final = Arc::new(AtomicBool::new(false));
	let db = Arc::new(db);
	let output_helper = Arc::new(RwLock::new(OutputHelper::new()));

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

	let total_num_expected_tree_nodes: u32 =
		data::DEPTH_CHILD_COUNT_HISTOGRAMS.iter().map(|x| x.1.iter().sum::<u32>()).sum();
	output_helper
		.write()
		.println(format!("Expected average num tree nodes: {}", total_num_expected_tree_nodes));

	let chain_generator = ChainGenerator::new(
		data::DEPTH_CHILD_COUNT_HISTOGRAMS,
		data::AGE_HISTOGRAM,
		data::VALUE_LENGTH_HISTOGRAM,
		args.seed.unwrap_or(0),
		args.compress,
	);
	let chain_generator = Arc::new(chain_generator);

	let start_time = std::time::Instant::now();

	COMMITS.store(start_commit, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit, Ordering::SeqCst);
	NUM_REMOVED.store(num_removed, Ordering::SeqCst);

	{
		let db = db.clone();
		let shutdown = shutdown.clone();
		let shutdown_final = shutdown_final.clone();
		let output_helper = output_helper.clone();

		threads.push(thread::spawn(move || informant(db, shutdown, shutdown_final, output_helper)));
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
		let output_helper = output_helper.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("writer {i}"))
				.spawn(move || {
					writer(db, args, chain_generator, shutdown, start_commit, output_helper)
				})
				.unwrap(),
		);
	}

	if args.pruning > 0 && THREAD_PRUNING {
		let db = db.clone();
		let shutdown = shutdown_final.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();
		let output_helper = output_helper.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("pruner"))
				.spawn(move || pruner(db, args, chain_generator, shutdown, output_helper))
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

	output_helper.write().println(format!(
		"Completed {} commits in {} seconds. {} cps. {} queries, {} iterations",
		commits,
		elapsed_time,
		commits as f64 / elapsed_time,
		queries,
		iterations
	));

	// Continue removing trees until they are all gone.
	TARGET_NUM_REMOVED.store(args.commits, Ordering::SeqCst);
	while NUM_REMOVED.load(Ordering::Relaxed) < args.commits {
		thread::sleep(std::time::Duration::from_millis(50));
	}

	// Wait for all entries to actually be removed from Db.
	while !shutdown_final.load(Ordering::Relaxed) {
		thread::sleep(std::time::Duration::from_millis(50));
	}

	for t in threads.into_iter() {
		t.join().unwrap()?;
	}

	output_helper.write().println_final(format!("Removed all entries"));

	Ok(())
}
