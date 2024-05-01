// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use super::*;

mod data;

pub use parity_db::{Db, Key, TreeReader};
use parity_db::{NewNode, NodeRef, Operation};

use parking_lot::{RwLock, RwLockReadGuard};

use rand::{RngCore, SeedableRng};
use std::{
	collections::{BTreeMap, VecDeque},
	io::Write,
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

pub const TREE_COLUMN: u8 = 0;
pub const INFO_COLUMN: u8 = 1;

const KEY_LAST_COMMIT: Key = [1u8; 32];
const KEY_NUM_REMOVED: Key = [2u8; 32];

const MAX_NUM_CHILDREN: usize = 16;
const NUM_TREES_TO_CACHE: usize = 3;

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
			readers: self.readers.unwrap_or(1),
			iter: self.iter.unwrap_or(0),
			writers: self.writers.unwrap_or(1),
			commits: self.commits.unwrap_or(10_000),
			seed: self.seed,
			append: self.append,
			empty_on_shutdown: self.empty_on_shutdown,
			pruning: self.pruning.unwrap_or(256),
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

#[derive(Clone)]
enum ChildSpec {
	/// New node.
	New(Arc<NodeData>),
	/// Node that existed in the previous tree at the specified previous index.
	Existing(u8, Arc<NodeData>),
	/// Replacement for a node that existed in the previous tree at the specified previous index.
	Replacement(u8, Arc<NodeData>),
}

impl ChildSpec {
	fn get_child_as_ref(&self) -> &Arc<NodeData> {
		match self {
			ChildSpec::New(child) => child,
			ChildSpec::Existing(_, child) => child,
			ChildSpec::Replacement(_, child) => child,
		}
	}
}

#[derive(Clone)]
struct NodeData {
	data: Vec<u8>,
	children: Vec<ChildSpec>,
	leaf: bool,
}

struct ChainGenerator {
	depth_child_count_histograms: Vec<Histogram>,
	depth_leaf_count_cumulative: Vec<u32>,
	value_length_histogram: Histogram,
	seed: u64,
	compressable: bool,
}

impl ChainGenerator {
	fn new(
		depth_child_count_histogram: &[(u32, [u32; 17])],
		depth_leaf_count_histogram: &[(u32, u32)],
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

		let mut depth_leaf_count_cumulative = Vec::new();
		let mut total = 0;
		for entry in depth_leaf_count_histogram {
			assert_eq!(entry.0, depth_leaf_count_cumulative.len() as u32);
			total += entry.1;
			depth_leaf_count_cumulative.push(total);
		}

		let value_length_histogram = Histogram::new(value_length_histogram);

		ChainGenerator {
			depth_child_count_histograms,
			depth_leaf_count_cumulative,
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

	/// Returns tuple of node data and child specs. Child specs are depth and seed.
	fn generate_node(
		&self,
		depth: u32,
		seed: u64,
		enumerated_depth_child_counts: &mut Vec<Vec<usize>>,
	) -> (Vec<u8>, Vec<(u32, u64)>) {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);

		let num_children =
			enumerated_depth_child_counts.get_mut(depth as usize).map_or(None, |x| x.pop());

		let num_children = num_children.unwrap_or_else(|| {
			self.depth_child_count_histograms
				.get(depth as usize)
				.map_or(0, |h| h.sample(rng.next_u64()) as usize)
		});

		let mut children = Vec::with_capacity(num_children as usize);

		for _ in 0..num_children {
			children.push((depth + 1, rng.next_u64()));
		}

		let v = self.generate_node_data(&mut rng, num_children == 0);

		(v, children)
	}

	fn generate_node_data(&self, rng: &mut rand::rngs::SmallRng, leaf: bool) -> Vec<u8> {
		// Polkadot avoids storing actual values in branch nodes, they mainly go in leaf nodes.
		// Hence the value stored in the db will only be for the nibble path.
		let mut size = 4;
		if leaf {
			// Leaf node, so simulate an actual value using the size histogram.
			size = self.value_length_histogram.sample(rng.next_u64()) as usize;
			if FORCE_NO_MULTIPART_VALUES {
				size = std::cmp::min(size, 32760 - 64);
			}
		}
		let mut v = Vec::new();

		v.resize(size, 0);
		let fill = if !self.compressable { size } else { size / 2 };
		rng.fill_bytes(&mut v[..fill]);

		v
	}

	fn change_seed(&self, tree_index: u64) -> u64 {
		use std::hash::Hasher;
		let mut hasher = siphasher::sip::SipHasher24::new();
		hasher.write_u64(self.seed);
		hasher.write_u64(tree_index);
		hasher.write_u64(3);
		let seed = hasher.finish();
		seed
	}

	fn build_tree_with_changes(
		&self,
		tree_index: u64,
		previous_root_node: &NodeData,
	) -> Result<NodeData, String> {
		let mut rng = rand::rngs::SmallRng::seed_from_u64(self.change_seed(tree_index));

		// This results in about the right number of new nodes for each tree (AVERAGE_NUM_NEW_NODES)
		let num_additions = 47;
		let num_removals = (num_additions * 90) / 100;

		self.build_node_with_changes(
			&mut rng,
			Some(previous_root_node),
			0,
			num_removals,
			num_additions,
		)
	}

	fn build_node_with_changes(
		&self,
		rng: &mut rand::rngs::SmallRng,
		previous_node: Option<&NodeData>,
		depth: u32,
		num_removals: u32,
		num_additions: u32,
	) -> Result<NodeData, String> {
		let mut num_children = 0;
		let mut num_leaf_children: u32 = 0;
		let mut num_branch_children: u32 = 0;
		if let Some(previous_node) = previous_node {
			num_children = previous_node.children.len();
			for child_node in previous_node.children.iter() {
				if child_node.get_child_as_ref().leaf {
					num_leaf_children += 1;
				} else {
					num_branch_children += 1;
				}
			}
		}
		assert!(num_children <= MAX_NUM_CHILDREN);

		// Tuple of (Existing index, Leaf, Num removals, Num additions) for each child
		let mut child_changes = if let Some(previous_node) = previous_node {
			let mut child_changes = Vec::with_capacity(previous_node.children.len());
			for (i, child_node) in previous_node.children.iter().enumerate() {
				let leaf = child_node.get_child_as_ref().leaf;
				child_changes.push((Some(i), leaf, 0, 0));
			}
			child_changes
		} else {
			Vec::new()
		};

		let mut num_removals = num_removals;

		// Distribute removals and additions among children

		// Removals can be applied to any child, either passed down to branch children or leaf
		// children can themselves be removed.

		// Check if we should remove any leaf children
		child_changes.retain(|x| {
			if x.1 && num_removals > 0 {
				let leaf_probability = 1.0 / (MAX_NUM_CHILDREN as f64);
				let rand_float = (rng.next_u32() as f64) / (u32::MAX as f64);
				if rand_float <= leaf_probability {
					num_leaf_children -= 1;
					num_children -= 1;
					num_removals -= 1;
					false
				} else {
					true
				}
			} else {
				true
			}
		});

		let num_removal_slots = num_branch_children;
		if num_removal_slots > 0 {
			let mut removal_slots = Vec::with_capacity(num_removal_slots as usize);
			for _ in 0..num_removal_slots {
				removal_slots.push(0);
			}

			// Allocate removals to removal_slots
			let num_removals = if num_removals >= num_removal_slots {
				let num_per_slot = num_removals / num_removal_slots;
				for i in 0..num_removal_slots {
					removal_slots[i as usize] += num_per_slot;
				}
				num_removals - (num_removal_slots * num_per_slot)
			} else {
				num_removals
			};
			if num_removals > 0 {
				assert!(num_removals < num_removal_slots);
				let mut index = rng.next_u32() % num_removal_slots;
				for _ in 0..num_removals {
					removal_slots[index as usize] += 1;
					index = (index + 1) % num_removal_slots;
				}
			}

			let mut slot_index = 0;
			for child in child_changes.iter_mut() {
				if !child.1 {
					child.2 += removal_slots[slot_index];
					slot_index += 1;
				}
			}
		}

		// Based on depth leaf count histogram a certain number of additions should happen now as
		// new leaf child nodes
		let num_immediate_additions =
			if depth as usize >= self.depth_leaf_count_cumulative.len() - 2 {
				num_additions
			} else {
				let num = self.depth_leaf_count_cumulative[(depth + 1) as usize] -
					self.depth_leaf_count_cumulative[depth as usize];
				let total = self.depth_leaf_count_cumulative
					[self.depth_leaf_count_cumulative.len() - 1] -
					self.depth_leaf_count_cumulative[depth as usize];
				if num_additions == 1 {
					let probability = num as f64 / total as f64;
					let rand_float = (rng.next_u32() as f64) / (u32::MAX as f64);
					if rand_float <= probability {
						1
					} else {
						0
					}
				} else {
					(num * num_additions) / total
				}
			};
		// Each immediate addition needs a new leaf node so make sure we don't end up with more than
		// 16 children. TODO: Would be better to split this node and allow all immediate additions.
		let num_immediate_additions =
			std::cmp::min(num_immediate_additions, (MAX_NUM_CHILDREN - num_children) as u32);
		let num_additions = if depth as usize >= self.depth_leaf_count_cumulative.len() - 2 {
			0
		} else {
			num_additions - num_immediate_additions
		};

		// Additions should not necessarilly go to an existing child. It goes to 1 of 16 slots.
		// The children each take up a slot, so it might go to them. If not we create a new child
		// node and give the addition to that. TODO: Would be nice to allow all 16 slots and split
		// this node if num children is > 16.
		let num_addition_slots =
			MAX_NUM_CHILDREN as u32 - (num_leaf_children + num_immediate_additions);
		if num_addition_slots > 0 {
			let mut addition_slots = Vec::with_capacity(num_addition_slots as usize);
			for _ in 0..num_addition_slots {
				addition_slots.push(0);
			}

			// Allocate additions to addition_slots
			let num_additions = if num_additions >= num_addition_slots {
				let num_per_slot = num_additions / num_addition_slots;
				for i in 0..num_addition_slots {
					addition_slots[i as usize] += num_per_slot;
				}
				num_additions - (num_addition_slots * num_per_slot)
			} else {
				num_additions
			};
			if num_additions > 0 {
				assert!(num_additions < num_addition_slots);
				let mut index = rng.next_u32() % num_addition_slots;
				for _ in 0..num_additions {
					addition_slots[index as usize] += 1;
					index = (index + 1) % num_addition_slots;
				}
			}

			let mut slot_index = 0;
			for child in child_changes.iter_mut() {
				if !child.1 {
					child.3 += addition_slots[slot_index];
					slot_index += 1;
				}
			}

			for _ in slot_index..num_addition_slots as usize {
				if addition_slots[slot_index] > 0 {
					// Insert at random index in child_changes
					let index = rng.next_u32() as usize % (child_changes.len() + 1);
					child_changes.insert(index, (None, false, 0, addition_slots[slot_index]));
				}
				slot_index += 1;
			}
		}

		// Immediate additions
		for _ in 0..num_immediate_additions {
			child_changes.push((None, true, 0, 0));
		}

		// Apply child_changes
		let mut children = Vec::with_capacity(child_changes.len());
		for (existing_index, leaf, removals, additions) in child_changes.iter() {
			if let Some(existing_index) = existing_index {
				let previous_child_node = &previous_node.unwrap().children[*existing_index];
				let previous_child = previous_child_node.get_child_as_ref();
				if *removals > 0 || *additions > 0 {
					assert!(!*leaf);
					let child = self.build_node_with_changes(
						rng,
						Some(previous_child),
						depth + 1,
						*removals,
						*additions,
					)?;
					children.push(ChildSpec::Replacement(*existing_index as u8, Arc::new(child)));
				} else {
					children
						.push(ChildSpec::Existing(*existing_index as u8, previous_child.clone()));
				}
			} else {
				assert!(*leaf || *removals > 0 || *additions > 0);
				let child =
					self.build_node_with_changes(rng, None, depth + 1, *removals, *additions)?;
				children.push(ChildSpec::New(Arc::new(child)));
			}
		}

		// Remove non-leaf nodes that have no children
		children.retain(|x| match x {
			ChildSpec::Replacement(_, child) => child.leaf || child.children.len() > 0,
			_ => true,
		});

		let (data, leaf) = if let Some(previous_node) = previous_node {
			(previous_node.data.clone(), previous_node.leaf)
		} else {
			let leaf = (num_removals == 0) && (num_additions == 0);
			let data = self.generate_node_data(rng, leaf);
			(data, leaf)
		};

		Ok(NodeData { data, children, leaf })
	}
}

fn build_full_tree(
	node_spec: (u32, u64),
	chain_generator: &ChainGenerator,
	enumerated_depth_child_counts: &mut Vec<Vec<usize>>,
) -> (NodeData, u32) {
	let node_data =
		chain_generator.generate_node(node_spec.0, node_spec.1, enumerated_depth_child_counts);
	let mut total_num_nodes = 1;
	let mut children = Vec::with_capacity(node_data.1.len());
	for child_spec in node_data.1 {
		let (child_node, num_child_nodes) =
			build_full_tree(child_spec, chain_generator, enumerated_depth_child_counts);
		total_num_nodes += num_child_nodes;
		children.push(ChildSpec::New(Arc::new(child_node)));
	}
	let num_children = children.len();
	(NodeData { data: node_data.0, children, leaf: num_children == 0 }, total_num_nodes)
}

fn build_node_from_db<'a>(
	database_node_data: Vec<u8>,
	database_children: &Vec<u64>,
	reader: &RwLockReadGuard<'a, Box<dyn TreeReader + Send + Sync>>,
	chain_generator: &ChainGenerator,
) -> NodeData {
	let mut children = Vec::with_capacity(database_children.len());
	for i in 0..database_children.len() {
		let child_address = database_children[i];
		match reader.get_node(child_address).unwrap() {
			Some((db_node_data, db_children)) => {
				children.push(ChildSpec::New(Arc::new(build_node_from_db(
					db_node_data,
					&db_children,
					reader,
					chain_generator,
				))));
			},
			None => panic!("Child address not in database"),
		}
	}
	let num_children = children.len();
	NodeData { data: database_node_data, children, leaf: num_children == 0 }
}

fn build_tree_from_db(tree_index: u64, db: &Db, chain_generator: &ChainGenerator) -> NodeData {
	let root_seed = chain_generator.root_seed(tree_index);
	let key = chain_generator.key(root_seed);
	match db.get_tree(TREE_COLUMN, &key).unwrap() {
		Some(reader) => {
			let reader = reader.read();
			match reader.get_root().unwrap() {
				Some((db_node_data, db_children)) =>
					build_node_from_db(db_node_data, &db_children, &reader, &chain_generator),
				None => {
					panic!("Latest tree root not in database at restart");
				},
			}
		},
		None => {
			panic!("Latest tree not in database at restart");
		},
	}
}

fn informant(
	db: Arc<Db>,
	shutdown: Arc<AtomicBool>,
	shutdown_final: Arc<AtomicBool>,
	total_commits: usize,
	start_commit: usize,
	start_num_nodes: usize,
	output_helper: Arc<RwLock<OutputHelper>>,
) -> Result<(), String> {
	let mut last_commits = start_commit;
	let mut last_time = std::time::Instant::now();
	let mut last_num_expected_entries = start_num_nodes;
	let mut num_expected_entries = 0;
	let mut num_entries = 0;
	let mut finished = false;
	let mut iteration_count = 0;
	while !shutdown_final.load(Ordering::Relaxed) {
		let write_info = if FIXED_TEXT_POSITION {
			thread::sleep(std::time::Duration::from_millis(100));
			iteration_count = (iteration_count + 1) % 10;
			iteration_count == 0
		} else {
			thread::sleep(std::time::Duration::from_secs(1));
			true
		};

		let new_num_expected_entries = EXPECTED_NUM_ENTRIES.load(Ordering::Relaxed);
		let new_num_entries = db.get_num_column_value_entries(TREE_COLUMN).unwrap();

		if write_info {
			let commits = COMMITS.load(Ordering::Acquire);
			let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
			let now = std::time::Instant::now();

			if !finished {
				if commits > 0 && commits > last_commits {
					output_helper.write().println(format!(
						"{}/{} tree commits, {:.2} cps, {} removed, {:.2} average new nodes",
						commits - start_commit,
						total_commits,
						((commits - last_commits) as f64) / (now - last_time).as_secs_f64(),
						num_removed,
						(new_num_expected_entries - last_num_expected_entries) as f64 /
							(commits - last_commits) as f64
					));
				}

				if (commits - start_commit) >= total_commits &&
					num_removed >= (start_commit + total_commits)
				{
					finished = true;
				}
			}

			last_commits = commits;
			last_time = now;
			last_num_expected_entries = new_num_expected_entries;
		}

		if new_num_expected_entries != num_expected_entries || new_num_entries != num_entries {
			num_expected_entries = new_num_expected_entries;
			num_entries = new_num_entries;

			output_helper.write().print_fixed(format!(
				"Entries, Created: {}, in ValueTables: {}",
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

fn build_initial_commit_tree(
	node: &NodeData,
	db: &Db,
	chain_generator: &ChainGenerator,
) -> Result<NodeRef, String> {
	let mut children = Vec::with_capacity(node.children.len());
	for child in node.children.iter() {
		match child {
			ChildSpec::New(child) => {
				children.push(build_initial_commit_tree(child, db, chain_generator)?);
			},
			ChildSpec::Existing(..) =>
				return Err("Existing node found in initial commit".to_string()),
			ChildSpec::Replacement(..) =>
				return Err("Replacement node found in initial commit".to_string()),
		}
	}
	let new_node = NewNode { data: node.data.clone(), children };
	Ok(NodeRef::New(new_node))
}

fn build_commit_tree(
	node: &NodeData,
	db: &Db,
	chain_generator: &ChainGenerator,
	prev_child_addresses: Option<Vec<u64>>,
	prev_reader: &RwLockReadGuard<Box<dyn TreeReader + Send + Sync>>,
) -> Result<NodeRef, String> {
	let mut children = Vec::with_capacity(node.children.len());
	for child in node.children.iter() {
		match child {
			ChildSpec::New(child) => {
				children.push(build_commit_tree(child, db, chain_generator, None, prev_reader)?);
			},
			ChildSpec::Existing(prev_child_index, _child) => match &prev_child_addresses {
				Some(addresses) => {
					children.push(NodeRef::Existing(addresses[*prev_child_index as usize]));
				},
				None => return Err("Existing node without child addresses".to_string()),
			},
			ChildSpec::Replacement(prev_child_index, child) => match &prev_child_addresses {
				Some(addresses) => {
					let address = addresses[*prev_child_index as usize];
					match prev_reader.get_node_children(address).unwrap() {
						Some(db_children) => {
							children.push(build_commit_tree(
								child,
								db,
								chain_generator,
								Some(db_children),
								prev_reader,
							)?);
						},
						None => return Err("Child address not in database".to_string()),
					}
				},
				None => return Err("Replacement node without child addresses".to_string()),
			},
		}
	}
	let new_node = NewNode { data: node.data.clone(), children };
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
	gen_root_node: &NodeData,
	rng: &mut rand::rngs::SmallRng,
	db: &Db,
	chain_generator: &ChainGenerator,
) -> Result<(), String> {
	let root_seed = chain_generator.root_seed(tree_index);
	let key = chain_generator.key(root_seed);
	match db.get_tree(TREE_COLUMN, &key).unwrap() {
		Some(reader) => {
			let reader = reader.read();
			match reader.get_root().unwrap() {
				Some((db_node_data, db_children)) => {
					assert_eq!(gen_root_node.data, db_node_data);
					assert_eq!(gen_root_node.children.len(), db_children.len());

					let mut generated_children = &gen_root_node.children;
					let mut database_children = db_children;

					while generated_children.len() > 0 {
						let child_index = rng.next_u64() % generated_children.len() as u64;

						let child_address = database_children[child_index as usize];

						let gen_child = generated_children[child_index as usize].get_child_as_ref();
						let gen_node_data = &gen_child.data;
						let gen_children = &gen_child.children;

						match reader.get_node(child_address).unwrap() {
							Some((db_node_data, db_children)) => {
								assert_eq!(*gen_node_data, db_node_data);
								assert_eq!(gen_children.len(), db_children.len());

								generated_children = gen_children;
								database_children = db_children;
							},
							None => panic!("Child address not in database"),
						}
					}

					QUERIES.fetch_add(1, Ordering::SeqCst);
				},
				None => {
					// Is this expected?
					let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
					if tree_index >= num_removed as u64 {
						panic!("Tree root not in database during read");
					}
				},
			}
		},
		None => {
			// Is this expected?
			let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
			if tree_index >= num_removed as u64 {
				panic!(
					"Tree not in database during read (index: {}, removed: {})",
					tree_index, num_removed
				);
			}
		},
	}

	Ok(())
}

fn writer(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	trees: Arc<RwLock<VecDeque<(u64, Arc<NodeData>)>>>,
	shutdown: Arc<AtomicBool>,
	start_commit: usize,
) -> Result<(), String> {
	let mut commit = Vec::new();

	loop {
		let n = NEXT_COMMIT.fetch_add(1, Ordering::SeqCst);
		if n >= start_commit + args.commits || shutdown.load(Ordering::Relaxed) {
			break
		}

		let tree_index = n as u64;
		let root_seed = chain_generator.root_seed(tree_index);

		let (previous_tree_index, previous_tree) = if args.writers > 1 {
			// Not much advantage to multiple writers. They have to work from the previous commit so
			// just wait their turn.
			let required_tree_index = if n == 0 { 0 } else { tree_index - 1 };
			let mut previous_tree_index = 0;
			let mut previous_tree = None;
			while previous_tree.is_none() {
				let check = if n == 1 {
					let commits = COMMITS.load(Ordering::Relaxed);
					commits > start_commit
				} else {
					true
				};
				if check {
					let tree_lock = trees.read();
					if let Some(back_tree) = tree_lock.back() {
						if back_tree.0 == required_tree_index {
							previous_tree_index = back_tree.0;
							previous_tree = Some(back_tree.1.clone());
						}
					}
				}
			}
			(previous_tree_index, previous_tree)
		} else {
			let tree_lock = trees.read();
			let (previous_tree_index, previous_tree) = if let Some(previous_tree) = tree_lock.back()
			{
				let previous_root_node = previous_tree.1.clone();
				(previous_tree.0, Some(previous_root_node))
			} else {
				// There should always be a cached tree
				assert!(false);
				(0, None)
			};
			(previous_tree_index, previous_tree)
		};

		if let Some(previous_tree) = previous_tree {
			let prev_root_seed = chain_generator.root_seed(previous_tree_index);
			let prev_key = chain_generator.key(prev_root_seed);

			let reader = if n > 0 {
				Some(match db.get_tree(TREE_COLUMN, &prev_key).unwrap() {
					Some(r) => r,
					None => return Err("Previous tree not found in database".to_string()),
				})
			} else {
				None
			};

			let reader_lock = reader.as_ref().map(|f| f.read());

			let (root_node_ref, tree) = if n == 0 {
				assert_eq!(previous_tree_index, 0);
				let root = build_initial_commit_tree(&previous_tree, &db, &chain_generator)?;
				(root, None)
			} else {
				assert_eq!(previous_tree_index, tree_index - 1);

				// Create new tree with removals and additions
				let new_tree =
					chain_generator.build_tree_with_changes(tree_index, &previous_tree)?;

				let reader = reader_lock.as_ref().unwrap();

				let prev_child_address = match reader.get_root().unwrap() {
					Some(prev_root) => prev_root.1,
					None => return Err("Previous root not found in database".to_string()),
				};

				let root = build_commit_tree(
					&new_tree,
					&db,
					&chain_generator,
					Some(prev_child_address),
					&reader,
				)?;
				(root, Some(new_tree))
			};

			let mut num_existing_nodes = 0;
			let num_new_nodes = num_new_nodes(&root_node_ref, &mut num_existing_nodes);
			if let NodeRef::New(node) = root_node_ref {
				let key: [u8; 32] = chain_generator.key(root_seed);

				commit.push((TREE_COLUMN, Operation::InsertTree(key.to_vec(), node)));

				commit.push((
					INFO_COLUMN,
					Operation::Set(KEY_LAST_COMMIT.to_vec(), (n as u64).to_be_bytes().to_vec()),
				));

				db.commit_changes(commit.drain(..)).unwrap();

				drop(reader_lock);

				if let Some(tree) = tree {
					let mut tree_lock = trees.write();
					tree_lock.push_back((tree_index, Arc::new(tree)));
					while tree_lock.len() > NUM_TREES_TO_CACHE {
						tree_lock.pop_front();
					}
				}

				COMMITS.fetch_add(1, Ordering::SeqCst);
				EXPECTED_NUM_ENTRIES.fetch_add(num_new_nodes as usize, Ordering::SeqCst);
				commit.clear();

				// Immediately read and check a random value from the tree
				/* let tree_lock = trees.read();
				if let Some(back_tree) = tree_lock.back() {
					if back_tree.0 == tree_index {
						let root_node = back_tree.1.clone();
						let mut rng =
							rand::rngs::SmallRng::seed_from_u64(args.seed.unwrap_or(0) + n as u64);
						read_value(tree_index, &root_node, &mut rng, &db, &chain_generator)?;
					}
				}
				drop(tree_lock); */

				if args.pruning > 0 && !THREAD_PRUNING {
					try_prune(&db, &args, &chain_generator, &mut commit)?;
				}

				if args.commit_time > 0 {
					thread::sleep(std::time::Duration::from_millis(args.commit_time));
				}
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
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	trees: Arc<RwLock<VecDeque<(u64, Arc<NodeData>)>>>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Query random values from random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue
		}

		let trees = trees.read();
		let generated_tree = if trees.len() > 0 {
			let cached_index = rng.next_u32() as usize % trees.len();
			let gen_tree = &trees[cached_index];
			let gen_tree_root = gen_tree.1.clone();
			Some((gen_tree.0, gen_tree_root))
		} else {
			None
		};
		drop(trees);
		if let Some((tree_index, gen_root_node)) = generated_tree {
			read_value(tree_index, &gen_root_node, &mut rng, &db, &chain_generator)?;
		}
	}

	Ok(())
}

fn iter_children<'a>(
	generated_children: &Vec<ChildSpec>,
	database_children: &Vec<u64>,
	reader: &RwLockReadGuard<'a, Box<dyn TreeReader + Send + Sync>>,
	chain_generator: &ChainGenerator,
) -> Result<(), String> {
	for i in 0..database_children.len() {
		let child_address = database_children[i];

		let gen_child = generated_children[i].get_child_as_ref();
		let gen_node_data = &gen_child.data;
		let gen_children = &gen_child.children;

		match reader.get_node(child_address).unwrap() {
			Some((db_node_data, db_children)) => {
				assert_eq!(*gen_node_data, db_node_data);
				assert_eq!(gen_children.len(), db_children.len());

				iter_children(gen_children, &db_children, reader, chain_generator)?;
			},
			None => panic!("Child address not in database"),
		}
	}

	Ok(())
}

fn iter(
	db: Arc<Db>,
	args: Arc<Args>,
	chain_generator: Arc<ChainGenerator>,
	trees: Arc<RwLock<VecDeque<(u64, Arc<NodeData>)>>>,
	index: u64,
	shutdown: Arc<AtomicBool>,
) -> Result<(), String> {
	// Iterate over nodes in random trees while writing
	let offset = args.seed.unwrap_or(0);
	let mut rng = rand::rngs::SmallRng::seed_from_u64(offset + index);

	while !shutdown.load(Ordering::Relaxed) {
		let commits = COMMITS.load(Ordering::Relaxed) as u64;
		if commits == 0 {
			continue
		}

		let trees = trees.read();
		let generated_tree = if trees.len() > 0 {
			let cached_index = rng.next_u32() as usize % trees.len();
			let gen_tree = &trees[cached_index];
			let gen_tree_root = gen_tree.1.clone();
			Some((gen_tree.0, gen_tree_root))
		} else {
			None
		};
		drop(trees);
		if let Some((tree_index, gen_root_node)) = generated_tree {
			let root_seed = chain_generator.root_seed(tree_index);
			let key = chain_generator.key(root_seed);
			match db.get_tree(TREE_COLUMN, &key).unwrap() {
				Some(reader) => {
					let reader = reader.read();
					match reader.get_root().unwrap() {
						Some((db_node_data, db_children)) => {
							assert_eq!(gen_root_node.data, db_node_data);
							assert_eq!(gen_root_node.children.len(), db_children.len());

							// Iterate over all children recursively in depth first order.
							let generated_children = &gen_root_node.children;
							let database_children = db_children;

							iter_children(
								generated_children,
								&database_children,
								&reader,
								&chain_generator,
							)?;

							ITERATIONS.fetch_add(1, Ordering::SeqCst);
						},
						None => {
							// Is this expected?
							let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
							if tree_index >= num_removed as u64 {
								panic!("Tree root not in database");
							}
						},
					}
				},
				None => {
					let num_removed = NUM_REMOVED.load(Ordering::Relaxed);
					if tree_index >= num_removed as u64 {
						panic!(
							"Tree not in database during iterator (index: {}, removed: {})",
							tree_index, num_removed
						);
					}
				},
			}
		} else {
			panic!("Trying to iterate over tree that isn't cached");
		}
	}

	Ok(())
}

fn enumerate_depth_child_counts(num_depths: u32) -> Vec<Vec<usize>> {
	let mut depth_child_counts = Vec::new();
	for i in 0..num_depths {
		let mut child_counts = Vec::new();
		let counts = data::DEPTH_CHILD_COUNT_HISTOGRAMS[i as usize];
		for c in 0..counts.1.len() {
			for _ in 0..counts.1[c] {
				child_counts.push(c);
			}
		}
		depth_child_counts.push(child_counts);
	}
	depth_child_counts
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

	output_helper
		.write()
		.println(format!("Expected average num tree nodes: {}", data::NUM_NODES));
	output_helper
		.write()
		.println(format!("Expected average num new nodes: {}", data::AVERAGE_NUM_NEW_NODES));

	let chain_generator = ChainGenerator::new(
		data::DEPTH_CHILD_COUNT_HISTOGRAMS,
		data::DEPTH_LEAF_COUNT_HISTOGRAM,
		data::VALUE_LENGTH_HISTOGRAM,
		args.seed.unwrap_or(0),
		args.compress,
	);
	let chain_generator = Arc::new(chain_generator);

	if start_commit != 0 {
		output_helper.write().println(format!("Start commit: {}", start_commit));
	}
	if num_removed != 0 {
		output_helper.write().println(format!("Num removed: {}", num_removed));
	}

	let (tree_index, node, start_num_nodes) = if start_commit == 0 {
		let tree_index = start_commit as u64;
		let root_seed = chain_generator.root_seed(tree_index);
		let node_spec = (0, root_seed);
		let mut enumerated_depth_child_counts = enumerate_depth_child_counts(2);
		let (node, start_num_nodes) =
			build_full_tree(node_spec, &chain_generator, &mut enumerated_depth_child_counts);
		output_helper.write().println(format!("Initial num nodes: {}", start_num_nodes));
		(tree_index, node, start_num_nodes)
	} else {
		let tree_index = start_commit as u64 - 1;
		(tree_index, build_tree_from_db(tree_index, &db, &chain_generator), 0)
	};

	let mut trees: VecDeque<_> = VecDeque::new();
	trees.push_back((tree_index, Arc::new(node)));
	let trees = Arc::new(RwLock::new(trees));

	let start_time = std::time::Instant::now();

	COMMITS.store(start_commit, Ordering::SeqCst);
	NEXT_COMMIT.store(start_commit, Ordering::SeqCst);
	NUM_REMOVED.store(num_removed, Ordering::SeqCst);

	{
		let db = db.clone();
		let shutdown = shutdown.clone();
		let shutdown_final = shutdown_final.clone();
		let total_commits = args.commits;
		let output_helper = output_helper.clone();

		threads.push(thread::spawn(move || {
			informant(
				db,
				shutdown,
				shutdown_final,
				total_commits,
				start_commit,
				start_num_nodes as usize,
				output_helper,
			)
		}));
	}

	for i in 0..args.readers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();
		let trees = trees.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("reader {i}"))
				.spawn(move || reader(db, args, chain_generator, trees, i as u64, shutdown))
				.unwrap(),
		);
	}

	let iter_start_index = args.readers;
	for i in 0..args.iter {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();
		let trees = trees.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("iter {i}"))
				.spawn(move || {
					iter(db, args, chain_generator, trees, (iter_start_index + i) as u64, shutdown)
				})
				.unwrap(),
		);
	}

	for i in 0..args.writers {
		let db = db.clone();
		let shutdown = shutdown.clone();
		let args = args.clone();
		let chain_generator = chain_generator.clone();
		let trees = trees.clone();

		threads.push(
			thread::Builder::new()
				.name(format!("writer {i}"))
				.spawn(move || writer(db, args, chain_generator, trees, shutdown, start_commit))
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

	output_helper.write().println(format!(
		"Completed {} commits in {} seconds. {} cps. {} queries, {} iterations",
		commits,
		elapsed_time,
		commits as f64 / elapsed_time,
		queries,
		iterations
	));

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
		output_helper
			.write()
			.println_final(format!("Removed all entries. Total time: {}", elapsed_time));
	}

	Ok(())
}
