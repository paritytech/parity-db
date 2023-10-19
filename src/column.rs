// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

use crate::{
	btree::BTreeTable,
	compress::Compress,
	db::{check::CheckDisplay, NodeChange, Operation, RcValue},
	display::hex,
	error::{try_io, Error, Result},
	index::{Address, IndexTable, PlanOutcome, TableId as IndexTableId},
	log::{Log, LogAction, LogOverlays, LogQuery, LogReader, LogWriter},
	multitree::{Children, NewNode, NodeAddress, NodeRef},
	options::{ColumnOptions, Metadata, Options, DEFAULT_COMPRESSION_THRESHOLD},
	parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard},
	ref_count::{RefCountTable, RefCountTableId},
	stats::{ColumnStatSummary, ColumnStats},
	table::{
		key::{TableKey, TableKeyQuery},
		TableId as ValueTableId, Value, ValueTable, SIZE_TIERS,
	},
	Key,
};
use std::{
	collections::{HashMap, VecDeque},
	path::PathBuf,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};

const MIN_INDEX_BITS: u8 = 16;
const MIN_REF_COUNT_BITS: u8 = 11;
// Measured in index entries
const MAX_REINDEX_BATCH: usize = 8192;

pub type ColId = u8;
pub type Salt = [u8; 32];

// The size tiers follow log distribution. Generated with the following code:
//
//{
//	let mut r = [0u16; SIZE_TIERS - 1];
//	let  start = MIN_ENTRY_SIZE as f64;
//	let  end = MAX_ENTRY_SIZE as f64;
//	let  n_slices = SIZE_TIERS - 1;
//	let factor = ((end.ln() - start.ln()) / (n_slices - 1) as f64).exp();
//
//	let mut s = start;
//	let mut i = 0;
//	while i <  n_slices {
//		r[i] = s.round() as u16;
//		s = s * factor;
//		i += 1;
//	}
//	r
//};

const SIZES: [u16; SIZE_TIERS - 1] = [
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 46, 47, 48, 50, 51, 52, 54, 55, 57, 58, 60,
	62, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 88, 90, 93, 95, 98, 101, 103, 106, 109,
	112, 115, 119, 122, 125, 129, 132, 136, 140, 144, 148, 152, 156, 160, 165, 169, 174, 179, 183,
	189, 194, 199, 205, 210, 216, 222, 228, 235, 241, 248, 255, 262, 269, 276, 284, 292, 300, 308,
	317, 325, 334, 344, 353, 363, 373, 383, 394, 405, 416, 428, 439, 452, 464, 477, 490, 504, 518,
	532, 547, 562, 577, 593, 610, 627, 644, 662, 680, 699, 718, 738, 758, 779, 801, 823, 846, 869,
	893, 918, 943, 969, 996, 1024, 1052, 1081, 1111, 1142, 1174, 1206, 1239, 1274, 1309, 1345,
	1382, 1421, 1460, 1500, 1542, 1584, 1628, 1673, 1720, 1767, 1816, 1866, 1918, 1971, 2025, 2082,
	2139, 2198, 2259, 2322, 2386, 2452, 2520, 2589, 2661, 2735, 2810, 2888, 2968, 3050, 3134, 3221,
	3310, 3402, 3496, 3593, 3692, 3794, 3899, 4007, 4118, 4232, 4349, 4469, 4593, 4720, 4850, 4984,
	5122, 5264, 5410, 5559, 5713, 5871, 6034, 6200, 6372, 6548, 6729, 6916, 7107, 7303, 7506, 7713,
	7927, 8146, 8371, 8603, 8841, 9085, 9337, 9595, 9860, 10133, 10413, 10702, 10998, 11302, 11614,
	11936, 12266, 12605, 12954, 13312, 13681, 14059, 14448, 14848, 15258, 15681, 16114, 16560,
	17018, 17489, 17973, 18470, 18981, 19506, 20046, 20600, 21170, 21756, 22358, 22976, 23612,
	24265, 24936, 25626, 26335, 27064, 27812, 28582, 29372, 30185, 31020, 31878, 32760,
];

#[derive(Debug)]
struct Tables {
	index: IndexTable,
	value: Vec<ValueTable>,
	ref_count: Option<RefCountTable>,
}

impl Tables {
	fn get_ref_count(&self) -> &RefCountTable {
		self.ref_count.as_ref().unwrap()
	}
}

#[derive(Debug)]
enum ReindexEntry {
	Index(IndexTable),
	RefCount(RefCountTable),
}

#[derive(Debug)]
struct Reindex {
	queue: VecDeque<ReindexEntry>,
	progress: AtomicU64,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Column {
	Hash(HashColumn),
	Tree(BTreeTable),
}

#[derive(Debug)]
pub struct HashColumn {
	col: ColId,
	tables: RwLock<Tables>,
	reindex: RwLock<Reindex>,
	ref_count_cache: Option<RwLock<HashMap<u64, u64>>>,
	path: PathBuf,
	preimage: bool,
	uniform_keys: bool,
	collect_stats: bool,
	ref_counted: bool,
	append_only: bool,
	salt: Salt,
	stats: ColumnStats,
	compression: Compress,
	db_version: u32,
}

#[derive(Clone, Copy)]
pub struct TablesRef<'a> {
	pub tables: &'a [ValueTable],
	pub compression: &'a Compress,
	pub col: ColId,
	pub preimage: bool,
	pub ref_counted: bool,
}

/// Value iteration state
pub struct ValueIterState {
	/// Reference counter.
	pub rc: u32,
	/// Value.
	pub value: Vec<u8>,
}

// Only used for DB validation and migration.
pub struct CorruptedIndexEntryInfo {
	pub chunk_index: u64,
	pub sub_index: u32,
	pub entry: crate::index::Entry,
	pub value_entry: Option<Vec<u8>>,
	pub error: Option<Error>,
}

// Only used for DB validation and migration.
pub struct IterState {
	pub item_index: u64,
	pub total_items: u64,
	pub key: Key,
	pub rc: u32,
	pub value: Vec<u8>,
}

// Only used for DB validation and migration.
enum IterStateOrCorrupted {
	Item(IterState),
	Corrupted(CorruptedIndexEntryInfo),
}

#[inline]
pub fn hash_key(key: &[u8], salt: &Salt, uniform: bool, db_version: u32) -> Key {
	use blake2::{
		digest::{typenum::U32, FixedOutput, Update},
		Blake2bMac,
	};

	let mut k = Key::default();
	if uniform {
		if db_version <= 5 {
			k.copy_from_slice(&key[0..32]);
		} else if db_version <= 7 {
			// XOR with salt.
			let key = &key[0..32];
			for i in 0..32 {
				k[i] = key[i] ^ salt[i];
			}
		} else {
			#[cfg(any(test, feature = "instrumentation"))]
			// Used for forcing collisions in tests.
			if salt == &Salt::default() {
				k.copy_from_slice(&key);
				return k
			}
			// siphash 1-3 first 128 bits of the key
			use siphasher::sip128::Hasher128;
			use std::hash::Hasher;
			let mut hasher = siphasher::sip128::SipHasher13::new_with_key(
				salt[..16].try_into().expect("Salt length is 32"),
			);
			hasher.write(&key);
			let hash = hasher.finish128();
			k[0..8].copy_from_slice(&hash.h1.to_le_bytes());
			k[8..16].copy_from_slice(&hash.h2.to_le_bytes());
			k[16..].copy_from_slice(&key[16..]);
		}
	} else {
		let mut ctx = Blake2bMac::<U32>::new_with_salt_and_personal(salt, &[], &[])
			.expect("Salt length (32) is a valid key length (<= 64)");
		ctx.update(key);
		let hash = ctx.finalize_fixed();
		k.copy_from_slice(&hash);
	}
	k
}

pub struct ReindexBatch {
	pub drop_index: Option<IndexTableId>,
	pub batch: Vec<(Key, Address)>,
	pub drop_ref_count: Option<RefCountTableId>,
	pub ref_count_batch: Vec<(Address, u64)>,
	pub ref_count_batch_source: Option<RefCountTableId>,
}

impl HashColumn {
	pub fn get(&self, key: &Key, log: &impl LogQuery) -> Result<Option<(Value, u32)>> {
		let tables = self.tables.read();
		let values = self.as_ref(&tables.value);
		if let Some((tier, rc, value)) = self.get_in_index(key, &tables.index, values, log)? {
			if self.collect_stats {
				self.stats.query_hit(tier);
			}
			return Ok(Some((value, rc)))
		}
		for entry in &self.reindex.read().queue {
			if let ReindexEntry::Index(r) = entry {
				if let Some((tier, rc, value)) = self.get_in_index(key, r, values, log)? {
					if self.collect_stats {
						self.stats.query_hit(tier);
					}
					return Ok(Some((value, rc)))
				}
			}
		}
		if self.collect_stats {
			self.stats.query_miss();
		}
		Ok(None)
	}

	pub fn get_size(&self, key: &Key, log: &RwLock<LogOverlays>) -> Result<Option<u32>> {
		Ok(self.get(key, log)?.map(|(v, _rc)| v.len() as u32))
	}

	pub fn get_value(&self, address: Address, log: &impl LogQuery) -> Result<Option<Value>> {
		let tables = self.tables.read();
		let values = self.as_ref(&tables.value);
		if let Some((tier, _rc, value)) =
			Column::get_value(TableKeyQuery::Check(&TableKey::NoHash), address, values, log)?
		{
			if self.collect_stats {
				self.stats.query_hit(tier);
			}
			return Ok(Some(value))
		}
		if self.collect_stats {
			self.stats.query_miss();
		}
		Ok(None)
	}

	fn get_in_index(
		&self,
		key: &Key,
		index: &IndexTable,
		tables: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<(u8, u32, Value)>> {
		let (mut entry, mut sub_index) = index.get(key, 0, log)?;
		while !entry.is_empty() {
			let address = entry.address(index.id.index_bits());
			let value = Column::get_value(
				TableKeyQuery::Check(&TableKey::Partial(*key)),
				address,
				tables,
				log,
			)?;
			match value {
				Some(result) => return Ok(Some(result)),
				None => {
					let (next_entry, next_index) = index.get(key, sub_index + 1, log)?;
					entry = next_entry;
					sub_index = next_index;
				},
			}
		}
		Ok(None)
	}

	pub fn as_ref<'a>(&'a self, tables: &'a [ValueTable]) -> TablesRef<'a> {
		TablesRef {
			tables,
			preimage: self.preimage,
			col: self.col,
			ref_counted: self.ref_counted,
			compression: &self.compression,
		}
	}
}

impl Column {
	pub fn get_value(
		mut key: TableKeyQuery,
		address: Address,
		tables: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<(u8, u32, Value)>> {
		let size_tier = address.size_tier() as usize;
		if let Some((value, compressed, rc)) =
			tables.tables[size_tier].query(&mut key, address.offset(), log)?
		{
			let value = if compressed { tables.compression.decompress(&value)? } else { value };
			return Ok(Some((size_tier as u8, rc, value)))
		}
		Ok(None)
	}

	pub fn compress(
		compression: &Compress,
		key: &TableKey,
		value: &[u8],
		tables: &[ValueTable],
	) -> (Option<Vec<u8>>, usize) {
		let (len, result) = if value.len() > compression.threshold as usize {
			let cvalue = compression.compress(value);
			if cvalue.len() < value.len() {
				(cvalue.len(), Some(cvalue))
			} else {
				(value.len(), None)
			}
		} else {
			(value.len(), None)
		};
		let target_tier = tables
			.iter()
			.position(|t| t.value_size(key).map_or(false, |s| len <= s as usize));
		let target_tier = target_tier.unwrap_or_else(|| {
			log::trace!(target: "parity-db", "Using blob {}", key);
			tables.len() - 1
		});

		(result, target_tier)
	}

	pub fn open(col: ColId, options: &Options, metadata: &Metadata) -> Result<Column> {
		let path = &options.path;
		let arc_path = Arc::new(path.clone());
		let column_options = &metadata.columns[col as usize];
		let db_version = metadata.version;
		let value = (0..SIZE_TIERS)
			.map(|i| Self::open_table(arc_path.clone(), col, i as u8, column_options, db_version))
			.collect::<Result<_>>()?;

		if column_options.btree_index {
			Ok(Column::Tree(BTreeTable::open(col, value, options, metadata)?))
		} else {
			Ok(Column::Hash(HashColumn::open(col, value, options, metadata)?))
		}
	}

	fn open_table(
		path: Arc<PathBuf>,
		col: ColId,
		tier: u8,
		options: &ColumnOptions,
		db_version: u32,
	) -> Result<ValueTable> {
		let id = ValueTableId::new(col, tier);
		let entry_size = SIZES.get(tier as usize).cloned();
		ValueTable::open(path, id, entry_size, options, db_version)
	}

	pub(crate) fn drop_files(column: ColId, path: PathBuf) -> Result<()> {
		// It is not specified how read_dir behaves when deleting and iterating in the same loop
		// We collect a list of paths to be deleted first.
		let mut to_delete = Vec::new();
		for entry in try_io!(std::fs::read_dir(&path)) {
			let entry = try_io!(entry);
			if let Some(file) = entry.path().file_name().and_then(|f| f.to_str()) {
				if crate::index::TableId::is_file_name(column, file) ||
					crate::table::TableId::is_file_name(column, file) ||
					crate::ref_count::RefCountTableId::is_file_name(column, file)
				{
					to_delete.push(PathBuf::from(file));
				}
			}
		}

		for file in to_delete {
			let mut path = path.clone();
			path.push(file);
			try_io!(std::fs::remove_file(path));
		}
		Ok(())
	}
}

pub fn packed_node_size(data: &Vec<u8>, num_children: u8) -> usize {
	1 + data.len() + num_children as usize * 8
}

pub fn pack_node_data(data: Vec<u8>, child_data: Vec<u8>, num_children: u8) -> Vec<u8> {
	[vec![num_children], data, child_data].concat()
}

pub fn unpack_node_data(data: Vec<u8>) -> Result<(Vec<u8>, Children)> {
	if data.len() == 0 {
		return Err(Error::InvalidValueData)
	}
	let num_children = data[0] as usize;
	let (_, data) = data.split_at(1);
	let child_buf_len = num_children * 8;
	if data.len() < child_buf_len {
		return Err(Error::InvalidValueData)
	}
	let (data, child_buf) = data.split_at(data.len() - child_buf_len);

	let mut children = Children::new();
	for i in 0..num_children {
		let node_address = u64::from_le_bytes(child_buf[i * 8..(i + 1) * 8].try_into().unwrap());
		children.push(node_address);
	}

	Ok((data.to_vec(), children))
}

impl HashColumn {
	fn open(
		col: ColId,
		value: Vec<ValueTable>,
		options: &Options,
		metadata: &Metadata,
	) -> Result<HashColumn> {
		let (index, mut reindexing, stats) = Self::open_index(&options.path, col)?;
		let collect_stats = options.stats;
		let path = &options.path;
		let col_options = &metadata.columns[col as usize];
		let db_version = metadata.version;
		let (ref_count, ref_count_cache) = if col_options.multitree && !col_options.append_only {
			(
				Some(Self::open_ref_count(&options.path, col, &mut reindexing)?),
				Some(RwLock::new(Default::default())),
			)
		} else {
			(None, None)
		};
		Ok(HashColumn {
			col,
			tables: RwLock::new(Tables { index, value, ref_count }),
			reindex: RwLock::new(Reindex { queue: reindexing, progress: AtomicU64::new(0) }),
			ref_count_cache,
			path: path.into(),
			preimage: col_options.preimage,
			uniform_keys: col_options.uniform,
			ref_counted: col_options.ref_counted,
			append_only: col_options.append_only,
			collect_stats,
			salt: metadata.salt,
			stats,
			compression: Compress::new(
				col_options.compression,
				options
					.compression_threshold
					.get(&col)
					.copied()
					.unwrap_or(DEFAULT_COMPRESSION_THRESHOLD),
			),
			db_version,
		})
	}

	pub fn init_table_data(&mut self) -> Result<()> {
		let mut tables = self.tables.write();
		for table in &mut tables.value {
			table.init_table_data()?;
		}

		if let Some(cache) = &self.ref_count_cache {
			let mut cache = cache.write();

			for entry in &self.reindex.read().queue {
				if let ReindexEntry::RefCount(table) = entry {
					for index in 0..table.id.total_chunks() {
						let entries = table.table_entries(index)?;
						for entry in entries.iter() {
							if !entry.is_empty() {
								cache.insert(entry.address().as_u64(), entry.ref_count());
							}
						}
					}
				}
			}

			let table = &tables.ref_count;
			if let Some(table) = table {
				for index in 0..table.id.total_chunks() {
					let entries = table.table_entries(index)?;
					for entry in entries.iter() {
						if !entry.is_empty() {
							cache.insert(entry.address().as_u64(), entry.ref_count());
						}
					}
				}
			}
		}

		Ok(())
	}

	pub fn hash_key(&self, key: &[u8]) -> Key {
		hash_key(key, &self.salt, self.uniform_keys, self.db_version)
	}

	pub fn flush(&self) -> Result<()> {
		let tables = self.tables.read();
		tables.index.flush()?;
		for t in tables.value.iter() {
			t.flush()?;
		}
		if tables.ref_count.is_some() {
			tables.get_ref_count().flush()?;
		}
		Ok(())
	}

	fn open_index(
		path: &std::path::Path,
		col: ColId,
	) -> Result<(IndexTable, VecDeque<ReindexEntry>, ColumnStats)> {
		let mut reindexing = VecDeque::new();
		let mut top = None;
		let mut stats = ColumnStats::empty();
		for bits in (MIN_INDEX_BITS..65).rev() {
			let id = IndexTableId::new(col, bits);
			if let Some(table) = IndexTable::open_existing(path, id)? {
				if top.is_none() {
					stats = table.load_stats()?;
					log::trace!(target: "parity-db", "Opened main index {}", table.id);
					top = Some(table);
				} else {
					log::trace!(target: "parity-db", "Opened stale index {}", table.id);
					reindexing.push_front(ReindexEntry::Index(table));
				}
			}
		}
		let table = match top {
			Some(table) => table,
			None => IndexTable::create_new(path, IndexTableId::new(col, MIN_INDEX_BITS)),
		};
		Ok((table, reindexing, stats))
	}

	fn open_ref_count(
		path: &std::path::Path,
		col: ColId,
		reindexing: &mut VecDeque<ReindexEntry>,
	) -> Result<RefCountTable> {
		let mut top = None;
		for bits in (MIN_REF_COUNT_BITS..65).rev() {
			let id = RefCountTableId::new(col, bits);
			if let Some(table) = RefCountTable::open_existing(path, id)? {
				if top.is_none() {
					log::trace!(target: "parity-db", "Opened main ref count {}", table.id);
					top = Some(table);
				} else {
					log::trace!(target: "parity-db", "Opened stale ref count {}", table.id);
					reindexing.push_front(ReindexEntry::RefCount(table));
				}
			}
		}
		let table = match top {
			Some(table) => table,
			None => RefCountTable::create_new(path, RefCountTableId::new(col, MIN_REF_COUNT_BITS)),
		};
		Ok(table)
	}

	fn trigger_reindex<'a, 'b>(
		tables: RwLockUpgradableReadGuard<'a, Tables>,
		reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		path: &std::path::Path,
	) -> (RwLockUpgradableReadGuard<'a, Tables>, RwLockUpgradableReadGuard<'b, Reindex>) {
		let mut tables = RwLockUpgradableReadGuard::upgrade(tables);
		let mut reindex = RwLockUpgradableReadGuard::upgrade(reindex);
		log::info!(
			target: "parity-db",
			"Started reindex for {}",
			tables.index.id,
		);
		// Start reindex
		let new_index_id =
			IndexTableId::new(tables.index.id.col(), tables.index.id.index_bits() + 1);
		let new_table = IndexTable::create_new(path, new_index_id);
		let old_table = std::mem::replace(&mut tables.index, new_table);
		reindex.queue.push_back(ReindexEntry::Index(old_table));
		(
			RwLockWriteGuard::downgrade_to_upgradable(tables),
			RwLockWriteGuard::downgrade_to_upgradable(reindex),
		)
	}

	pub fn write_reindex_plan(
		&self,
		key: &Key,
		address: Address,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		self.write_reindex_plan_locked(tables, reindex, key, address, log)
	}

	fn write_reindex_plan_locked(
		&self,
		mut tables: RwLockUpgradableReadGuard<Tables>,
		mut reindex: RwLockUpgradableReadGuard<Reindex>,
		key: &Key,
		address: Address,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		if Self::contains_partial_key_with_address(key, address, &tables.index, log)? {
			log::trace!(target: "parity-db", "{}: Skipped reindex entry {} when reindexing", tables.index.id, hex(key));
			return Ok(PlanOutcome::Skipped)
		}
		let mut outcome = PlanOutcome::Written;
		while let PlanOutcome::NeedReindex =
			tables.index.write_insert_plan(key, address, None, log)?
		{
			log::debug!(target: "parity-db", "{}: Index chunk full {} when reindexing", tables.index.id, hex(key));
			(tables, reindex) = Self::trigger_reindex(tables, reindex, self.path.as_path());
			outcome = PlanOutcome::NeedReindex;
		}
		Ok(outcome)
	}

	fn search_index<'a>(
		key: &Key,
		index: &'a IndexTable,
		tables: &'a Tables,
		log: &LogWriter,
	) -> Result<Option<(&'a IndexTable, usize, Address)>> {
		let (mut existing_entry, mut sub_index) = index.get(key, 0, log)?;
		while !existing_entry.is_empty() {
			let existing_address = existing_entry.address(index.id.index_bits());
			let existing_tier = existing_address.size_tier();
			let table_key = TableKey::Partial(*key);
			if tables.value[existing_tier as usize].has_key_at(
				existing_address.offset(),
				&table_key,
				log,
			)? {
				return Ok(Some((index, sub_index, existing_address)))
			}

			let (next_entry, next_index) = index.get(key, sub_index + 1, log)?;
			existing_entry = next_entry;
			sub_index = next_index;
		}
		Ok(None)
	}

	fn contains_partial_key_with_address(
		key: &Key,
		address: Address,
		index: &IndexTable,
		log: &LogWriter,
	) -> Result<bool> {
		let (mut existing_entry, mut sub_index) = index.get(key, 0, log)?;
		while !existing_entry.is_empty() {
			let existing_address = existing_entry.address(index.id.index_bits());
			if existing_address == address {
				return Ok(true)
			}
			let (next_entry, next_index) = index.get(key, sub_index + 1, log)?;
			existing_entry = next_entry;
			sub_index = next_index;
		}
		Ok(false)
	}

	fn search_all_indexes<'a>(
		key: &Key,
		tables: &'a Tables,
		reindex: &'a Reindex,
		log: &LogWriter,
	) -> Result<Option<(&'a IndexTable, usize, Address)>> {
		if let Some(r) = Self::search_index(key, &tables.index, tables, log)? {
			return Ok(Some(r))
		}
		// Check old indexes
		// TODO: don't search if index precedes reindex progress
		for entry in &reindex.queue {
			if let ReindexEntry::Index(index) = entry {
				if let Some(r) = Self::search_index(key, index, tables, log)? {
					return Ok(Some(r))
				}
			}
		}
		Ok(None)
	}

	pub fn write_plan(
		&self,
		change: &Operation<Key, RcValue>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let existing = Self::search_all_indexes(change.key(), &tables, &reindex, log)?;
		if let Some((table, sub_index, existing_address)) = existing {
			self.write_plan_existing(&tables, change, log, table, sub_index, existing_address)
		} else {
			match change {
				Operation::Set(key, value) => {
					let (r, _, _) =
						self.write_plan_new(tables, reindex, key, value.as_ref(), log)?;
					Ok(r)
				},
				Operation::Dereference(key) => {
					log::trace!(target: "parity-db", "{}: Deleting missing key {}", tables.index.id, hex(key));
					if self.collect_stats {
						self.stats.remove_miss();
					}
					Ok(PlanOutcome::Skipped)
				},
				Operation::Reference(key) => {
					log::trace!(target: "parity-db", "{}: Ignoring increase rc, missing key {}", tables.index.id, hex(key));
					if self.collect_stats {
						self.stats.reference_increase_miss();
					}
					Ok(PlanOutcome::Skipped)
				},
				Operation::InsertTree(..) |
				Operation::ReferenceTree(..) |
				Operation::DereferenceTree(..) =>
					Err(Error::InvalidConfiguration("Unsupported operation on hash column".into())),
			}
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn write_plan_existing(
		&self,
		tables: &Tables,
		change: &Operation<Key, RcValue>,
		log: &mut LogWriter,
		index: &IndexTable,
		sub_index: usize,
		existing_address: Address,
	) -> Result<PlanOutcome> {
		let stats = if self.collect_stats { Some(&self.stats) } else { None };

		let key = change.key();
		let table_key = TableKey::Partial(*key);
		match Column::write_existing_value_plan(
			&table_key,
			self.as_ref(&tables.value),
			existing_address,
			change,
			log,
			stats,
			self.ref_counted,
		)? {
			(Some(outcome), _) => Ok(outcome),
			(None, Some(value_address)) => {
				// If it was found in an older index we just insert a new entry. Reindex won't
				// overwrite it.
				let sub_index = if index.id == tables.index.id { Some(sub_index) } else { None };
				tables.index.write_insert_plan(key, value_address, sub_index, log)
			},
			(None, None) => {
				log::trace!(target: "parity-db", "{}: Removing from index {}", tables.index.id, hex(key));
				index.write_remove_plan(key, sub_index, log)?;
				Ok(PlanOutcome::Written)
			},
		}
	}

	fn write_plan_new<'a, 'b>(
		&self,
		mut tables: RwLockUpgradableReadGuard<'a, Tables>,
		mut reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		key: &Key,
		value: &[u8],
		log: &mut LogWriter,
	) -> Result<(
		PlanOutcome,
		RwLockUpgradableReadGuard<'a, Tables>,
		RwLockUpgradableReadGuard<'b, Reindex>,
	)> {
		let stats = self.collect_stats.then_some(&self.stats);
		let table_key = TableKey::Partial(*key);
		let address = Column::write_new_value_plan(
			&table_key,
			self.as_ref(&tables.value),
			value,
			log,
			stats,
		)?;
		let mut outcome = PlanOutcome::Written;
		while let PlanOutcome::NeedReindex =
			tables.index.write_insert_plan(key, address, None, log)?
		{
			log::debug!(target: "parity-db", "{}: Index chunk full {}", tables.index.id, hex(key));
			(tables, reindex) = Self::trigger_reindex(tables, reindex, self.path.as_path());
			outcome = PlanOutcome::NeedReindex;
		}
		Ok((outcome, tables, reindex))
	}

	fn trigger_ref_count_reindex<'a, 'b>(
		tables: RwLockUpgradableReadGuard<'a, Tables>,
		reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		path: &std::path::Path,
	) -> (RwLockUpgradableReadGuard<'a, Tables>, RwLockUpgradableReadGuard<'b, Reindex>) {
		let mut tables = RwLockUpgradableReadGuard::upgrade(tables);
		let mut reindex = RwLockUpgradableReadGuard::upgrade(reindex);
		log::info!(
			target: "parity-db",
			"Started reindex for ref count {}",
			tables.get_ref_count().id,
		);
		// Start reindex
		let new_id = RefCountTableId::new(
			tables.get_ref_count().id.col(),
			tables.get_ref_count().id.index_bits() + 1,
		);
		let new_table = Some(RefCountTable::create_new(path, new_id));
		let old_table = std::mem::replace(&mut tables.ref_count, new_table);
		reindex.queue.push_back(ReindexEntry::RefCount(old_table.unwrap()));
		(
			RwLockWriteGuard::downgrade_to_upgradable(tables),
			RwLockWriteGuard::downgrade_to_upgradable(reindex),
		)
	}

	pub fn write_ref_count_reindex_plan(
		&self,
		address: Address,
		ref_count: u64,
		source: RefCountTableId,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		self.write_ref_count_reindex_plan_locked(tables, reindex, address, ref_count, source, log)
	}

	fn write_ref_count_reindex_plan_locked(
		&self,
		mut tables: RwLockUpgradableReadGuard<Tables>,
		mut reindex: RwLockUpgradableReadGuard<Reindex>,
		address: Address,
		ref_count: u64,
		source: RefCountTableId,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		if let Some((_ref_count, _sub_index)) = tables.get_ref_count().get(address, log)? {
			log::trace!(target: "parity-db", "{}: Skipped ref count reindex entry {} when reindexing", tables.get_ref_count().id, address);
			return Ok(PlanOutcome::Skipped)
		}
		// An intermediate reindex table might contain a more recent value for the ref count so need
		// to check for this and skip.
		for entry in reindex.queue.iter().rev() {
			if let ReindexEntry::RefCount(ref_count_table) = entry {
				if ref_count_table.id == source {
					break
				}
				if let Some(_r) = Self::search_ref_count(address, ref_count_table, log)? {
					log::trace!(target: "parity-db", "{}: Skipped ref count reindex entry {} when reindexing", ref_count_table.id, address);
					return Ok(PlanOutcome::Skipped)
				}
			}
		}
		let mut outcome = PlanOutcome::Written;
		while let PlanOutcome::NeedReindex =
			tables.get_ref_count().write_insert_plan(address, ref_count, None, log)?
		{
			log::debug!(target: "parity-db", "{}: Ref count chunk full {} when reindexing", tables.get_ref_count().id, address);
			(tables, reindex) =
				Self::trigger_ref_count_reindex(tables, reindex, self.path.as_path());
			outcome = PlanOutcome::NeedReindex;
		}
		Ok(outcome)
	}

	fn search_ref_count<'a>(
		address: Address,
		ref_count_table: &'a RefCountTable,
		log: &LogWriter,
	) -> Result<Option<(&'a RefCountTable, usize, u64)>> {
		if let Some((ref_count, sub_index)) = ref_count_table.get(address, log)? {
			return Ok(Some((ref_count_table, sub_index, ref_count)))
		}
		Ok(None)
	}

	fn search_all_ref_count<'a>(
		address: Address,
		tables: &'a Tables,
		reindex: &'a Reindex,
		log: &LogWriter,
	) -> Result<Option<(&'a RefCountTable, usize, u64)>> {
		if let Some(r) = Self::search_ref_count(address, tables.get_ref_count(), log)? {
			return Ok(Some(r))
		}
		// Check old tables
		// TODO: don't search if table precedes reindex progress
		for entry in reindex.queue.iter().rev() {
			if let ReindexEntry::RefCount(ref_count_table) = entry {
				if let Some(r) = Self::search_ref_count(address, ref_count_table, log)? {
					return Ok(Some(r))
				}
			}
		}
		Ok(None)
	}

	fn write_ref_count_plan_existing<'a>(
		&self,
		tables: &Tables,
		reindex: &'a Reindex,
		change: (Address, Option<u64>),
		log: &mut LogWriter,
		ref_count_table: &RefCountTable,
		sub_index: usize,
	) -> Result<PlanOutcome> {
		let (address, ref_count) = change;
		if let Some(ref_count) = ref_count {
			// Replacing
			assert!(ref_count_table.id == tables.get_ref_count().id);
			tables
				.get_ref_count()
				.write_insert_plan(address, ref_count, Some(sub_index), log)
		} else {
			// Removing
			let result = ref_count_table.write_remove_plan(address, sub_index, log);
			// Need to remove from all old tables in reindex otherwise it will appear that this
			// entry still exists and it might get reintroduced during reindex.
			{
				if ref_count_table.id != tables.get_ref_count().id {
					if let Some((table, sub_index, _ref_count)) =
						Self::search_ref_count(address, tables.get_ref_count(), log)?
					{
						table.write_remove_plan(address, sub_index, log)?;
					}
				}
				for entry in &reindex.queue {
					if let ReindexEntry::RefCount(table) = entry {
						if table.id != ref_count_table.id {
							if let Some((table, sub_index, _ref_count)) =
								Self::search_ref_count(address, table, log)?
							{
								table.write_remove_plan(address, sub_index, log)?;
							}
						}
					}
				}
			}
			result
		}
	}

	fn write_ref_count_plan_new<'a, 'b>(
		&self,
		mut tables: RwLockUpgradableReadGuard<'a, Tables>,
		mut reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		address: Address,
		ref_count: u64,
		log: &mut LogWriter,
	) -> Result<(
		PlanOutcome,
		RwLockUpgradableReadGuard<'a, Tables>,
		RwLockUpgradableReadGuard<'b, Reindex>,
	)> {
		let mut outcome = PlanOutcome::Written;
		while let PlanOutcome::NeedReindex =
			tables.get_ref_count().write_insert_plan(address, ref_count, None, log)?
		{
			log::debug!(target: "parity-db", "{}: Ref count chunk full {}", tables.get_ref_count().id, address);
			(tables, reindex) =
				Self::trigger_ref_count_reindex(tables, reindex, self.path.as_path());
			outcome = PlanOutcome::NeedReindex;
		}
		let (test_ref_count, _test_sub_index) = tables.get_ref_count().get(address, log)?.unwrap();
		assert!(test_ref_count == ref_count);
		Ok((outcome, tables, reindex))
	}

	fn prepare_children(
		&self,
		children: &Vec<NodeRef>,
		tables: TablesRef,
		tier_count: &mut HashMap<usize, usize>,
	) -> Result<()> {
		for child in children {
			match child {
				NodeRef::New(node) => self.prepare_node(node, tables, tier_count)?,
				NodeRef::Existing(_address) => {},
			};
		}
		Ok(())
	}

	fn prepare_node(
		&self,
		node: &NewNode,
		tables: TablesRef,
		tier_count: &mut HashMap<usize, usize>,
	) -> Result<()> {
		let data_size = packed_node_size(&node.data, node.children.len() as u8);

		let table_key = TableKey::NoHash;

		/* let (cval, target_tier) =
			Column::compress(tables.compression, &table_key, data.as_ref(), tables.tables);
		let (cval, compressed) = cval
			.as_ref()
			.map(|cval| (cval.as_slice(), true))
			.unwrap_or((data.as_ref(), false));

		let cval: RcValue = cval.to_vec().into();
		let val = if compressed { data.into() } else { cval.clone() }; */

		let target_tier = tables
			.tables
			.iter()
			.position(|t| t.value_size(&table_key).map_or(false, |s| data_size <= s as usize));
		let target_tier = target_tier.unwrap_or_else(|| tables.tables.len() - 1);

		// Check it isn't multipart
		//assert!(target_tier < (SIZE_TIERS - 1));

		match tier_count.entry(target_tier) {
			std::collections::hash_map::Entry::Occupied(mut entry) => {
				*entry.get_mut() += 1;
			},
			std::collections::hash_map::Entry::Vacant(entry) => {
				entry.insert(1);
			},
		}

		self.prepare_children(&node.children, tables, tier_count)?;

		Ok(())
	}

	fn claim_children(
		&self,
		children: &Vec<NodeRef>,
		tables: TablesRef,
		tier_addresses: &HashMap<usize, Vec<u64>>,
		tier_index: &mut HashMap<usize, usize>,
		node_values: &mut Vec<NodeChange>,
	) -> Result<Vec<u8>> {
		let mut data = Vec::new();
		for child in children {
			let address = match child {
				NodeRef::New(node) =>
					self.claim_node(node, tables, tier_addresses, tier_index, node_values)?,
				NodeRef::Existing(address) => {
					if !self.append_only {
						node_values.push(NodeChange::IncrementReference(*address));
					}
					*address
				},
			};
			let mut data_buf = [0u8; 8];
			data_buf.copy_from_slice(&address.to_le_bytes());
			data.append(&mut data_buf.to_vec());
		}
		Ok(data)
	}

	fn claim_node(
		&self,
		node: &NewNode,
		tables: TablesRef,
		tier_addresses: &HashMap<usize, Vec<u64>>,
		tier_index: &mut HashMap<usize, usize>,
		node_values: &mut Vec<NodeChange>,
	) -> Result<NodeAddress> {
		let num_children = node.children.len();

		let data_size = packed_node_size(&node.data, num_children as u8);

		let table_key = TableKey::NoHash;

		let target_tier = tables
			.tables
			.iter()
			.position(|t| t.value_size(&table_key).map_or(false, |s| data_size <= s as usize));
		let target_tier = target_tier.unwrap_or_else(|| tables.tables.len() - 1);

		let index = *tier_index.get(&target_tier).unwrap();
		tier_index.insert(target_tier, index + 1);

		let offset = tier_addresses.get(&target_tier).unwrap()[index];

		let data = pack_node_data(
			node.data.clone(),
			self.claim_children(&node.children, tables, tier_addresses, tier_index, node_values)?,
			num_children as u8,
		);

		// Can't support compression as we need to know the size earlier to get the tier.
		let val: RcValue = data.into();
		let cval = val.clone();
		let compressed = false;

		let address = Address::new(offset, target_tier as u8);

		node_values.push(NodeChange::NewValue(address.as_u64(), val, cval, compressed));

		Ok(address.as_u64())
	}

	/// returns value for the root node and vector of NodeChange for nodes.
	pub fn claim_tree_values(
		&self,
		change: &Operation<Value, Value>,
	) -> Result<(Vec<u8>, Vec<NodeChange>)> {
		match change {
			Operation::InsertTree(_key, node) => {
				let tables = self.tables.upgradable_read();

				let values = self.as_ref(&tables.value);

				let mut tier_count: HashMap<usize, usize> = Default::default();
				self.prepare_children(&node.children, values, &mut tier_count)?;

				let mut tier_addresses: HashMap<usize, Vec<u64>> = Default::default();
				let mut tier_index: HashMap<usize, usize> = Default::default();
				for (tier, count) in tier_count {
					let offsets = values.tables[tier].claim_contiguous_entries(count, 8)?;
					tier_addresses.insert(tier, offsets);
					tier_index.insert(tier, 0);
				}

				let mut node_values: Vec<NodeChange> = Default::default();

				let num_children = node.children.len();
				let data = pack_node_data(
					node.data.clone(),
					self.claim_children(
						&node.children,
						values,
						&tier_addresses,
						&mut tier_index,
						&mut node_values,
					)?,
					num_children as u8,
				);

				return Ok((data, node_values))
			},
			Operation::ReferenceTree(..) | Operation::DereferenceTree(..) =>
				return Err(Error::InvalidInput(format!(
					"claim_tree_values should not be called from ReferenceTree or DereferenceTree"
				))),
			_ =>
				return Err(Error::InvalidInput(format!(
					"Invalid operation for column {}",
					self.col
				))),
		}
	}

	pub fn write_address_value_plan(
		&self,
		address: u64,
		cval: RcValue,
		compressed: bool,
		val_len: u32,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let tables = self.as_ref(&tables.value);
		let address = Address::from_u64(address);
		let target_tier = address.size_tier();
		let offset = address.offset();
		tables.tables[target_tier as usize].write_claimed_plan(
			offset,
			&TableKey::NoHash,
			cval.as_ref(),
			log,
			compressed,
		)?;

		let stats = self.collect_stats.then_some(&self.stats);
		if let Some(stats) = stats {
			stats.insert_val(val_len, cval.value().len() as u32);
		}

		Ok(PlanOutcome::Written)
	}

	pub fn write_address_inc_ref_plan(
		&self,
		address: u64,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let address = Address::from_u64(address);
		let cached = self
			.ref_count_cache
			.as_ref()
			.map_or(None, |c| c.read().get(&address.as_u64()).cloned());
		if let Some(cached_ref_count) = cached {
			let existing: Option<(&RefCountTable, usize, u64)> =
				Self::search_all_ref_count(address, &tables, &reindex, log)?;
			let (table, sub_index, table_ref_count) = existing.unwrap();
			assert!(cached_ref_count > 1);
			assert!(table_ref_count == cached_ref_count);
			let new_ref_count = cached_ref_count + 1;
			self.ref_count_cache
				.as_ref()
				.map(|c| c.write().insert(address.as_u64(), new_ref_count));
			if table.id == tables.get_ref_count().id {
				self.write_ref_count_plan_existing(
					&tables,
					&reindex,
					(address, Some(new_ref_count)),
					log,
					table,
					sub_index,
				)
			} else {
				let (r, _, _) =
					self.write_ref_count_plan_new(tables, reindex, address, new_ref_count, log)?;
				Ok(r)
			}
		} else {
			// inc ref is only called on addresses that already exist, so we know they must have
			// only 1 reference.
			self.ref_count_cache.as_ref().map(|c| c.write().insert(address.as_u64(), 2));
			let (r, _, _) = self.write_ref_count_plan_new(tables, reindex, address, 2, log)?;
			Ok(r)
		}
	}

	pub fn write_address_dec_ref_plan(
		&self,
		address: u64,
		log: &mut LogWriter,
	) -> Result<(bool, PlanOutcome)> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let address = Address::from_u64(address);
		let cached = self
			.ref_count_cache
			.as_ref()
			.map_or(None, |c| c.read().get(&address.as_u64()).cloned());
		if let Some(cached_ref_count) = cached {
			let existing: Option<(&RefCountTable, usize, u64)> =
				Self::search_all_ref_count(address, &tables, &reindex, log)?;
			let (table, sub_index, table_ref_count) = existing.unwrap();
			assert!(cached_ref_count > 1);
			assert!(table_ref_count == cached_ref_count);
			let new_ref_count = cached_ref_count - 1;
			self.ref_count_cache.as_ref().map(|c| {
				if new_ref_count > 1 {
					c.write().insert(address.as_u64(), new_ref_count);
				} else {
					c.write().remove(&address.as_u64());
				}
			});
			let new_ref_count = if new_ref_count > 1 { Some(new_ref_count) } else { None };
			let outcome = if new_ref_count.is_some() && table.id != tables.get_ref_count().id {
				let (r, _, _) = self.write_ref_count_plan_new(
					tables,
					reindex,
					address,
					new_ref_count.unwrap(),
					log,
				)?;
				r
			} else {
				self.write_ref_count_plan_existing(
					&tables,
					&reindex,
					(address, new_ref_count),
					log,
					table,
					sub_index,
				)?
			};
			Ok((true, outcome))
		} else {
			// dec ref is only called on addresses that already exist, so we know they must have
			// only 1 reference.
			let tables = self.as_ref(&tables.value);
			let target_tier = address.size_tier();
			let offset = address.offset();
			tables.tables[target_tier as usize].write_remove_plan(offset, log)?;
			Ok((false, PlanOutcome::Written))
		}
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.enact_plan(record.index, log)?;
				} else if let Some(table) = reindex
					.queue
					.iter()
					.filter_map(|s| if let ReindexEntry::Index(t) = s { Some(t) } else { None })
					.find(|r| r.id == record.table)
				{
					table.enact_plan(record.index, log)?;
				} else {
					// This may happen when removal is planned for an old index when reindexing.
					// We can safely skip the removal since the new index does not have the entry
					// anyway and the old index is already dropped.
					log::debug!(
						target: "parity-db",
						"Missing index {}. Skipped",
						record.table,
					);
					IndexTable::skip_plan(log)?;
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].enact_plan(record.index, log)?;
			},
			LogAction::InsertRefCount(record) => {
				if tables.get_ref_count().id == record.table {
					tables.get_ref_count().enact_plan(record.index, log)?;
				} else if let Some(table) = reindex
					.queue
					.iter()
					.filter_map(|s| if let ReindexEntry::RefCount(t) = s { Some(t) } else { None })
					.find(|r| r.id == record.table)
				{
					table.enact_plan(record.index, log)?;
				} else {
					// This may happen when removal is planned for an old ref count when reindexing.
					// We can safely skip the removal since the new ref count does not have the
					// entry anyway and the old ref count is already dropped.
					log::debug!(
						target: "parity-db",
						"Missing ref count {}. Skipped",
						record.table,
					);
					RefCountTable::skip_plan(log)?;
				}
			},
			// This should never happen, unless something has modified the log file while the
			// database is running. Existing logs should be validated with `validate_plan` on
			// startup.
			_ => return Err(Error::Corruption("Unexpected log action".into())),
		}
		Ok(())
	}

	pub fn validate_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.validate_plan(record.index, log)?;
				} else if let Some(table) = reindex
					.queue
					.iter()
					.filter_map(|s| if let ReindexEntry::Index(t) = s { Some(t) } else { None })
					.find(|r| r.id == record.table)
				{
					table.validate_plan(record.index, log)?;
				} else {
					if record.table.index_bits() < tables.index.id.index_bits() {
						// Insertion into a previously dropped index.
						log::warn!( target: "parity-db", "Index {} is too old. Current is {}", record.table, tables.index.id);
						return Err(Error::Corruption("Unexpected log index id".to_string()))
					}
					// Re-launch previously started reindex
					// TODO: add explicit log records for reindexing events.
					log::warn!(
						target: "parity-db",
						"Missing table {}, starting reindex",
						record.table,
					);
					let lock = Self::trigger_reindex(tables, reindex, self.path.as_path());
					std::mem::drop(lock);
					return self.validate_plan(LogAction::InsertIndex(record), log)
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].validate_plan(record.index, log)?;
			},
			LogAction::InsertRefCount(record) => {
				if tables.get_ref_count().id == record.table {
					tables.get_ref_count().validate_plan(record.index, log)?;
				} else if let Some(table) = reindex
					.queue
					.iter()
					.filter_map(|s| if let ReindexEntry::RefCount(t) = s { Some(t) } else { None })
					.find(|r| r.id == record.table)
				{
					table.validate_plan(record.index, log)?;
				} else {
					if record.table.index_bits() < tables.get_ref_count().id.index_bits() {
						// Insertion into a previously dropped ref count.
						log::warn!( target: "parity-db", "Ref count {} is too old. Current is {}", record.table, tables.get_ref_count().id);
						return Err(Error::Corruption("Unexpected log ref count id".to_string()))
					}
					// Re-launch previously started reindex
					// TODO: add explicit log records for reindexing events.
					log::warn!(
						target: "parity-db",
						"Missing ref count {}, starting reindex",
						record.table,
					);
					let lock =
						Self::trigger_ref_count_reindex(tables, reindex, self.path.as_path());
					std::mem::drop(lock);
					return self.validate_plan(LogAction::InsertRefCount(record), log)
				}
			},
			_ => {
				log::error!(target: "parity-db", "Unexpected log action");
				return Err(Error::Corruption("Unexpected log action".to_string()))
			},
		}
		Ok(())
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.value.iter() {
			t.complete_plan(log)?;
		}
		if self.collect_stats {
			self.stats.commit()
		}
		Ok(())
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		let tables = self.tables.read();
		for t in tables.value.iter() {
			t.refresh_metadata()?;
		}
		Ok(())
	}

	pub fn write_stats_text(&self, writer: &mut impl std::io::Write) -> Result<()> {
		let tables = self.tables.read();
		tables.index.write_stats(&self.stats)?;
		self.stats.write_stats_text(writer, tables.index.id.col()).map_err(Error::Io)
	}

	fn stat_summary(&self) -> ColumnStatSummary {
		self.stats.summary()
	}

	fn clear_stats(&self) -> Result<()> {
		let tables = self.tables.read();
		self.stats.clear();
		tables.index.write_stats(&self.stats)
	}

	pub fn iter_values(&self, log: &Log, mut f: impl FnMut(ValueIterState) -> bool) -> Result<()> {
		let tables = self.tables.read();
		for table in &tables.value {
			log::debug!( target: "parity-db", "{}: Iterating table {}", tables.index.id, table.id);
			table.iter_while(log.overlays(), |_, rc, value, compressed| {
				let value = if compressed {
					if let Ok(value) = self.compression.decompress(&value) {
						value
					} else {
						return false
					}
				} else {
					value
				};
				let state = ValueIterState { rc, value };
				f(state)
			})?;
			log::debug!( target: "parity-db", "{}: Done iterating table {}", tables.index.id, table.id);
		}
		Ok(())
	}

	pub fn iter_index(&self, log: &Log, mut f: impl FnMut(IterState) -> bool) -> Result<()> {
		let action = |state| match state {
			IterStateOrCorrupted::Item(item) => Ok(f(item)),
			IterStateOrCorrupted::Corrupted(..) =>
				Err(Error::Corruption("Missing indexed value".into())),
		};
		self.iter_index_internal(log, action, 0)
	}

	fn iter_index_internal(
		&self,
		log: &Log,
		mut f: impl FnMut(IterStateOrCorrupted) -> Result<bool>,
		start_chunk: u64,
	) -> Result<()> {
		let tables = self.tables.read();
		let source = &tables.index;
		let total_chunks = source.id.total_chunks();

		for c in start_chunk..total_chunks {
			let entries = source.entries(c, log.overlays())?;
			for (sub_index, entry) in entries.iter().enumerate() {
				if entry.is_empty() {
					continue
				}
				let (size_tier, offset) = {
					let address = entry.address(source.id.index_bits());
					(address.size_tier(), address.offset())
				};

				let value = tables.value[size_tier as usize].get_with_meta(offset, log.overlays());
				let (value, rc, pk, compressed) = match value {
					Ok(Some(v)) => v,
					Ok(None) => {
						let value_entry = tables.value[size_tier as usize].dump_entry(offset).ok();
						if !f(IterStateOrCorrupted::Corrupted(CorruptedIndexEntryInfo {
							chunk_index: c,
							sub_index: sub_index as u32,
							value_entry,
							entry: *entry,
							error: None,
						}))? {
							return Ok(())
						}
						continue
					},
					Err(e) => {
						let value_entry = if let Error::Corruption(_) = &e {
							tables.value[size_tier as usize].dump_entry(offset).ok()
						} else {
							None
						};
						if !f(IterStateOrCorrupted::Corrupted(CorruptedIndexEntryInfo {
							chunk_index: c,
							sub_index: sub_index as u32,
							value_entry,
							entry: *entry,
							error: Some(e),
						}))? {
							return Ok(())
						}
						continue
					},
				};
				let mut key = source.recover_key_prefix(c, *entry);
				key[6..].copy_from_slice(&pk);
				let value = if compressed { self.compression.decompress(&value)? } else { value };
				log::debug!(
					target: "parity-db",
					"{}: Iterating at {}/{}, key={:?}, pk={:?}",
					source.id,
					c,
					source.id.total_chunks(),
					hex(&key),
					hex(&pk),
				);
				let state = IterStateOrCorrupted::Item(IterState {
					item_index: c,
					total_items: total_chunks,
					key,
					rc,
					value,
				});
				if !f(state)? {
					return Ok(())
				}
			}
		}
		Ok(())
	}

	fn iter_index_fast(
		&self,
		log: &Log,
		mut f: impl FnMut(IterStateOrCorrupted) -> Result<bool>,
		_start_chunk: u64,
	) -> Result<()> {
		let tables = self.tables.read();
		let index = &tables.index;

		let entries = index.sorted_entries()?;
		let total = entries.len();
		for (sub_index, entry) in entries.into_iter().enumerate() {
			let (size_tier, offset) = {
				let address = entry.address(index.id.index_bits());
				(address.size_tier(), address.offset())
			};

			let value = tables.value[size_tier as usize].get_with_meta(offset, log.overlays());
			let (value, rc, pk, compressed) = match value {
				Ok(Some(v)) => v,
				Ok(None) => {
					let value_entry = tables.value[size_tier as usize].dump_entry(offset).ok();
					if !f(IterStateOrCorrupted::Corrupted(CorruptedIndexEntryInfo {
						chunk_index: sub_index as u64,
						sub_index: sub_index as u32,
						value_entry,
						entry,
						error: None,
					}))? {
						return Ok(())
					}
					continue
				},
				Err(e) => {
					let value_entry = if let Error::Corruption(_) = &e {
						tables.value[size_tier as usize].dump_entry(offset).ok()
					} else {
						None
					};
					if !f(IterStateOrCorrupted::Corrupted(CorruptedIndexEntryInfo {
						chunk_index: sub_index as u64,
						sub_index: sub_index as u32,
						value_entry,
						entry,
						error: Some(e),
					}))? {
						return Ok(())
					}
					continue
				},
			};
			let value = if compressed { self.compression.decompress(&value)? } else { value };
			log::debug!(
				target: "parity-db",
				"{}: Iterating at {}/{}, pk={:?}",
				index.id,
				sub_index,
				total,
				hex(&pk),
			);
			let state = IterStateOrCorrupted::Item(IterState {
				item_index: sub_index as u64,
				total_items: total as u64,
				key: Default::default(),
				rc,
				value,
			});
			if !f(state)? {
				return Ok(())
			}
		}
		Ok(())
	}

	fn dump(&self, log: &Log, check_params: &crate::CheckOptions, col: ColId) -> Result<()> {
		let start_chunk = check_params.from.unwrap_or(0);
		let end_chunk = check_params.bound;

		let step = if check_params.fast { 1_000_000 } else { 10_000 };
		let (denom, suffix) = if check_params.fast { (1_000_000, "m") } else { (1_000, "k") };
		let mut next_info_at = step;
		let start_time = std::time::Instant::now();
		let index_id = self.tables.read().index.id;
		log::info!(target: "parity-db", "Column {} (hash): Starting index validation", col);
		let iter_fn =
			if check_params.fast { Self::iter_index_fast } else { Self::iter_index_internal };
		iter_fn(
			self,
			log,
			|state| match state {
				IterStateOrCorrupted::Item(IterState {
					item_index,
					total_items,
					key,
					rc,
					value,
				}) => {
					if Some(item_index) == end_chunk {
						return Ok(false)
					}
					if item_index >= next_info_at {
						next_info_at += step;
						log::info!(target: "parity-db", "Validated {}{} / {}{} entries", item_index / denom, suffix, total_items / denom, suffix);
					}

					match check_params.display {
						CheckDisplay::Full => {
							log::info!(
								"Index key: {:x?}\n \
							\tRc: {}",
								&key,
								rc,
							);
							log::info!("Value: {}", hex(&value));
						},
						CheckDisplay::Short(t) => {
							log::info!("Index key: {:x?}", &key);
							log::info!("Rc: {}, Value len: {}", rc, value.len());
							log::info!(
								"Value: {}",
								hex(&value[..std::cmp::min(t as usize, value.len())])
							);
						},
						CheckDisplay::None => (),
					}
					Ok(true)
				},
				IterStateOrCorrupted::Corrupted(c) => {
					log::error!(
						"Corrupted value for index entry: [{}][{}]: {} ({:?}). Error: {:?}",
						c.chunk_index,
						c.sub_index,
						c.entry.address(index_id.index_bits()),
						hex(&c.entry.as_u64().to_le_bytes()),
						c.error,
					);
					if let Some(v) = c.value_entry {
						log::error!("Value entry: {:?}", hex(v.as_slice()));
					}
					Ok(true)
				},
			},
			start_chunk,
		)?;

		log::info!(target: "parity-db", "Index validation complete successfully, elapsed {:?}", start_time.elapsed());
		if check_params.validate_free_refs {
			log::info!(target: "parity-db", "Validating free refs");
			let tables = self.tables.read();
			let mut total = 0;
			for t in &tables.value {
				match t.check_free_refs() {
					Err(e) => log::warn!(target: "parity-db", "{}: Error: {:?}", t.id, e),
					Ok(n) => total += n,
				}
			}
			log::info!(target: "parity-db", "{} Total free refs", total);
		}
		Ok(())
	}

	pub fn reindex(&self, log: &Log) -> Result<ReindexBatch> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		let mut plan = Vec::new();
		let mut drop_index = None;
		let mut ref_count_plan = Vec::new();
		let mut ref_count_source = None;
		let mut drop_ref_count = None;
		if let Some(source) = reindex.queue.front() {
			let progress = reindex.progress.load(Ordering::Relaxed);
			match source {
				ReindexEntry::Index(source) => {
					if progress != source.id.total_chunks() {
						let mut source_index = progress;
						if source_index % 500 == 0 {
							log::debug!(target: "parity-db", "{}: Reindexing at {}/{}", tables.index.id, source_index, source.id.total_chunks());
						}
						log::debug!(target: "parity-db", "{}: Continue reindex at {}/{}", tables.index.id, source_index, source.id.total_chunks());
						while source_index < source.id.total_chunks() &&
							plan.len() < MAX_REINDEX_BATCH
						{
							log::trace!(target: "parity-db", "{}: Reindexing {}", source.id, source_index);
							let entries = source.entries(source_index, log.overlays())?;
							for entry in entries.iter() {
								if entry.is_empty() {
									continue
								}
								// We only need key prefix to reindex.
								let key = source.recover_key_prefix(source_index, *entry);
								plan.push((key, entry.address(source.id.index_bits())))
							}
							source_index += 1;
						}
						log::trace!(target: "parity-db", "{}: End reindex batch {} ({})", tables.index.id, source_index, plan.len());
						reindex.progress.store(source_index, Ordering::Relaxed);
						if source_index == source.id.total_chunks() {
							log::info!(target: "parity-db", "Completed reindex {} into {}", source.id, tables.index.id);
							drop_index = Some(source.id);
						}
					}
				},
				ReindexEntry::RefCount(source) =>
					if progress != source.id.total_chunks() {
						let mut source_index = progress;
						if source_index % 500 == 0 {
							log::debug!(target: "parity-db", "{}: Reindexing ref count at {}/{}", tables.get_ref_count().id, source_index, source.id.total_chunks());
						}
						ref_count_source = Some(source.id);
						log::debug!(target: "parity-db", "{}: Continue reindex ref count at {}/{}", tables.get_ref_count().id, source_index, source.id.total_chunks());
						while source_index < source.id.total_chunks() &&
							ref_count_plan.len() < MAX_REINDEX_BATCH
						{
							log::trace!(target: "parity-db", "{}: Reindexing ref count {}", source.id, source_index);
							let entries = source.entries(source_index, log.overlays())?;
							for entry in entries.iter() {
								if entry.is_empty() {
									continue
								}
								ref_count_plan.push((entry.address(), entry.ref_count()));
							}
							source_index += 1;
						}
						log::trace!(target: "parity-db", "{}: End reindex ref count batch {} ({})", tables.get_ref_count().id, source_index, ref_count_plan.len());
						reindex.progress.store(source_index, Ordering::Relaxed);
						if source_index == source.id.total_chunks() {
							log::info!(target: "parity-db", "Completed reindex ref count {} into {}", source.id, tables.get_ref_count().id);
							drop_ref_count = Some(source.id);
						}
					},
			}
		}
		Ok(ReindexBatch {
			drop_index,
			batch: plan,
			drop_ref_count,
			ref_count_batch: ref_count_plan,
			ref_count_batch_source: ref_count_source,
		})
	}

	pub fn drop_index(&self, id: IndexTableId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		let mut reindex = self.reindex.write();
		if reindex.queue.front_mut().map_or(false, |e| {
			if let ReindexEntry::Index(t) = e {
				t.id == id
			} else {
				false
			}
		}) {
			reindex.progress.store(0, Ordering::Relaxed);
			let table = reindex.queue.pop_front().unwrap();
			let table = if let ReindexEntry::Index(table) = table {
				table
			} else {
				return Err(Error::Corruption(format!("Incorrect reindex type")))
			};
			table.drop_file()?;
		} else {
			log::warn!(target: "parity-db", "Dropping invalid index {}", id);
			return Ok(())
		}
		log::debug!(target: "parity-db", "Dropped {}", id);
		Ok(())
	}

	pub fn drop_ref_count(&self, id: RefCountTableId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping ref count {}", id);
		let mut reindex = self.reindex.write();
		if reindex.queue.front_mut().map_or(false, |e| {
			if let ReindexEntry::RefCount(t) = e {
				t.id == id
			} else {
				false
			}
		}) {
			reindex.progress.store(0, Ordering::Relaxed);
			let table = reindex.queue.pop_front().unwrap();
			let table = if let ReindexEntry::RefCount(table) = table {
				table
			} else {
				return Err(Error::Corruption(format!("Incorrect reindex type")))
			};
			table.drop_file()?;
		} else {
			log::warn!(target: "parity-db", "Dropping invalid ref count {}", id);
			return Ok(())
		}
		log::debug!(target: "parity-db", "Dropped ref count {}", id);
		Ok(())
	}

	pub fn get_num_value_entries(&self) -> Result<u64> {
		let tables = self.tables.read();
		let mut num_entries = 0;
		for value_table in &tables.value {
			num_entries += value_table.get_num_entries()?;
		}
		Ok(num_entries)
	}
}

impl Column {
	pub fn write_existing_value_plan<K, V: AsRef<[u8]>>(
		key: &TableKey,
		tables: TablesRef,
		address: Address,
		change: &Operation<K, V>,
		log: &mut LogWriter,
		stats: Option<&ColumnStats>,
		ref_counted: bool,
	) -> Result<(Option<PlanOutcome>, Option<Address>)> {
		let tier = address.size_tier() as usize;

		let fetch_size = || -> Result<(u32, u32)> {
			let (cur_size, compressed) =
				tables.tables[tier].size(key, address.offset(), log)?.unwrap_or((0, false));
			Ok(if compressed {
				// This is very costly.
				let compressed = tables.tables[tier]
					.get(key, address.offset(), log)?
					.expect("Same query as size")
					.0;
				let uncompressed = tables.compression.decompress(compressed.as_slice())?;

				(cur_size, uncompressed.len() as u32)
			} else {
				(cur_size, cur_size)
			})
		};

		match change {
			Operation::Reference(_) =>
				if ref_counted {
					log::trace!(target: "parity-db", "{}: Increment ref {}", tables.col, key);
					tables.tables[tier].write_inc_ref(address.offset(), log)?;
					if let Some(stats) = stats {
						stats.reference_increase();
					}
					Ok((Some(PlanOutcome::Written), None))
				} else {
					Ok((Some(PlanOutcome::Skipped), None))
				},
			Operation::Set(_, val) => {
				if ref_counted {
					log::trace!(target: "parity-db", "{}: Increment ref {}", tables.col, key);
					tables.tables[tier].write_inc_ref(address.offset(), log)?;
					return Ok((Some(PlanOutcome::Written), None))
				}
				if tables.preimage {
					// Replace is not supported
					return Ok((Some(PlanOutcome::Skipped), None))
				}

				let (cval, target_tier) =
					Column::compress(tables.compression, key, val.as_ref(), tables.tables);
				let (cval, compressed) = cval
					.as_ref()
					.map(|cval| (cval.as_slice(), true))
					.unwrap_or((val.as_ref(), false));

				if let Some(stats) = stats {
					let (cur_size, uncompressed) = fetch_size()?;
					stats.replace_val(
						cur_size,
						uncompressed,
						val.as_ref().len() as u32,
						cval.len() as u32,
					);
				}
				if tier == target_tier {
					log::trace!(target: "parity-db", "{}: Replacing {}", tables.col, key);
					tables.tables[target_tier].write_replace_plan(
						address.offset(),
						key,
						cval,
						log,
						compressed,
					)?;
					Ok((Some(PlanOutcome::Written), None))
				} else {
					log::trace!(target: "parity-db", "{}: Replacing in a new table {}", tables.col, key);
					tables.tables[tier].write_remove_plan(address.offset(), log)?;
					let new_offset =
						tables.tables[target_tier].write_insert_plan(key, cval, log, compressed)?;
					let new_address = Address::new(new_offset, target_tier as u8);
					Ok((None, Some(new_address)))
				}
			},
			Operation::Dereference(_) => {
				// Deletion
				let cur_size = if stats.is_some() { Some(fetch_size()?) } else { None };
				let remove = if ref_counted {
					let removed = !tables.tables[tier].write_dec_ref(address.offset(), log)?;
					log::trace!(target: "parity-db", "{}: Dereference {}, deleted={}", tables.col, key, removed);
					removed
				} else {
					log::trace!(target: "parity-db", "{}: Deleting {}", tables.col, key);
					tables.tables[tier].write_remove_plan(address.offset(), log)?;
					true
				};
				if remove {
					if let Some((compressed_size, uncompressed_size)) = cur_size {
						if let Some(stats) = stats {
							stats.remove_val(uncompressed_size, compressed_size);
						}
					}
					Ok((None, None))
				} else {
					Ok((Some(PlanOutcome::Written), None))
				}
			},
			Operation::InsertTree(..) |
			Operation::ReferenceTree(..) |
			Operation::DereferenceTree(..) =>
				Err(Error::InvalidInput(format!("Invalid operation for column {}", tables.col))),
		}
	}

	pub fn write_new_value_plan(
		key: &TableKey,
		tables: TablesRef,
		val: &[u8],
		log: &mut LogWriter,
		stats: Option<&ColumnStats>,
	) -> Result<Address> {
		let (cval, target_tier) = Column::compress(tables.compression, key, val, tables.tables);
		let (cval, compressed) =
			cval.as_ref().map(|cval| (cval.as_slice(), true)).unwrap_or((val, false));

		log::trace!(target: "parity-db", "{}: Inserting new {}, size = {}", tables.col, key, cval.len());
		let offset = tables.tables[target_tier].write_insert_plan(key, cval, log, compressed)?;
		let address = Address::new(offset, target_tier as u8);

		if let Some(stats) = stats {
			stats.insert_val(val.len() as u32, cval.len() as u32);
		}
		Ok(address)
	}

	pub fn complete_plan(&self, log: &mut LogWriter) -> Result<()> {
		match self {
			Column::Hash(column) => column.complete_plan(log),
			Column::Tree(column) => column.complete_plan(log),
		}
	}

	pub fn validate_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		match self {
			Column::Hash(column) => column.validate_plan(action, log),
			Column::Tree(column) => column.validate_plan(action, log),
		}
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		match self {
			Column::Hash(column) => column.enact_plan(action, log),
			Column::Tree(column) => column.enact_plan(action, log),
		}
	}

	pub fn init_table_data(&mut self) -> Result<()> {
		match self {
			Column::Hash(column) => column.init_table_data(),
			Column::Tree(_column) => Ok(()),
		}
	}

	pub fn flush(&self) -> Result<()> {
		match self {
			Column::Hash(column) => column.flush(),
			Column::Tree(column) => column.flush(),
		}
	}

	pub fn refresh_metadata(&self) -> Result<()> {
		match self {
			Column::Hash(column) => column.refresh_metadata(),
			Column::Tree(column) => column.refresh_metadata(),
		}
	}

	pub fn write_stats_text(&self, writer: &mut impl std::io::Write) -> Result<()> {
		match self {
			Column::Hash(column) => column.write_stats_text(writer),
			Column::Tree(_column) => Ok(()),
		}
	}

	pub fn clear_stats(&self) -> Result<()> {
		match self {
			Column::Hash(column) => column.clear_stats(),
			Column::Tree(_column) => Ok(()),
		}
	}

	pub fn stats(&self) -> Option<ColumnStatSummary> {
		match self {
			Column::Hash(column) => Some(column.stat_summary()),
			Column::Tree(_column) => None,
		}
	}

	pub fn dump(&self, log: &Log, check_params: &crate::CheckOptions, col: ColId) -> Result<()> {
		match self {
			Column::Hash(column) => column.dump(log, check_params, col),
			Column::Tree(_column) => Ok(()),
		}
	}

	#[cfg(test)]
	#[cfg(feature = "instrumentation")]
	pub fn index_bits(&self) -> Option<u8> {
		match self {
			Column::Hash(column) => Some(column.tables.read().index.id.index_bits()),
			Column::Tree(_column) => None,
		}
	}
}
