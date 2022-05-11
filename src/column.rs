// Copyright 2015-2022 Parity Technologies (UK) Ltd.
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

use crate::{
	btree::BTreeTable,
	compress::Compress,
	db::check::CheckDisplay,
	display::hex,
	error::{Error, Result},
	index::{Address, IndexTable, PlanOutcome, TableId as IndexTableId},
	log::{Log, LogAction, LogOverlays, LogQuery, LogReader, LogWriter},
	options::{ColumnOptions, Metadata, Options},
	stats::ColumnStats,
	table::{
		key::{TableKey, TableKeyQuery},
		TableId as ValueTableId, Value, ValueTable, SIZE_TIERS,
	},
	Key,
};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
	collections::VecDeque,
	sync::atomic::{AtomicU64, Ordering},
};

const MIN_INDEX_BITS: u8 = 16;
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

struct Tables {
	index: IndexTable,
	value: Vec<ValueTable>,
}

struct Reindex {
	queue: VecDeque<IndexTable>,
	progress: AtomicU64,
}

#[allow(clippy::large_enum_variant)]
pub enum Column {
	Hash(HashColumn),
	Tree(BTreeTable),
}

pub struct HashColumn {
	col: ColId,
	tables: RwLock<Tables>,
	reindex: RwLock<Reindex>,
	path: std::path::PathBuf,
	preimage: bool,
	uniform_keys: bool,
	collect_stats: bool,
	ref_counted: bool,
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

pub struct IterState {
	pub chunk_index: u64,
	pub key: Key,
	pub rc: u32,
	pub value: Vec<u8>,
}

enum IterStateOrCorrupted {
	Item(IterState),
	Corrupted(crate::index::Entry, Option<Error>),
}

#[inline]
pub fn hash_key(key: &[u8], salt: &Salt, uniform: bool, db_version: u32) -> Key {
	let mut k = Key::default();
	if uniform {
		if db_version <= 5 {
			k.copy_from_slice(&key[0..32]);
		} else {
			// For keys that are hashes already we do a simple XOR with salt.
			let key = &key[0..32];
			for i in 0..32 {
				k[i] = key[i] ^ salt[i];
			}
		}
	} else {
		k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, salt, key).as_bytes());
	}
	k
}

pub struct ReindexBatch {
	pub drop_index: Option<IndexTableId>,
	pub batch: Vec<(Key, Address)>,
}

impl HashColumn {
	pub fn get(&self, key: &Key, log: &impl LogQuery) -> Result<Option<Value>> {
		let tables = self.tables.read();
		let values = self.as_ref(&tables.value);
		if let Some((tier, value)) = self.get_in_index(key, &tables.index, values, log)? {
			if self.collect_stats {
				self.stats.query_hit(tier);
			}
			return Ok(Some(value))
		}
		for r in &self.reindex.read().queue {
			if let Some((tier, value)) = self.get_in_index(key, r, values, log)? {
				if self.collect_stats {
					self.stats.query_hit(tier);
				}
				return Ok(Some(value))
			}
		}
		if self.collect_stats {
			self.stats.query_miss();
		}
		Ok(None)
	}

	pub fn get_size(&self, key: &Key, log: &RwLock<LogOverlays>) -> Result<Option<u32>> {
		self.get(key, log).map(|v| v.map(|v| v.len() as u32))
	}

	fn get_in_index(
		&self,
		key: &Key,
		index: &IndexTable,
		tables: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<(u8, Value)>> {
		let (mut entry, mut sub_index) = index.get(key, 0, log);
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
					let (next_entry, next_index) = index.get(key, sub_index + 1, log);
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
	) -> Result<Option<(u8, Value)>> {
		let size_tier = address.size_tier() as usize;
		if let Some((value, compressed, _rc)) =
			tables.tables[size_tier].query(&mut key, address.offset(), log)?
		{
			let value = if compressed { tables.compression.decompress(&value)? } else { value };
			return Ok(Some((size_tier as u8, value)))
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
		let target_tier = match target_tier {
			Some(tier) => tier as usize,
			None => {
				log::trace!(target: "parity-db", "Using blob {}", key);
				tables.len() - 1
			},
		};

		(result, target_tier)
	}

	pub fn open(col: ColId, options: &Options, metadata: &Metadata) -> Result<Column> {
		let path = &options.path;
		let arc_path = std::sync::Arc::new(path.clone());
		let column_options = &metadata.columns[col as usize];
		let db_version = metadata.version;
		let value = (0..SIZE_TIERS)
			.map(|i| Self::open_table(arc_path.clone(), col, i as u8, column_options, db_version))
			.collect::<Result<_>>()?;

		if column_options.btree_index {
			Ok(Column::Tree(BTreeTable::open(col, value, metadata)?))
		} else {
			Ok(Column::Hash(HashColumn::open(col, value, options, metadata)?))
		}
	}

	fn open_table(
		path: std::sync::Arc<std::path::PathBuf>,
		col: ColId,
		tier: u8,
		options: &ColumnOptions,
		db_version: u32,
	) -> Result<ValueTable> {
		let id = ValueTableId::new(col, tier);
		let entry_size = SIZES.get(tier as usize).cloned();
		ValueTable::open(path, id, entry_size, options, db_version)
	}
}

impl HashColumn {
	fn open(
		col: ColId,
		value: Vec<ValueTable>,
		options: &Options,
		metadata: &Metadata,
	) -> Result<HashColumn> {
		let (index, reindexing, stats) = Self::open_index(&options.path, col)?;
		let collect_stats = options.stats;
		let path = &options.path;
		let options = &metadata.columns[col as usize];
		let db_version = metadata.version;
		Ok(HashColumn {
			col,
			tables: RwLock::new(Tables { index, value }),
			reindex: RwLock::new(Reindex { queue: reindexing, progress: AtomicU64::new(0) }),
			path: path.into(),
			preimage: options.preimage,
			uniform_keys: options.uniform,
			ref_counted: options.ref_counted,
			collect_stats,
			salt: metadata.salt,
			stats,
			compression: Compress::new(options.compression, options.compression_threshold),
			db_version,
		})
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
		Ok(())
	}

	fn open_index(
		path: &std::path::Path,
		col: ColId,
	) -> Result<(IndexTable, VecDeque<IndexTable>, ColumnStats)> {
		let mut reindexing = VecDeque::new();
		let mut top = None;
		let mut stats = ColumnStats::empty();
		for bits in (MIN_INDEX_BITS..65).rev() {
			let id = IndexTableId::new(col, bits);
			if let Some(table) = IndexTable::open_existing(path, id)? {
				if top.is_none() {
					stats = table.load_stats();
					top = Some(table);
				} else {
					reindexing.push_front(table);
				}
			}
		}
		let table = match top {
			Some(table) => table,
			None => IndexTable::create_new(path, IndexTableId::new(col, MIN_INDEX_BITS)),
		};
		Ok((table, reindexing, stats))
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
		reindex.queue.push_back(old_table);
		(
			parking_lot::RwLockWriteGuard::downgrade_to_upgradable(tables),
			parking_lot::RwLockWriteGuard::downgrade_to_upgradable(reindex),
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

	fn write_reindex_plan_locked<'a, 'b>(
		&self,
		tables: RwLockUpgradableReadGuard<'a, Tables>,
		reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		key: &Key,
		address: Address,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		if Self::search_index(key, &tables.index, &*tables, log)?.is_some() {
			return Ok(PlanOutcome::Skipped)
		}
		match tables.index.write_insert_plan(key, address, None, log)? {
			PlanOutcome::NeedReindex => {
				log::debug!(target: "parity-db", "{}: Index chunk full {} when reindexing", tables.index.id, hex(key));
				let (tables, reindex) = Self::trigger_reindex(tables, reindex, self.path.as_path());
				self.write_reindex_plan_locked(tables, reindex, key, address, log)?;
				Ok(PlanOutcome::NeedReindex)
			},
			_ => Ok(PlanOutcome::Written),
		}
	}

	fn search_index<'a>(
		key: &Key,
		index: &'a IndexTable,
		tables: &'a Tables,
		log: &LogWriter,
	) -> Result<Option<(&'a IndexTable, usize, Address)>> {
		let (mut existing_entry, mut sub_index) = index.get(key, 0, log);
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

			let (next_entry, next_index) = index.get(key, sub_index + 1, log);
			existing_entry = next_entry;
			sub_index = next_index;
		}
		Ok(None)
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
		for index in &reindex.queue {
			if let Some(r) = Self::search_index(key, index, tables, log)? {
				return Ok(Some(r))
			}
		}
		Ok(None)
	}

	pub fn write_plan(
		&self,
		key: &Key,
		value: Option<&[u8]>,
		log: &mut LogWriter,
	) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let existing = Self::search_all_indexes(key, &*tables, &*reindex, log)?;
		if let Some((table, sub_index, existing_address)) = existing {
			self.write_plan_existing(&*tables, key, value, log, table, sub_index, existing_address)
		} else if let Some(value) = value {
			let (r, _, _) = self.write_plan_new(tables, reindex, key, value, log)?;
			Ok(r)
		} else {
			log::trace!(target: "parity-db", "{}: Deleting missing key {}", tables.index.id, hex(key));
			if self.collect_stats {
				self.stats.remove_miss();
			}
			Ok(PlanOutcome::Skipped)
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn write_plan_existing(
		&self,
		tables: &Tables,
		key: &Key,
		value: Option<&[u8]>,
		log: &mut LogWriter,
		index: &IndexTable,
		sub_index: usize,
		existing_address: Address,
	) -> Result<PlanOutcome> {
		let stats = if self.collect_stats { Some(&self.stats) } else { None };

		let table_key = TableKey::Partial(*key);
		match Column::write_existing_value_plan(
			&table_key,
			self.as_ref(&tables.value),
			existing_address,
			value,
			log,
			stats,
		)? {
			(Some(outcome), _) => return Ok(outcome),
			(None, Some(value_address)) =>
				if value.is_some() {
					// If it was found in an older index we just insert a new entry. Reindex won't
					// overwrite it.
					let sub_index =
						if index.id == tables.index.id { Some(sub_index) } else { None };
					return tables.index.write_insert_plan(key, value_address, sub_index, log)
				} else {
					log::trace!(target: "parity-db", "{}: Removing from index {}", tables.index.id, hex(key));
					index.write_remove_plan(key, sub_index, log)?;
				},
			_ => unreachable!(),
		}
		Ok(PlanOutcome::Skipped)
	}

	fn write_plan_new<'a, 'b>(
		&self,
		tables: RwLockUpgradableReadGuard<'a, Tables>,
		reindex: RwLockUpgradableReadGuard<'b, Reindex>,
		key: &Key,
		value: &[u8],
		log: &mut LogWriter,
	) -> Result<(
		PlanOutcome,
		RwLockUpgradableReadGuard<'a, Tables>,
		RwLockUpgradableReadGuard<'b, Reindex>,
	)> {
		let stats = self.collect_stats.then(|| &self.stats);
		let table_key = TableKey::Partial(*key);
		let address = Column::write_new_value_plan(
			&table_key,
			self.as_ref(&tables.value),
			value,
			log,
			stats,
		)?;
		match tables.index.write_insert_plan(key, address, None, log)? {
			PlanOutcome::NeedReindex => {
				log::debug!(target: "parity-db", "{}: Index chunk full {}", tables.index.id, hex(key));
				let (tables, reindex) = Self::trigger_reindex(tables, reindex, self.path.as_path());
				let (_, t, r) = self.write_plan_new(tables, reindex, key, value, log)?;
				Ok((PlanOutcome::NeedReindex, t, r))
			},
			_ => Ok((PlanOutcome::Written, tables, reindex)),
		}
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.enact_plan(record.index, log)?;
				} else if let Some(table) = reindex.queue.iter().find(|r| r.id == record.table) {
					table.enact_plan(record.index, log)?;
				} else {
					// This may happen when removal is planed for an old index when reindexing.
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
				} else if let Some(table) = reindex.queue.iter().find(|r| r.id == record.table) {
					table.validate_plan(record.index, log)?;
				} else {
					// Re-launch previously started reindex
					// TODO: add explicit log records for reindexing events.
					log::warn!(
						target: "parity-db",
						"Missing table {}, starting reindex",
						record.table,
					);
					let _lock = Self::trigger_reindex(tables, reindex, self.path.as_path());
					return self.validate_plan(LogAction::InsertIndex(record), log)
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].validate_plan(record.index, log)?;
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

	pub fn write_stats(&self, writer: &mut impl std::io::Write) {
		let tables = self.tables.read();
		tables.index.write_stats(&self.stats);
		self.stats.write_summary(writer, tables.index.id.col());
	}

	pub fn clear_stats(&self) {
		let tables = self.tables.read();
		let empty_stats = ColumnStats::empty();
		tables.index.write_stats(&empty_stats);
	}

	pub fn iter_while(&self, log: &Log, mut f: impl FnMut(IterState) -> bool) -> Result<()> {
		let action = |state| match state {
			IterStateOrCorrupted::Item(item) => Ok(f(item)),
			IterStateOrCorrupted::Corrupted(..) =>
				Err(Error::Corruption("Missing indexed value".into())),
		};
		self.iter_while_inner(log, action, 0, true)
	}

	fn iter_while_inner(
		&self,
		log: &Log,
		mut f: impl FnMut(IterStateOrCorrupted) -> Result<bool>,
		start_chunk: u64,
		skip_preimage_indexes: bool,
	) -> Result<()> {
		let tables = self.tables.read();
		let source = &tables.index;

		if skip_preimage_indexes && self.preimage {
			// It is much faster to iterate over the value table than index.
			// We have to assume hashing scheme however.
			for table in &tables.value[..tables.value.len() - 1] {
				log::debug!( target: "parity-db", "{}: Iterating table {}", source.id, table.id);
				table.iter_while(&*log.overlays(), |index, rc, value, compressed| {
					let value = if compressed {
						if let Ok(value) = self.compression.decompress(&value) {
							value
						} else {
							return false
						}
					} else {
						value
					};
					let key = blake2_rfc::blake2b::blake2b(32, &[], &value);
					let key = self.hash_key(key.as_bytes());
					let state = IterStateOrCorrupted::Item(IterState {
						chunk_index: index,
						key,
						rc,
						value,
					});
					f(state).unwrap_or(false)
				})?;
				log::debug!( target: "parity-db", "{}: Done Iterating table {}", source.id, table.id);
			}
		}

		for c in start_chunk..source.id.total_chunks() {
			let entries = source.entries(c, &*log.overlays());
			for entry in entries.iter() {
				if entry.is_empty() {
					continue
				}
				let (size_tier, offset) = if self.db_version >= 4 {
					let address = entry.address(source.id.index_bits());
					(address.size_tier(), address.offset())
				} else {
					let addr_bits = source.id.index_bits() + 10;
					let address = Address::from_u64(entry.as_u64() & ((1u64 << addr_bits) - 1));
					let size_tier = (address.as_u64() & 0x0f) as u8;
					let offset = address.as_u64() >> 4;
					(size_tier, offset)
				};

				if skip_preimage_indexes &&
					self.preimage && size_tier as usize != tables.value.len() - 1
				{
					continue
				}
				let value =
					tables.value[size_tier as usize].get_with_meta(offset, &*log.overlays());
				let (value, rc, pk, compressed) = match value {
					Ok(Some(v)) => v,
					Ok(None) => {
						f(IterStateOrCorrupted::Corrupted(*entry, None))?;
						continue
					},
					Err(e) => {
						f(IterStateOrCorrupted::Corrupted(*entry, Some(e)))?;
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
				let state =
					IterStateOrCorrupted::Item(IterState { chunk_index: c, key, rc, value });
				if !f(state)? {
					return Ok(())
				}
			}
		}
		Ok(())
	}

	fn dump(&self, log: &Log, check_param: &crate::CheckOptions, col: ColId) -> Result<()> {
		let start_chunk = check_param.from.unwrap_or(0);
		let end_chunk = check_param.bound;

		let step = 1000;
		let start_time = std::time::Instant::now();
		log::info!(target: "parity-db", "Starting full index iteration at {:?}", start_time);
		log::info!(target: "parity-db", "for {} chunks of column {}", self.tables.read().index.id.total_chunks(), col);
		self.iter_while_inner(
			log,
			|state| match state {
				IterStateOrCorrupted::Item(IterState { chunk_index, key, rc, value }) => {
					if Some(chunk_index) == end_chunk {
						return Ok(false)
					}
					if chunk_index % step == 0 {
						log::info!(target: "parity-db", "Chunk iteration at {}", chunk_index);
					}

					match check_param.display {
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
				IterStateOrCorrupted::Corrupted(entry, e) => {
					log::info!("Corrupted value for index entry: {}:\n\t{:?}", entry.as_u64(), e);
					Ok(true)
				},
			},
			start_chunk,
			false,
		)?;

		log::info!(target: "parity-db", "Ended full index check, elapsed {:?}", start_time.elapsed());
		Ok(())
	}

	pub fn reindex(&self, log: &Log) -> Result<ReindexBatch> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		let mut plan = Vec::new();
		let mut drop_index = None;
		if let Some(source) = reindex.queue.front() {
			let progress = reindex.progress.load(Ordering::Relaxed);
			if progress != source.id.total_chunks() {
				let mut source_index = progress;
				if source_index % 500 == 0 {
					log::debug!(target: "parity-db", "{}: Reindexing at {}/{}", tables.index.id, source_index, source.id.total_chunks());
				}
				log::debug!(target: "parity-db", "{}: Continue reindex at {}/{}", tables.index.id, source_index, source.id.total_chunks());
				while source_index < source.id.total_chunks() && plan.len() < MAX_REINDEX_BATCH {
					log::trace!(target: "parity-db", "{}: Reindexing {}", source.id, source_index);
					let entries = source.entries(source_index, &*log.overlays());
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
		}
		Ok(ReindexBatch { drop_index, batch: plan })
	}

	pub fn drop_index(&self, id: IndexTableId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		let mut reindex = self.reindex.write();
		if reindex.queue.front_mut().map_or(false, |index| index.id == id) {
			let table = reindex.queue.pop_front();
			reindex.progress.store(0, Ordering::Relaxed);
			table.unwrap().drop_file()?;
		} else {
			log::warn!(target: "parity-db", "Dropping invalid index {}", id);
			return Ok(())
		}
		log::debug!(target: "parity-db", "Dropped {}", id);
		Ok(())
	}
}

impl Column {
	pub fn write_existing_value_plan(
		key: &TableKey,
		tables: TablesRef,
		address: Address,
		value: Option<&[u8]>,
		log: &mut LogWriter,
		stats: Option<&ColumnStats>,
	) -> Result<(Option<PlanOutcome>, Option<Address>)> {
		let tier = address.size_tier() as usize;
		if let Some(val) = value.as_ref() {
			if tables.ref_counted {
				log::trace!(target: "parity-db", "{}: Increment ref {}", tables.col, key);
				tables.tables[tier].write_inc_ref(address.offset(), log)?;
				return Ok((Some(PlanOutcome::Written), None))
			}
			if tables.preimage {
				// Replace is not supported
				return Ok((Some(PlanOutcome::Skipped), None))
			}

			let (cval, target_tier) = Column::compress(tables.compression, key, val, tables.tables);
			let (cval, compressed) =
				cval.as_ref().map(|cval| (cval.as_slice(), true)).unwrap_or((val, false));

			if let Some(stats) = stats {
				let (cur_size, compressed) =
					tables.tables[tier].size(key, address.offset(), log)?.unwrap_or((0, false));
				if compressed {
					// This is very costly.
					let compressed = tables.tables[tier]
						.get(key, address.offset(), log)?
						.expect("Same query as size")
						.0;
					let uncompressed = tables.compression.decompress(compressed.as_slice())?;

					stats.replace_val(
						cur_size,
						uncompressed.len() as u32,
						val.len() as u32,
						cval.len() as u32,
					);
				} else {
					stats.replace_val(cur_size, cur_size, val.len() as u32, cval.len() as u32);
				}
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
		} else {
			// Deletion
			let cur_size = if stats.is_some() {
				let (cur_size, compressed) =
					tables.tables[tier].size(key, address.offset(), log)?.unwrap_or((0, false));
				Some(if compressed {
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
			} else {
				None
			};
			let remove = if tables.ref_counted {
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
				Ok((None, Some(address)))
			} else {
				Ok((Some(PlanOutcome::Written), None))
			}
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

	pub fn write_stats(&self, writer: &mut impl std::io::Write) {
		match self {
			Column::Hash(column) => column.write_stats(writer),
			Column::Tree(_column) => (),
		}
	}

	pub fn clear_stats(&self) {
		match self {
			Column::Hash(column) => column.clear_stats(),
			Column::Tree(_column) => (),
		}
	}

	pub fn dump(&self, log: &Log, check_param: &crate::CheckOptions, col: ColId) -> Result<()> {
		match self {
			Column::Hash(column) => column.dump(log, check_param, col),
			Column::Tree(_column) => Ok(()),
		}
	}
}
