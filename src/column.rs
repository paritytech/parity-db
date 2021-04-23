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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use crate::{
	error::{Error, Result},
	table::{TableId as ValueTableId, ValueTable, Key, Value},
	log::{Log, LogOverlays, LogReader, LogWriter, LogAction},
	display::hex,
	index::{IndexTable, TableId as IndexTableId, PlanOutcome, Address},
	options::Options,
	stats::ColumnStats,
};

const START_BITS: u8 = 16;
const MAX_REBALANCE_BATCH: u32 = 1024;

pub type ColId = u8;
pub type Salt = [u8; 32];

struct Tables {
	index: IndexTable,
	value: [ValueTable; 16],
}

struct Reindex {
	queue: VecDeque<IndexTable>,
	progress: AtomicU64,
}

pub struct Column {
	tables: RwLock<Tables>,
	reindex: RwLock<Reindex>,
	path: std::path::PathBuf,
	preimage: bool,
	uniform_keys: bool,
	collect_stats: bool,
	ref_counted: bool,
	salt: Option<Salt>,
	stats: ColumnStats,
	compression: crate::compress::Compress,
}

impl Column {
	pub fn get(&self, key: &Key, log: &RwLock<LogOverlays>) -> Result<Option<Value>> {
		let tables = self.tables.read();
		if let Some((tier, value)) = self.get_in_index(key, &tables.index, &*tables, log)? {
			if self.collect_stats {
				self.stats.query_hit(tier);
			}
			return Ok(Some(value));
		}
		for r in &self.reindex.read().queue {
			if let Some((tier, value)) = self.get_in_index(key, &r, &*tables, log)? {
				if self.collect_stats {
					self.stats.query_hit(tier);
				}
				return Ok(Some(value));
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

	fn get_in_index(&self, key: &Key, index: &IndexTable, tables: &Tables, log: &RwLock<LogOverlays>) -> Result<Option<(u8, Value)>> {
		let (mut entry, mut sub_index) = index.get(key, 0, log);
		while !entry.is_empty() {
			let size_tier = entry.address(index.id.index_bits()).size_tier() as usize;
			match tables.value[size_tier].get(key, entry.address(index.id.index_bits()).offset(), log)? {
				Some(value) => {
					let value = self.decompress(&value);
					return Ok(Some((size_tier as u8, value)));
				}
				None =>  {
					let (next_entry, next_index) = index.get(key, sub_index + 1, log);
					entry = next_entry;
					sub_index = next_index;
				}
			}
		}
		Ok(None)
	}

	fn compress(&self, buf: &[u8]) -> Vec<u8> {
		self.compression.compress(buf)
	}

	fn decompress(&self, buf: &[u8]) -> Vec<u8> {
		self.compression.decompress(buf)
	}

	pub fn open(col: ColId, options: &Options, salt: Option<Salt>, create: bool) -> Result<Column> {
		let (index, reindexing, stats) = Self::open_index(&options.path, col)?;
		let collect_stats = options.stats;
		let path = &options.path;
		let options = &options.columns[col as usize];
		let tables = Tables {
			index,
			value: [
				Self::open_table(path, col, 0, Some(options.sizes[0]), create)?,
				Self::open_table(path, col, 1, Some(options.sizes[1]), create)?,
				Self::open_table(path, col, 2, Some(options.sizes[2]), create)?,
				Self::open_table(path, col, 3, Some(options.sizes[3]), create)?,
				Self::open_table(path, col, 4, Some(options.sizes[4]), create)?,
				Self::open_table(path, col, 5, Some(options.sizes[5]), create)?,
				Self::open_table(path, col, 6, Some(options.sizes[6]), create)?,
				Self::open_table(path, col, 7, Some(options.sizes[7]), create)?,
				Self::open_table(path, col, 8, Some(options.sizes[8]), create)?,
				Self::open_table(path, col, 9, Some(options.sizes[9]), create)?,
				Self::open_table(path, col, 10, Some(options.sizes[10]), create)?,
				Self::open_table(path, col, 11, Some(options.sizes[11]), create)?,
				Self::open_table(path, col, 12, Some(options.sizes[12]), create)?,
				Self::open_table(path, col, 13, Some(options.sizes[13]), create)?,
				Self::open_table(path, col, 14, Some(options.sizes[14]), create)?,
				Self::open_table(path, col, 15, None, create)?,
			],
		};

		Ok(Column {
			tables: RwLock::new(tables),
			reindex: RwLock::new(Reindex {
				queue: reindexing,
				progress: AtomicU64::new(0),
			}),
			path: path.into(),
			preimage: options.preimage,
			uniform_keys: options.uniform,
			ref_counted: options.ref_counted,
			collect_stats,
			salt,
			stats,
			compression: options.compression.into(),
		})
	}

	pub fn hash(&self, key: &[u8]) -> Key {
		let mut k = Key::default();
		if self.uniform_keys {
			k.copy_from_slice(&key[0..32]);
		} else if let Some(salt) = &self.salt {
			k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &salt[..], &key).as_bytes());
		} else {
			k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &[], &key).as_bytes());
		}
		k
	}

	fn open_index(path: &std::path::Path, col: ColId) -> Result<(IndexTable, VecDeque<IndexTable>, ColumnStats)> {
		let mut reindexing = VecDeque::new();
		let mut top = None;
		let mut stats = ColumnStats::empty();
		for bits in (START_BITS .. 65).rev() {
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
			None => IndexTable::create_new(path, IndexTableId::new(col, START_BITS)),
		};
		Ok((table, reindexing, stats))
	}

	fn open_table(path: &std::path::Path, col: ColId, tier: u8, entry_size: Option<u16>, create: bool) -> Result<ValueTable> {
		let id = ValueTableId::new(col, tier);
		ValueTable::open(path, id, entry_size, create)
	}

	fn trigger_reindex(
		tables: parking_lot::RwLockUpgradableReadGuard<Tables>,
		reindex: parking_lot::RwLockUpgradableReadGuard<Reindex>,
		path: &std::path::Path,
	) {
		let mut tables = parking_lot::RwLockUpgradableReadGuard::upgrade(tables);
		let mut reindex = parking_lot::RwLockUpgradableReadGuard::upgrade(reindex);
		log::info!(
			target: "parity-db",
			"Started reindex for {}",
			tables.index.id,
		);
		// Start reindex
		let new_index_id = IndexTableId::new(
			tables.index.id.col(),
			tables.index.id.index_bits() + 1
		);
		let new_table = IndexTable::create_new(path, new_index_id);
		let old_table = std::mem::replace(&mut tables.index, new_table);
		reindex.queue.push_back(old_table);
	}

	pub fn write_index_plan(&self, key: &Key, address: Address, log: &mut LogWriter) -> Result<PlanOutcome> {
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		if Self::search_index(key, &tables.index, &*tables, log)?.is_some() {
			return Ok(PlanOutcome::Skipped);
		}
		match tables.index.write_insert_plan(key, address, None, log)? {
			PlanOutcome::NeedReindex => {
				log::debug!(target: "parity-db", "{}: Index chunk full {}", tables.index.id, hex(key));
				Self::trigger_reindex(tables, reindex, self.path.as_path());
				self.write_index_plan(key, address, log)?;
				return Ok(PlanOutcome::NeedReindex);
			}
			_ => {
				return Ok(PlanOutcome::Written);
			}
		}
	}

	fn search_index<'a>(
		key: &Key,
		index: &'a IndexTable,
		tables: &'a Tables,
		log: &LogWriter
	) -> Result<Option<(&'a IndexTable, usize, u8, Address)>> {
		let (mut existing_entry, mut sub_index) = index.get(key, 0, log);
		while !existing_entry.is_empty() {
			let existing_address = existing_entry.address(index.id.index_bits());
			let existing_tier = existing_address.size_tier();
			if tables.value[existing_tier as usize].has_key_at(existing_address.offset(), &key, log)? {
				return Ok(Some((&index, sub_index, existing_tier, existing_address)));
			}

			let (next_entry, next_index) = index.get(key, sub_index + 1, log);
			existing_entry = next_entry;
			sub_index = next_index;
		};
		Ok(None)
	}

	fn search_all_indexes<'a>(
		key: &Key,
		tables: &'a Tables,
		reindex: &'a Reindex,
		log: &LogWriter
	) -> Result<Option<(&'a IndexTable, usize, u8, Address)>> {
			if let Some(r) = Self::search_index(key, &tables.index, tables, log)? {
				return Ok(Some(r));
			}
			// Check old indexes
			// TODO: don't search if index precedes reindex progress
			for index in &reindex.queue {
				if let Some(r) = Self::search_index(key, index, tables, log)? {
					return Ok(Some(r));
				}
			}
			Ok(None)
	}

	pub fn write_plan(&self, key: &Key, value: &Option<Value>, log: &mut LogWriter) -> Result<PlanOutcome> {
		//TODO: return sub-chunk position in index.get
		let tables = self.tables.upgradable_read();
		let reindex = self.reindex.upgradable_read();
		let existing = Self::search_all_indexes(key, &*tables, &*reindex, log)?;
		if let &Some(ref val) = value {
			let cval = self.compress(&val);
			let target_tier = tables.value.iter().position(|t| cval.len() <= t.value_size() as usize);
			let target_tier = match target_tier {
				Some(tier) => tier as usize,
				None => {
					log::trace!(target: "parity-db", "Inserted blob {}", hex(key));
					15
				}
			};

			if let Some((table, sub_index, existing_tier, existing_address)) = existing {
				let existing_tier = existing_tier as usize;
				if self.collect_stats {
					let cur_size = tables.value[existing_tier].size(&key, existing_address.offset(), log)?.unwrap_or(0);
					self.stats.replace_val(cur_size, cur_size, val.len() as u32, cval.len() as u32);
				}
				if self.ref_counted {
					log::trace!(target: "parity-db", "{}: Increment ref {}", tables.index.id, hex(key));
					tables.value[target_tier].write_inc_ref(existing_address.offset(), log)?;
					return Ok(PlanOutcome::Written);
				}
				if self.preimage {
					// Replace is not supported
					return Ok(PlanOutcome::Skipped);
				}
				if existing_tier == target_tier {
					log::trace!(target: "parity-db", "{}: Replacing {}", tables.index.id, hex(key));
					tables.value[target_tier].write_replace_plan(existing_address.offset(), key, &cval, log)?;
					return Ok(PlanOutcome::Written);
				} else {
					log::trace!(target: "parity-db", "{}: Replacing in a new table {}", tables.index.id, hex(key));
					tables.value[existing_tier].write_remove_plan(existing_address.offset(), log)?;
					let new_offset = tables.value[target_tier].write_insert_plan(key, &cval, log)?;
					let new_address = Address::new(new_offset, target_tier as u8);
					// If it was found in an older index we just insert a new entry. Reindex won't overwrite it.
					let sub_index = if table.id == tables.index.id { Some(sub_index) } else { None };
					return tables.index.write_insert_plan(key, new_address, sub_index, log);
				}
			} else {
				log::trace!(target: "parity-db", "{}: Inserting new index {}", tables.index.id, hex(key));
				let offset = tables.value[target_tier].write_insert_plan(key, &cval, log)?;
				let address = Address::new(offset, target_tier as u8);
				match tables.index.write_insert_plan(key, address, None, log)? {
					PlanOutcome::NeedReindex => {
						log::debug!(target: "parity-db", "{}: Index chunk full {}", tables.index.id, hex(key));
						Self::trigger_reindex(tables, reindex, self.path.as_path());
						self.write_plan(key, value, log)?;
						return Ok(PlanOutcome::NeedReindex);
					}
					_ => {
						if self.collect_stats {
							self.stats.insert_val(val.len() as u32, cval.len() as u32);
						}
						return Ok(PlanOutcome::Written);
					}
				}
			}
		} else {
			if let Some((table, sub_index, existing_tier, existing_address)) = existing {
				// Deletion
				let existing_tier = existing_tier as usize;
				let cur_size = if self.collect_stats {
					Some(tables.value[existing_tier].size(&key, existing_address.offset(), log)?.unwrap_or(0))
				} else {
					None
				};
				let remove = if self.ref_counted {
					let removed = !tables.value[existing_tier].write_dec_ref(existing_address.offset(), log)?;
					log::trace!(target: "parity-db", "{}: Dereference {}, deleted={}", table.id, hex(key), removed);
					removed
				} else {
					log::trace!(target: "parity-db", "{}: Deleting {}", table.id, hex(key));
					tables.value[existing_tier].write_remove_plan(existing_address.offset(), log)?;
					true
				};
				if remove {
					if let Some(cur_size) = cur_size {
						self.stats.remove_val(cur_size, cur_size);
					}
					table.write_remove_plan(key, sub_index, log)?;
				}
				return Ok(PlanOutcome::Written);
			}
			log::trace!(target: "parity-db", "{}: Deletion missed {}", tables.index.id, hex(key));
			if self.collect_stats {
				self.stats.remove_miss();
			}
		}
		Ok(PlanOutcome::Skipped)
	}

	pub fn enact_plan(&self, action: LogAction, log: &mut LogReader) -> Result<()> {
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		match action {
			LogAction::InsertIndex(record) => {
				if tables.index.id == record.table {
					tables.index.enact_plan(record.index, log)?;
				} else if let Some(table) = reindex.queue.iter().find(|r|r.id == record.table) {
					table.enact_plan(record.index, log)?;
				}
				else {
					log::warn!(
						target: "parity-db",
						"Missing table {}",
						record.table,
					);
					return Err(Error::Corruption("Missing table".into()));
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].enact_plan(record.index, log)?;
			}
			_ => panic!("Unexpected log action"),
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
				} else if let Some(table) = reindex.queue.iter().find(|r|r.id == record.table) {
					table.validate_plan(record.index, log)?;
				}
				else {
					// Re-launch previously started reindex
					// TODO: add explicit log records for reindexing events.
					log::warn!(
						target: "parity-db",
						"Missing table {}, starting reindex",
						record.table,
					);
					Self::trigger_reindex(tables, reindex, self.path.as_path());
					return self.validate_plan(LogAction::InsertIndex(record), log);
				}
			},
			LogAction::InsertValue(record) => {
				tables.value[record.table.size_tier() as usize].validate_plan(record.index, log)?;
			}
			_ => panic!("Unexpected log action"),
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

	pub fn reindex(&self, log: &Log) -> Result<(Option<IndexTableId>, Vec<(Key, Address)>)> {
		// TODO: handle overlay
		let tables = self.tables.read();
		let reindex = self.reindex.read();
		let mut plan = Vec::new();
		let mut drop_index = None;
		if let Some(source) = reindex.queue.front() {
			let progress = reindex.progress.load(Ordering::Relaxed);
			if progress != source.id.total_chunks() {
				let mut source_index = progress;
				let mut count = 0;
				if source_index % 50 == 0 {
					log::debug!(target: "parity-db", "{}: Reindexing at {}/{}", tables.index.id, source_index, source.id.total_chunks());
				}
				log::debug!(target: "parity-db", "{}: Continue reindex at {}/{}", tables.index.id, source_index, source.id.total_chunks());
				while source_index < source.id.total_chunks() && count < MAX_REBALANCE_BATCH {
					log::trace!(target: "parity-db", "{}: Reindexing {}", source.id, source_index);
					let entries = source.entries(source_index, &*log.overlays());
					for entry in entries.iter() {
						if entry.is_empty() {
							continue;
						}
						// Reconstruct as much of the original key as possible.
						let partial_key = entry.key_material(source.id.index_bits());
						let k = 64 - crate::index::Entry::address_bits(source.id.index_bits());
						let index_key = (source_index << 64 - source.id.index_bits()) |
							(partial_key << (64 - k - source.id.index_bits()));
						let mut key = Key::default();
						// restore 16 high bits
						&mut key[0..8].copy_from_slice(&index_key.to_be_bytes());
						log::trace!(target: "parity-db", "{}: Reinserting {}", source.id, hex(&key));
						plan.push((key, entry.address(source.id.index_bits())))
					}
					count += 1;
					source_index += 1;
				}
				log::trace!(target: "parity-db", "{}: End reindex batch {} ({})", tables.index.id, source_index, count);
				reindex.progress.store(source_index, Ordering::Relaxed);
				if source_index == source.id.total_chunks() {
					log::info!(target: "parity-db", "Completed reindex into {}", tables.index.id);
					drop_index = Some(source.id);
				}
			}
		}
		Ok((drop_index, plan))
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
			return Ok(());
		}
		log::debug!(target: "parity-db", "Dropped {}", id);
		Ok(())
	}

	// Migrate from current compression mode to another.
	// Double column size during processsing.
	// Do not support shutdown (old file are kept but
	// need to be restored manually).
	// Also require empty log and should not run when logger worker is active.
	pub(crate) fn migrate_column(
		&mut self,
		compression_target: crate::compress::CompressType,
		log: &Log,
	) -> Result<()> {
		let mut tables = self.tables.write();
		let reindex = self.reindex.write(); // keeping it locked until end.
		if !reindex.queue.is_empty() {
			return Err(Error::InvalidConfiguration("Db need ot be flush before runnig admin method.".into()));
		}
		let compression: crate::compress::Compress = compression_target.into();
		// store index as backup
		tables.index.backup_index(&self.path)?;

		// create value destination tables
		let nb_tables = tables.value.len();
		let mut dest_tables = Vec::with_capacity(nb_tables);
		for (i, table) in tables.value.iter().enumerate() {
			let size = if i == nb_tables - 1 {
				None
			} else {
				Some(table.entry_size)
			};
			dest_tables.push(ValueTable::open_extension(&self.path, table.id, size, true, "_dest")?);
		}

		log.clear_logs().unwrap(); // TODO this is only here because flush keep a log file at this point. + TODO with this implementation we can move this to db.
		let index_bits = tables.index.id.index_bits();
/*		let mut first = false;
		let mut first = &mut first;*/
		// process all value from index
		tables.index.for_all(None, None, |key, mut entry| {
			let size_tier = entry.address(index_bits).size_tier() as usize;
			let full_key = key.clone();
			std::mem::drop(key); // avoid using key
			let rc = 0u32;
			let mut pair = (full_key, rc);
			match tables.value[size_tier].get_from_index(&mut pair, entry.address(index_bits).offset(), &admin::NoopsLogQuery, false) {
				Ok(Some(value)) => {
					let full_key = pair.0;
					let rc = pair.1;

					let value = self.decompress(&value);
					let mut cval = compression.compress(value.as_slice());
					//let cval = compression.compress(value.as_slice());

					if self.collect_stats {
/*						if value.len() > 8_000 {
							println!("{} -> {}", value.len(), cval.len());
						}*/
						self.stats.insert_val(value.len() as u32, cval.len() as u32);
					}
/* TODO would need to store if compression did happen
 * into the size for instance.
 * */
					if cval.len() > value.len() {
						cval = value;
					}

					let target_tier = tables.value.iter().position(|t| cval.len() <= t.value_size() as usize);
					let target_tier = match target_tier {
						Some(tier) => tier as usize,
						None => 15,
					};

/*					if size_tier == 15 {
						println!("size_tier");
					}
					if cval.as_slice().len() == 1017646 {
						println!("{:?}", (target_tier, dest_tables[target_tier].value_size()));
					}*/
					// Not using log would be way faster, but using it avoid duplicating code.
					// TODO could also batch the log, but may be worth it do direct write
					// (awkward part is managing multipart file).
					// Especially since we skip log for index update.
					let index = dest_tables[target_tier].force_append_write(&full_key, cval.as_slice(), rc).unwrap(); // TODO result if closure.
					let address = Address::new(index, target_tier as u8);
/*			if !*first {
					let entry2 = crate::index::Entry::new(address, entry.key_material(index_bits), index_bits);
				println!("key {:?}", key);
				println!("key {:?}", full_key);
				println!("rc {:?}", rc);
				println!("entry {:x}", entry.0);
				println!("entry2 {:x}", entry2.0);
				println!("address {:x}", entry.address(index_bits).0);
				println!("address2 {:x}", entry2.address(index_bits).0);
				println!("index {}", index);
				println!("address {:x}", entry.address(index_bits).offset());
				*first = true;
			}
*/
	
					entry = crate::index::Entry::new(address, entry.key_material(index_bits), index_bits);
/*					let mut writer = log.begin_record();
					dest_tables[target_tier].write_insert_plan(&full_key, cval.as_slice(), &mut writer).unwrap();
					log.end_record(writer.drain()).unwrap();
					// Cycle through 2 log files
					let _ = log.read_next(false);
					log.flush_one().unwrap();
					let _ = log.read_next(false);
					log.flush_one().unwrap();
					let mut reader = log.read_next(false).unwrap().unwrap();
					loop {
						match reader.next().unwrap() {
							LogAction::BeginRecord(_) => {
								panic!("Unexpected log entry2");
							},
							LogAction::InsertIndex(_) => {
								panic!("Unexpected log entry22");
							},
							LogAction::BeginRecord(_) | LogAction::InsertIndex { .. } | LogAction::DropTable { .. } => {
								panic!("Unexpected log entry");
							},
							LogAction::EndRecord => {
								break;
							},
							LogAction::InsertValue(insertion) => {
								let address = Address::new(insertion.index, insertion.table.size_tier());
								entry = crate::index::Entry::new(address, entry.key_material(index_bits), index_bits);
								dest_tables[insertion.table.size_tier() as usize]
									.enact_plan(insertion.index, &mut reader).unwrap();
							},
						}
					}
*/
				},
				_ => {
					log::error!("Missing value for {:?}, removing index", key);
					entry = crate::index::Entry::empty();
				},
			}
			// Return new entry for index update.
			Some(entry)
		}, Some(500));

		if self.collect_stats {
			tables.index.write_stats(&self.stats);
		}

		// Update value tables replacing old
		for table in tables.value.iter_mut() {
			*table = dest_tables.remove(0);
			table.force_write_header()?;
			let mut from: std::path::PathBuf = self.path.clone();
			let mut file_name = table.id.file_name();
			file_name.push_str("_dest");
			from.push(file_name);
			let mut to: std::path::PathBuf = self.path.clone();
			to.push(table.id.file_name());
			// TODO might need to close file first...
			std::fs::rename(from, to)?;
		}

		// Revove index backups
		tables.index.remove_backup_index(&self.path)?;

		Ok(())
	}

	pub(crate) fn check_from_index(&mut self, check_param: &crate::db::check::CheckParam) -> Result<()> {
		// lock all, this is an admin method.
		let tables = self.tables.write();
		let reindex = self.reindex.write();
		if !reindex.queue.is_empty() {
			return Err(Error::InvalidConfiguration("Db need ot be flush before runnig admin method.".into()));
		}

		let index_bits = tables.index.id.index_bits();
		let end = check_param.bound
			.map(|len| check_param.from.clone().unwrap_or(0) + len);
		tables.index.for_all(check_param.from.clone(), end, |key, entry| {
			let mut result = None;
			let size_tier = entry.address(index_bits).size_tier() as usize;
			let full_key = key.clone();
			std::mem::drop(key); // avoid using key
			let rc = 0u32;
			let mut pair = (full_key, rc);
			match tables.value[size_tier].get_from_index(&mut pair, entry.address(index_bits).offset(), &admin::NoopsLogQuery, true) {
				Ok(Some(value)) => {
					let full_key = pair.0;
					let rc = pair.1;

					let value = self.decompress(&value);
					if check_param.display_content {
						println!("Index entry: {:x}", entry.0);
						println!("Index key: {}", hex(&full_key));
						println!("Rc: {}", rc);
						if let Some(t) = check_param.truncate_value_display.as_ref() {
							println!("Value: {}", hex(&value[..std::cmp::min(*t as usize, value.len())]));
							println!("Value len: {}", value.len());
						} else {
							println!("Value: {}", hex(&value));
						}
					}
				},
				Ok(None) => {
					println!("Missing value for index entry: {:x}", entry.0);

					if check_param.remove_on_corrupted {
						println!("Index will be removed.");

						result = Some(crate::index::Entry::empty());
					}
				},
				Err(Error::Corruption(e)) => {
					println!("Corrupted value for index entry: {:x}:\n\t{}", entry.0, e);
					if check_param.remove_on_corrupted {
						println!("Index will be removed.");

						result = Some(crate::index::Entry::empty());
					}
				},
				Err(e) => {
					println!("Error value for index entry: {:x}:\n\t{}", entry.0, e);
				},
			}
	
			result
		}, Some(500));
	
		Ok(())
	}

	// TODO
	pub(crate) fn salt(&self) -> Option<Salt> {
		self.salt.clone()
	}
}

/// Utility for admin operation.
pub(crate) mod admin {
	use super::*;

	pub struct NoopsLogQuery;

	impl crate::log::LogQuery for NoopsLogQuery {
		fn with_index<R, F: FnOnce(&crate::index::Chunk) -> R> (
			&self,
			_table: IndexTableId,
			_index: u64,
			_f: F,
		) -> Option<R> {
			None
		}

		fn value(&self, _table: ValueTableId, _index: u64, _dest: &mut[u8]) -> bool {
			false
		}
	}
}
