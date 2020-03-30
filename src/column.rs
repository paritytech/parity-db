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

use std::collections::{HashMap, VecDeque};
use crate::{
	error::{Error, Result},
	table::{TableId, Table, Key, Value, PlanOutcome, RebalanceProgress},
	log::{Log, LogReader, LogWriter},
	display::hex,
};

pub type ColId = u8;

struct Shard {
	table: Table,
	rebalancing: VecDeque<Table>,
	rebalance_progress: u64,
}

pub struct Column {
	// Ordered by value size.
	shards: Vec<Shard>,
	path: std::path::PathBuf,
	// TODO: make these private
	pub blobs: HashMap<Key, Value>,
	pub histogram: std::collections::BTreeMap<u64, u64>,
}

impl Column {
	pub fn get(&self, key: &Key, log: &Log) -> Option<Value> {
		for s in &self.shards {
			if let Some(v) = s.table.get(key, log) {
				return Some(v);
			}
			for r in &s.rebalancing {
				if let Some(v) = r.get(key, log) {
					return Some(v);
				}
			}
		}
		self.blobs.get(key).cloned()
	}

	pub fn open(index: ColId, path: &std::path::Path) -> Result<Column> {
		Ok(Column {
			shards: vec![
				Self::open_shard(path, index, 128)?,
				Self::open_shard(path, index, 256)?,
				Self::open_shard(path, index, 512)?,
				Self::open_shard(path, index, 1024)?,
				Self::open_shard(path, index, 4096)?,
			],
			blobs: HashMap::new(),
			path: path.into(),
			histogram: Default::default(),
		})
	}

	fn open_shard(path: &std::path::Path, col: ColId, entry_size: u16) -> Result<Shard> {
		let mut rebalancing = VecDeque::new();
		let mut top = None;
		for bits in (12 .. 65).rev() {
			let id = TableId::new(col, entry_size, bits);
			if let Some(table) = Table::open_existing(path, id)? {
				if top.is_none() {
					top = Some(table);
				} else {
					rebalancing.push_front(table);
				}
			}
		}
		let table = match top {
			Some(table) => table,
			None => Table::create_new(path, TableId::new(col, entry_size,  12))?,
		};
		Ok(Shard {
			table,
			rebalancing,
			rebalance_progress: 0,
		})
	}

	pub fn write_plan(&mut self, key: &Key, value: Option<Value>, log: &mut LogWriter) -> Result<()> {
		match value {
			Some(value) => {
				*self.histogram.entry(value.len() as u64).or_default() += 1;
				// TODO: delete from other shards?
				let target_shard = self.shards.iter()
					.position(|s|value.len() <= s.table.id.value_size() as usize);
				match target_shard {
					Some(target_shard) => {
						for i in 0 .. self.shards.len() {
							if i == target_shard {
								let s = &mut self.shards[i];
								match s.table.write_plan(key, Some(&value), log, true)? {
									PlanOutcome::NeedRebalance => {
										log::info!(
											target: "parity-db",
											"Started rebalance {} at {}/{} full",
											s.table.id,
											s.table.entries(),
											s.table.id.total_entries(),
										);
										// Start rebalance
										let new_table_id = TableId::new(
											s.table.id.col(),
											s.table.id.entry_size(),
											s.table.id.index_bits() + 1
										);
										let new_table = Table::create_new(self.path.as_path(), new_table_id)?;
										let old_table = std::mem::replace(&mut s.table, new_table);
										s.rebalancing.push_back(old_table);
										s.table.write_plan(key, Some(&value), log, true)?;
									}
									_ => {
									}
								}
							} else {
								match self.shards[i].table.write_plan(key, None, log, true)? {
									PlanOutcome::Written => {
										log::debug!(
											target: "parity-db",
											"Replaced to a different shard {}->{}: {}",
											self.shards[i].table.id,
											self.shards[target_shard].table.id,
											hex(key),
										);
									}
									_ => {},
								}
							}
						}
					}
					None => {
						log::trace!(
							target: "parity-db",
							"Inserted blob {} ({} bytes)",
							hex(key),
							value.len(),
						);
						self.blobs.insert(*key, value);
					}
				}
			},
			None => {
				log::trace!(
					target: "parity-db",
					"Removed blob {}",
					hex(key),
				);
				if self.blobs.remove(key).is_some() {
					return Ok(());
				}
				// Delete from all shards
				for s in self.shards.iter_mut() {
					match s.table.write_plan(key, None, log, true)? {
						PlanOutcome::Written => {
							break;
						}
						PlanOutcome::Skipped {} | PlanOutcome::NeedRebalance => {},
					}
				}
			}
		}
		Ok(())
	}

	pub fn enact_plan(&mut self, id: TableId, index: u64, log: &mut LogReader) -> Result<()> {
		// TODO: handle the case when table file does not exist
		let shard = self.shards.iter_mut().find(|s| s.table.id.entry_size() == id.entry_size());
		let shard = match shard {
			Some(s) => s,
			None => {
				log::warn!(
					target: "parity-db",
					"Missing table {}",
					id,
				);
				return Err(Error::Corruption("Missing table".into()));
			}
		};
		if shard.table.id == id {
			shard.table.enact_plan(index, log)?;
		} else {
			if let Some(table) = shard.rebalancing.iter_mut().find(|r|r.id == id) {
				table.enact_plan(index, log)?;
			}
			else {
				log::warn!(
					target: "parity-db",
					"Missing table {}",
					id,
				);
				return Err(Error::Corruption("Missing table".into()));
			}
		}
		Ok(())
	}

	pub fn rebalance(&mut self, log: &mut Log) -> Result<RebalanceProgress> {
		for s in self.shards.iter_mut() {
			if let Some(b) = s.rebalancing.front_mut() {
				if s.rebalance_progress != b.id.total_entries() {
					let mut log = log.begin_record()?;
					log::trace!(target: "parity-db", "{}: Start rebalance record {}", s.table.id, log.record_id());
					s.rebalance_progress = s.table.rebalance_from(&b, s.rebalance_progress, &mut log)?;
					if s.rebalance_progress == b.id.total_entries() {
						log::info!(target: "parity-db", "Completed rebalance {}", s.table.id);
						log.drop_table(b.id)?;
					}
					log::trace!(target: "parity-db", "{}: End rebalance record {}", s.table.id, log.record_id());
					log.end_record()?;
					return Ok(RebalanceProgress::InProgress((s.rebalance_progress, b.id.total_entries())))
				}
			}
		}
		Ok(RebalanceProgress::Inactive)
	}

	pub fn drop_table(&mut self, id: TableId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		for s in self.shards.iter_mut() {
			if s.rebalancing.front_mut().map_or(false, |b| b.id == id) {
				let table = s.rebalancing.pop_front();
				s.rebalance_progress = 0;
				table.unwrap().drop(self.path.as_path())?;
			}
		}
		Ok(())
	}
}

