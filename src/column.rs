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
	bucket::{BucketId, Bucket, Key, Value, PlanOutcome, RebalanceProgress},
	log::{Log, LogReader, LogWriter},
	display::hex,
};

pub type ColId = u8;

struct Shard {
	bucket: Bucket,
	rebalancing: VecDeque<Bucket>,
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
			if let Some(v) = s.bucket.get(key, log) {
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
		for bits in (10 .. 65).rev() {
			let id = BucketId::new(col, entry_size, bits);
			if let Some(bucket) = Bucket::open_existing(path, id)? {
				if top.is_none() {
					top = Some(bucket);
				} else {
					rebalancing.push_front(bucket);
				}
			}
		}
		let bucket = match top {
			Some(bucket) => bucket,
			None => Bucket::create_new(path, BucketId::new(col, entry_size,  10))?,
		};
		Ok(Shard {
			bucket,
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
					.position(|s|value.len() <= s.bucket.id.value_size() as usize);
				match target_shard {
					Some(target_shard) => {
						for i in 0 .. self.shards.len() {
							if i == target_shard {
								let s = &mut self.shards[i];
								match s.bucket.write_plan(key, Some(&value), log, true)? {
									PlanOutcome::NeedRebalance => {
										log::info!(
											target: "parity-db",
											"Started rebalance {} at {}/{} full",
											s.bucket.id,
											s.bucket.entries(),
											s.bucket.id.total_entries(),
										);
										// Start rebalance
										let new_bucket_id = BucketId::new(
											s.bucket.id.col(),
											s.bucket.id.entry_size(),
											s.bucket.id.index_bits() + 1
										);
										let new_bucket = Bucket::create_new(self.path.as_path(), new_bucket_id)?;
										let old_bucket = std::mem::replace(&mut s.bucket, new_bucket);
										s.rebalancing.push_back(old_bucket);
										s.bucket.write_plan(key, Some(&value), log, true)?;
									}
									_ => {
									}
								}
							} else {
								match self.shards[i].bucket.write_plan(key, None, log, true)? {
									PlanOutcome::Written => {
										log::debug!(
											target: "parity-db",
											"Replaced to a different shard {}->{}: {}",
											self.shards[i].bucket.id,
											self.shards[target_shard].bucket.id,
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
					match s.bucket.write_plan(key, None, log, true)? {
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

	pub fn enact_plan(&mut self, id: BucketId, index: u64, log: &mut LogReader) -> Result<()> {
		// TODO: handle the case when bucket file does not exist
		let shard = self.shards.iter_mut().find(|s| s.bucket.id.entry_size() == id.entry_size());
		let shard = match shard {
			Some(s) => s,
			None => {
				log::warn!(
					target: "parity-db",
					"Missing bucket {}",
					id,
				);
				return Err(Error::Corruption("Missing bucket".into()));
			}
		};
		if shard.bucket.id == id {
			shard.bucket.enact_plan(index, log)?;
		} else {
			if let Some(bucket) = shard.rebalancing.iter_mut().find(|r|r.id == id) {
				bucket.enact_plan(index, log)?;
			}
			else {
				log::warn!(
					target: "parity-db",
					"Missing bucket {}",
					id,
				);
				return Err(Error::Corruption("Missing bucket".into()));
			}
		}
		Ok(())
	}

	pub fn rebalance(&mut self, log: &mut Log) -> Result<RebalanceProgress> {
		for s in self.shards.iter_mut() {
			if let Some(b) = s.rebalancing.front_mut() {
				if s.rebalance_progress != b.id.total_entries() {
					let mut log = log.begin_record()?;
					log::trace!(target: "parity-db", "{}: Start rebalance record {}", s.bucket.id, log.record_id());
					s.rebalance_progress = s.bucket.rebalance_from(&b, s.rebalance_progress, &mut log)?;
					if s.rebalance_progress == b.id.total_entries() {
						log::info!(target: "parity-db", "Completed rebalance {}", s.bucket.id);
						log.drop_bucket(b.id)?;
					}
					log::trace!(target: "parity-db", "{}: End rebalance record {}", s.bucket.id, log.record_id());
					log.end_record()?;
					return Ok(RebalanceProgress::InProgress((s.rebalance_progress, b.id.total_entries())))
				}
			}
		}
		Ok(RebalanceProgress::Inactive)
	}

	pub fn drop_bucket(&mut self, id: BucketId) -> Result<()> {
		log::debug!(target: "parity-db", "Dropping {}", id);
		for s in self.shards.iter_mut() {
			if s.rebalancing.front_mut().map_or(false, |b| b.id == id) {
				let bucket = s.rebalancing.pop_front();
				s.rebalance_progress = 0;
				bucket.unwrap().drop(self.path.as_path())?;
			}
		}
		Ok(())
	}
}

