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

use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use parking_lot::{RwLock, Mutex, Condvar};
use crate::{
	table::{Key, Address},
	error::{Error, Result},
	column::{ColId, Column},
	log::{Log, LogAction},
	index::{PlanOutcome, TableId as IndexTableId},
};

const MAX_COMMIT_QUEUE_SIZE: usize = 200;
const MAX_LOG_QUEUE_SIZE: usize = 200;

pub type Value = Vec<u8>;

fn hash(key: &[u8]) -> Key {
	let mut k = Key::default();
	k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &[], key).as_bytes());
	//log::trace!(target: "parity-db", "HASH {} = {}", crate::display::hex(&key), crate::display::hex(&k));
	k
}

#[derive(Default)]
struct Commit{
	id: u64,
	changeset: Vec<(ColId, Key, Option<Value>)>,
}

#[derive(Default)]
struct CommitQueue{
	record_id: u64,
	commits: VecDeque<Commit>,
}

#[derive(Default)]
struct PendingRebalance {
	column: ColId,
	batch: Vec<(Key, Address)>,
	drop_index: Option<IndexTableId>,
}

struct DbInner {
	columns: Vec<Column>,
	_path: std::path::PathBuf,
	shutdown: std::sync::atomic::AtomicBool,
	rebalance_queue: Mutex<VecDeque<PendingRebalance>>,
	need_rebalance_cv: Condvar,
	rebalance_work: Mutex<()>,
	log: Log,
	commit_queue: Mutex<CommitQueue>,
	commit_queue_full_cv: Condvar,
	worker_cv: Condvar,
	work: Mutex<()>,
	commit_overlay: RwLock<HashMap<ColId, HashMap<Key, (u64, Option<Value>)>>>,
	log_cv: Condvar,
	log_queue: Mutex<usize>,
}

impl DbInner {
	pub fn open(path: &std::path::Path, num_columns: u8) -> Result<DbInner> {
		std::fs::create_dir_all(path)?;
		let mut columns = Vec::with_capacity(num_columns as usize);
		for c in 0 .. num_columns {
			columns.push(Column::open(c, path)?);
		}
		Ok(DbInner {
			columns,
			_path: path.into(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			rebalance_queue: Mutex::new(Default::default()),
			need_rebalance_cv: Condvar::new(),
			rebalance_work: Mutex::new(()),
			log: Log::open(path)?,
			commit_queue: Mutex::new(Default::default()),
			commit_queue_full_cv: Condvar::new(),
			worker_cv: Condvar::new(),
			work: Mutex::new(()),
			commit_overlay: RwLock::new(Default::default()),
			log_queue: Mutex::new(0),
			log_cv: Condvar::new(),
		})
	}

	fn signal_worker(&self) {
		self.worker_cv.notify_one();
	}

	fn signal_rebalance(&self) {
		self.need_rebalance_cv.notify_one();
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		let overlay = self.commit_overlay.read();
		let key = hash(key);
		if let Some(v) = overlay.get(&col).and_then(|o| o.get(&key).map(|(_, v)| v.clone())) {
			return Ok(v);
		}
		let log = self.log.overlays();
		self.columns[col as usize].get(&key, &log)
	}

	fn commit<I, K>(&self, tx: I) -> Result<()>
		where
			I: IntoIterator<Item=(ColId, K, Option<Value>)>,
			K: AsRef<[u8]>,
	{
		let commit = tx.into_iter().map(|(c, k, v)| (c, hash(k.as_ref()), v)).collect();

		{
			let mut queue = self.commit_queue.lock();
			if queue.commits.len() >= MAX_COMMIT_QUEUE_SIZE {
				self.commit_queue_full_cv.wait(&mut queue);
			}
			let mut overlay = self.commit_overlay.write();

			queue.record_id += 1;
			let record_id = queue.record_id + 1;
			let commit = Commit {
				id: record_id,
				changeset: commit,
			};

			for (c, k, v) in &commit.changeset {
				overlay.entry(*c).or_default().insert(*k, (record_id, v.clone()));
			}

			log::debug!(
				target: "parity-db",
				"Queued commit {}",
				commit.id,
			);
			queue.commits.push_back(commit);
			self.signal_worker();
		}
		Ok(())
	}

	fn process_commits(&self) -> Result<bool> {
		{
			let mut queue = self.log_queue.lock();
			if *queue >= MAX_LOG_QUEUE_SIZE {
				self.log_cv.wait(&mut queue);
			}
		}
		let commit = {
			let mut queue = self.commit_queue.lock();
			if queue.commits.len() == MAX_COMMIT_QUEUE_SIZE {
				self.commit_queue_full_cv.notify_one();
			}
			queue.commits.pop_front()
		};

		if let Some(commit) = commit {
			let mut rebalance = false;
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Processing commit {}, record {}",
				commit.id,
				writer.record_id(),
			);
			let mut ops: u64 = 0;
			for (c, key, value) in commit.changeset.iter() {
				match self.columns[*c as usize].write_plan(key, value, &mut writer)? {
					PlanOutcome::NeedRebalance => {
						rebalance = true;
					},
					_ => {},
				}
				ops += 1;
			}
			let record_id = writer.record_id();
			let l = writer.drain();
			self.log.end_record(l)?;

			{
				let mut overlay = self.commit_overlay.write();
				for (c, key, _) in commit.changeset.iter() {
					if let Some(overlay) = overlay.get_mut(c) {
						if let std::collections::hash_map::Entry::Occupied(e) = overlay.entry(*key) {
							if e.get().0 == commit.id {
								e.remove_entry();
							}
						}
					}
				}
			}

			if rebalance {
				self.signal_rebalance();
			}

			log::debug!(
				target: "parity-db",
				"Processed commit {}, {} ops",
				record_id,
				ops,
			);
			let mut queue = self.log_queue.lock();
			*queue += 1;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn process_rebalance(&self) -> Result<bool> {
		let rebalance =  {
			log::debug!(
				target: "parity-db",
				"Checking pending rebalance",
			);
			let mut queue = self.rebalance_queue.lock();
			queue.pop_front()
		};

		if let Some(rebalance) = rebalance {
			let mut next_rebalance = false;
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Creating rebalance record {}",
				writer.record_id(),
			);
			let column = &self.columns[rebalance.column as usize];
			for (key, address) in rebalance.batch.into_iter() {
				match column.write_index_plan(&key, address, &mut writer)? {
					PlanOutcome::NeedRebalance => {
						next_rebalance = true
					},
					_ => {},
				}
			}
			if let Some(table) = rebalance.drop_index {
				writer.drop_table(table);
			}
			let record_id = writer.record_id();
			let l = writer.drain();
			self.log.end_record(l)?;

			log::debug!(
				target: "parity-db",
				"Created rebalance record {}",
				record_id,
			);
			if next_rebalance {
				self.signal_rebalance();
			}
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn enact_logs(&self) -> Result<bool> {
		let cleared = {
			let reader = self.log.flush_one()?;
			if let Some(mut reader) = reader {
				log::debug!(
					target: "parity-db",
					"Enacting log {}",
					reader.record_id(),
				);
				loop {
					match reader.next()? {
						LogAction::BeginRecord(_) => {
							return Err(Error::Corruption("Bad log record".into()));
						},
						LogAction::EndRecord => {
							break;
						},
						LogAction::InsertIndex(insertion) => {
							self.columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertIndex(insertion), &mut reader)?;

						},
						LogAction::InsertValue(insertion) => {
							self.columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertValue(insertion), &mut reader)?;

						},
						LogAction::DropTable(id) => {
							log::info!(
								target: "parity-db",
								"Dropping index {}",
								id,
							);
							self.columns[id.col() as usize].drop_index(id)?;

						}
					}
				}
				for c in self.columns.iter() {
					c.complete_plan()?;
				}
				log::debug!(
					target: "parity-db",
					"Finished log {}",
					reader.record_id(),
				);
				let record_id = reader.record_id();
				let cleared = reader.drain();
				Some((record_id, cleared))
			} else {
				None
			}
		};

		if let Some((record_id, cleared)) = cleared {
			self.log.end_read(cleared, record_id);
			{
				let mut queue = self.log_queue.lock();
				if *queue == MAX_LOG_QUEUE_SIZE {
					self.log_cv.notify_all();
				}
				if *queue > 0 {
					*queue -= 1;
				}
			}
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn flush_all_logs(&self) -> Result<()> {
		while self.enact_logs()? { };
		Ok(())
	}

	fn collect_rebalance(&self) -> Result<bool> {
		// Process any pending rebalances
		for (i, c) in self.columns.iter().enumerate() {
			let (drop_index, batch) = c.rebalance(&self.log)?;
			if !batch.is_empty() {
				log::debug!(
					target: "parity-db",
					"Added pending rebalance",
				);
				let mut queue = self.rebalance_queue.lock();
				queue.push_back(PendingRebalance {
					drop_index,
					batch,
					column: i as u8,
				});
				self.signal_worker();
				return Ok(true);
			}
		}
		return Ok(false);
	}

	fn shutdown(&self) {
		self.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
		self.signal_worker();
		self.signal_rebalance();
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	rebalance_thread: Option<std::thread::JoinHandle<Result<()>>>,
	worker_thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl Db {
	pub fn open(path: &std::path::Path, columns: u8) -> Result<Db> {
		let db = Arc::new(DbInner::open(path, columns)?);
		db.flush_all_logs()?;
		let rebalance_db = db.clone();
		let rebalance_thread = std::thread::spawn(move ||
			Self::rebalance_worker(rebalance_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		let worker_db = db.clone();
		let worker_thread = std::thread::spawn(move ||
			Self::db_worker(worker_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		Ok(Db {
			inner: db,
			worker_thread: Some(worker_thread),
			rebalance_thread: Some(rebalance_thread),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		self.inner.get(col, key)
	}

	pub fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item=(ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		self.inner.commit(tx)
	}

	pub fn num_columns(&self) -> u8 {
		self.inner.columns.len() as u8
	}

	fn db_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.work.lock();
				db.worker_cv.wait(&mut work);
			}

			more_work = db.enact_logs()?;
			if !more_work {
				let more_commits = db.process_commits()?;
				let more_rebalance = db.process_rebalance()?;
				more_work = more_commits || more_rebalance;
			}
			more_work = more_work || db.process_rebalance()?;
		}
		Ok(())
	}

	fn rebalance_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.rebalance_work.lock();
				db.need_rebalance_cv.wait(&mut work);
			}

			more_work = db.collect_rebalance()?;
		}
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.rebalance_thread.take().map(|t| t.join());
		self.worker_thread.take().map(|t| t.join());
	}
}

