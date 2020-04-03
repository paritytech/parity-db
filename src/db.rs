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
	table::Key,
	error::{Error, Result},
	column::{ColId, Column, RebalanceProgress},
	log::{Log, LogAction},
};

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

struct DbInner {
	columns: RwLock<Vec<Column>>,
	_path: std::path::PathBuf,
	shutdown: std::sync::atomic::AtomicBool,
	rebalancing: Mutex<bool>,
	rebalace_condvar: Condvar,
	log: Log,
	commit_queue: RwLock<CommitQueue>,
	commit_overlay: RwLock<HashMap<ColId, HashMap<Key, (u64, Option<Value>)>>>,
}

impl DbInner {
	pub fn open(path: &std::path::Path, num_columns: u8) -> Result<DbInner> {
		std::fs::create_dir_all(path)?;
		let mut columns = Vec::with_capacity(num_columns as usize);
		for c in 0 .. num_columns {
			columns.push(Column::open(c, path)?);
		}
		Ok(DbInner {
			columns: RwLock::new(columns),
			_path: path.into(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			rebalancing: Mutex::new(false),
			rebalace_condvar: Condvar::new(),
			log: Log::open(path)?,
			commit_queue: RwLock::new(Default::default()),
			commit_overlay: RwLock::new(Default::default()),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		let columns = self.columns.read();
		let overlay = self.commit_overlay.read();
		let key = hash(key);
		if let Some(v) = overlay.get(&col).and_then(|o| o.get(&key).map(|(_, v)| v.clone())) {
			return Ok(v);
		}
		let log = self.log.overlays();
		columns[col as usize].get(&key, &log)
	}

	fn signal_worker(&self) {
		let mut active = self.rebalancing.lock();
		*active = true;
		self.rebalace_condvar.notify_one();
	}

	fn commit<I, K>(&self, tx: I) -> Result<()>
		where
			I: IntoIterator<Item=(ColId, K, Option<Value>)>,
			K: AsRef<[u8]>,
	{
		let commit = tx.into_iter().map(|(c, k, v)| (c, hash(k.as_ref()), v)).collect();

		{
			let mut queue = self.commit_queue.write();
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
		}

		self.signal_worker();
		Ok(())
	}

	fn process_commits(&self) -> Result<bool> {
		// TODO: take read lock on columns for writing log.
		let commit = {
			self.commit_queue.write().commits.pop_front()
		};

		if let Some(commit) = commit {
			let mut columns = self.columns.write();
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Processing commit {}, record {}",
				commit.id,
				writer.record_id(),
			);
			let mut ops: u64 = 0;
			for (c, key, value) in commit.changeset.iter() {
				columns[*c as usize].write_plan(key, value, &mut writer)?;
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

			log::debug!(
				target: "parity-db",
				"Processed commit {}, {} ops",
				record_id,
				ops,
			);
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn enact_logs(&self) -> Result<bool> {
		let mut columns = self.columns.write();

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
							columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertIndex(insertion), &mut reader)?;

						},
						LogAction::InsertValue(insertion) => {
							columns[insertion.table.col() as usize]
								.enact_plan(LogAction::InsertValue(insertion), &mut reader)?;

						},
						LogAction::DropTable(id) => {
							log::info!(
								target: "parity-db",
								"Dropping index {}",
								id,
							);
							columns[id.col() as usize].drop_index(id)?;

						}
					}
				}
				for c in columns.iter_mut() {
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
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn flush_all_logs(&self) -> Result<()> {
		while self.enact_logs()? { };
		Ok(())
	}

	fn process_rebalance(&self) -> Result<bool> {
		// Process any pending rebalances
		let mut columns = self.columns.write();
		for c in columns.iter_mut() {
			match c.rebalance(&self.log)? {
				RebalanceProgress::InProgress((p, t)) => {
					log::debug!(
						target: "parity-db",
						"Continue rebalance {}/{}",
						p,
						t,
					);
					return Ok(true);
				},
				RebalanceProgress::Inactive => {},
			}
		}
		return Ok(false);
	}

	fn shutdown(&self) {
		self.shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
		self.signal_worker();
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	balance_thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl Db {
	pub fn open(path: &std::path::Path, columns: u8) -> Result<Db> {
		let db = Arc::new(DbInner::open(path, columns)?);
		let worker_db = db.clone();
		let worker = std::thread::spawn(move ||
			Self::db_worker(worker_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		db.flush_all_logs()?;
		Ok(Db {
			inner: db,
			balance_thread: Some(worker),
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
		self.inner.columns.read().len() as u8
	}

	fn db_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut active = db.rebalancing.lock();
				if !*active {
					db.rebalace_condvar.wait(&mut active);
				}
				*active = false;
			}

			more_work = db.process_commits()?;
			more_work = more_work || db.enact_logs()?;
			more_work = more_work || db.process_rebalance()?;
			// Flush log
			/*
			let more_commits = db.process_commits()?;
			let more_logs = db.enact_logs()?;
			let more_rebalance = db.process_rebalance()?;
			more_work = more_commits || more_rebalance || more_logs;
			*/
		}
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.balance_thread.take().map(|t| t.join());
	}
}

