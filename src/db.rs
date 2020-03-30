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
use parking_lot::{RwLock, Mutex, Condvar};
use kvdb::{DBOp, DBTransaction};
use crate::{
	bucket::{Key, RebalanceProgress},
	error::{Error, Result},
	column::{ColId, Column},
	log::{Log, LogAction}
};

pub type Value = Vec<u8>;

fn hash(key: &[u8]) -> Key {
	let mut k = Key::default();
	k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, &[], key).as_bytes());
	//log::trace!(target: "parity-db", "HASH {} = {}", hex(&key), hex(&k));
	k
}



struct DbInner {
	columns: RwLock<Vec<Column>>,
	_path: std::path::PathBuf,
	shutdown: std::sync::atomic::AtomicBool,
	rebalancing: Mutex<bool>,
	rebalace_condvar: Condvar,
	log: RwLock<Log>,
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
			log: RwLock::new(Log::open(path)?),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Option<Value> {
		let columns = self.columns.read();
		let log = self.log.read();
		columns[col as usize].get(&hash(key), &log)
	}

	fn signal_worker(&self) {
		let mut active = self.rebalancing.lock();
		*active = true;
		log::trace!("Starting rebalance");
		self.rebalace_condvar.notify_one();
	}

	fn commit(&self, tx: DBTransaction) -> Result<()> {
		// TODO: take read lock on columns for writing log.
		{
			let mut columns = self.columns.write();
			let mut log = self.log.write();
			let mut log = log.begin_record()?;
			log::debug!(
				target: "parity-db",
				"Starting commit {}, {} ops",
				log.record_id(),
				tx.ops.len(),
			);
			for op in tx.ops {
				let (c, key, value) = match op {
					DBOp::Insert { col: c, key, value } => {
						(c, hash(&key), Some(value))
					}
					DBOp::Delete { col: c, key } => {
						(c, hash(&key), None)
					}
				};

				columns[c as usize].write_plan(&key, value, &mut log)?;
			}
			log.end_record()?;
			log::debug!(
				target: "parity-db",
				"Completed commit {}",
				log.record_id(),
			);
		}
		self.signal_worker();
		Ok(())
	}

	fn enact_logs(&self) -> Result<bool> {
		let mut columns = self.columns.write();
		let mut log = self.log.write();
		if let Some(mut reader) = log.flush_one()? {
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
					LogAction::Insert(insertion) => {
						columns[insertion.bucket.col() as usize].enact_plan(insertion.bucket, insertion.index, &mut reader)?;

					},
					LogAction::DropBucket(id) => {
						columns[id.col() as usize].drop_bucket(id)?;

					}
				}
			}
			log::debug!(
				target: "parity-db",
				"Finished log {}",
				reader.record_id(),
			);
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
		let mut log = self.log.write();
		for c in columns.iter_mut() {
			match c.rebalance(&mut log)? {
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
			Self::db_worker(worker_db).map_err(|e| { log::warn!("DB ERROR: {:?}", e); e })
		);
		db.flush_all_logs()?;
		Ok(Db {
			inner: db,
			balance_thread: Some(worker),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		Ok(self.inner.get(col, key))
	}

	pub fn commit(&self, tx: DBTransaction) -> Result<()> {
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

			// Flush log
			more_work = db.enact_logs()?;
			more_work = more_work || db.process_rebalance()?;
		}

		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.balance_thread.take().map(|t| t.join());
		for (i, c) in self.inner.columns.read().iter().enumerate() {
			println!("HISTOGRAM Column {}, {} blobs", i, c.blobs.len());
			for (s, count) in &c.histogram {
				println!("{}:{}", s, count);
			}
		}
	}
}

