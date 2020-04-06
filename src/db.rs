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

use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::collections::{HashMap, VecDeque};
use parking_lot::{RwLock, Mutex, Condvar};
use crate::{
	table::{Key, Address},
	error::{Error, Result},
	column::{ColId, Column},
	log::{Log, LogAction},
	index::{PlanOutcome, TableId as IndexTableId},
};


// These are in memory, so we use usize
const MAX_COMMIT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
// These are disk-backed, so we use u64
const MAX_LOG_QUEUE_BYTES: u64 = 32 * 1024 * 1024;

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
	bytes: usize,
	changeset: Vec<(ColId, Key, Option<Value>)>,
}

#[derive(Default)]
struct CommitQueue{
	record_id: u64,
	bytes: usize,
	commits: VecDeque<Commit>,
}

#[derive(Default)]
struct PendingReindex {
	column: ColId,
	batch: Vec<(Key, Address)>,
	drop_index: Option<IndexTableId>,
}

struct DbInner {
	columns: Vec<Column>,
	_path: std::path::PathBuf,
	shutdown: AtomicBool,
	reindex_queue: Mutex<VecDeque<PendingReindex>>,
	need_reindex_cv: Condvar,
	reindex_work: Mutex<bool>,
	log: Log,
	commit_queue: Mutex<CommitQueue>,
	commit_queue_full_cv: Condvar,
	log_worker_cv: Condvar,
	log_work: Mutex<bool>,
	commit_worker_cv: Condvar,
	commit_work: Mutex<bool>,
	commit_overlay: RwLock<HashMap<ColId, HashMap<Key, (u64, Option<Value>)>>>,
	log_cv: Condvar,
	log_queue_bytes: Mutex<u64>,
	flush_worker_cv: Condvar,
	flush_work: Mutex<bool>,
	enact_mutex: Mutex<()>,
	last_enacted: AtomicU64,
	next_reindex: AtomicU64,
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
			reindex_queue: Mutex::new(Default::default()),
			need_reindex_cv: Condvar::new(),
			reindex_work: Mutex::new(false),
			log: Log::open(path)?,
			commit_queue: Mutex::new(Default::default()),
			commit_queue_full_cv: Condvar::new(),
			log_worker_cv: Condvar::new(),
			log_work: Mutex::new(false),
			commit_worker_cv: Condvar::new(),
			commit_work: Mutex::new(false),
			commit_overlay: RwLock::new(Default::default()),
			log_queue_bytes: Mutex::new(0),
			log_cv: Condvar::new(),
			flush_worker_cv: Condvar::new(),
			flush_work: Mutex::new(false),
			enact_mutex: Mutex::new(()),
			next_reindex: AtomicU64::new(0),
			last_enacted: AtomicU64::new(0),
		})
	}

	fn signal_log_worker(&self) {
		let mut work = self.log_work.lock();
		*work = true;
		self.log_worker_cv.notify_one();
	}

	fn signal_commit_worker(&self) {
		let mut work = self.commit_work.lock();
		*work = true;
		self.commit_worker_cv.notify_one();
	}

	fn signal_reindex_worker(&self) {
		let mut work = self.reindex_work.lock();
		*work = true;
		self.need_reindex_cv.notify_one();
	}

	fn signal_flush_worker(&self) {
		let mut work = self.flush_work.lock();
		*work = true;
		self.flush_worker_cv.notify_one();
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
		let commit: Vec<_> = tx.into_iter().map(|(c, k, v)| (c, hash(k.as_ref()), v)).collect();

		{
			let mut queue = self.commit_queue.lock();
			if queue.bytes > MAX_COMMIT_QUEUE_BYTES {
				self.commit_queue_full_cv.wait(&mut queue);
			}
			let mut overlay = self.commit_overlay.write();

			queue.record_id += 1;
			let record_id = queue.record_id + 1;

			let mut bytes = 0;
			for (c, k, v) in &commit {
				bytes += k.len();
				bytes += v.as_ref().map_or(0, |v|v.len());
				overlay.entry(*c).or_default().insert(*k, (record_id, v.clone()));
			}

			let commit = Commit {
				id: record_id,
				changeset: commit,
				bytes,
			};

			log::debug!(
				target: "parity-db",
				"Queued commit {}, {} bytes",
				commit.id,
				bytes,
			);
			queue.commits.push_back(commit);
			queue.bytes += bytes;
			self.signal_log_worker();
		}
		Ok(())
	}

	fn process_commits(&self) -> Result<bool> {
		{
			let mut queue = self.log_queue_bytes.lock();
			if *queue > MAX_LOG_QUEUE_BYTES {
				self.log_cv.wait(&mut queue);
			}
		}
		let commit = {
			let mut queue = self.commit_queue.lock();
			if let Some(commit) = queue.commits.pop_front() {
				queue.bytes -= commit.bytes;
				log::debug!(
					target: "parity-db",
					"Removed {}. Still queued commits {} bytes",
					commit.bytes,
					queue.bytes,
				);
				if queue.bytes <= MAX_COMMIT_QUEUE_BYTES && (queue.bytes + commit.bytes) > MAX_COMMIT_QUEUE_BYTES {
					// Past the waiting threshold.
					log::debug!(
						target: "parity-db",
						"Waking up commit queue worker",
					);
					self.commit_queue_full_cv.notify_one();
				}
				Some(commit)
			} else {
				None
			}
		};

		if let Some(commit) = commit {
			let mut reindex = false;
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Processing commit {}, record {}, {} bytes",
				commit.id,
				writer.record_id(),
				commit.bytes,
			);
			let mut ops: u64 = 0;
			for (c, key, value) in commit.changeset.iter() {
				match self.columns[*c as usize].write_plan(key, value, &mut writer)? {
					PlanOutcome::NeedReindex => {
						reindex = true;
					},
					_ => {},
				}
				ops += 1;
			}
			// Collect final changes to value tables
			for c in self.columns.iter() {
				c.complete_plan(&mut writer)?;
			}
			let record_id = writer.record_id();
			let l = writer.drain();

			let bytes = {
				let mut logged_bytes = self.log_queue_bytes.lock();
				let bytes = self.log.end_record(l)?;
				*logged_bytes += bytes;
				self.signal_flush_worker();
				bytes
			};

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

			if reindex {
				self.start_reindex(record_id);
			}

			log::debug!(
				target: "parity-db",
				"Processed commit {} (record {}), {} ops, {} bytes written",
				commit.id,
				record_id,
				ops,
				bytes,
			);
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn start_reindex(&self, record_id: u64) {
		self.next_reindex.store(record_id, Ordering::SeqCst);
	}

	fn process_reindex(&self) -> Result<bool> {
		{
			let mut queue = self.log_queue_bytes.lock();
			if *queue > MAX_LOG_QUEUE_BYTES {
				self.log_cv.wait(&mut queue);
			}
		}
		let reindex =  {
			let mut queue = self.reindex_queue.lock();
			let r = queue.pop_front();
			if queue.is_empty() {
				queue.shrink_to_fit();
			}
			r
		};

		if let Some(reindex) = reindex {
			let mut next_reindex = false;
			let mut writer = self.log.begin_record();
			log::debug!(
				target: "parity-db",
				"Creating reindex record {}",
				writer.record_id(),
			);
			let column = &self.columns[reindex.column as usize];
			for (key, address) in reindex.batch.into_iter() {
				match column.write_index_plan(&key, address, &mut writer)? {
					PlanOutcome::NeedReindex => {
						next_reindex = true
					},
					_ => {},
				}
			}
			if let Some(table) = reindex.drop_index {
				writer.drop_table(table);
			}
			let record_id = writer.record_id();
			let l = writer.drain();
			let bytes = self.log.end_record(l)?;

			log::debug!(
				target: "parity-db",
				"Created reindex record {}, {} bytes",
				record_id,
				bytes,
			);
			let mut logged_bytes = self.log_queue_bytes.lock();
			*logged_bytes += bytes;
			if next_reindex {
				self.start_reindex(record_id);
			}
			self.signal_flush_worker();
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn enact_logs(&self, validation_mode: bool) -> Result<bool> {
		let cleared = {
			let _lock = self.enact_mutex.lock();
			let reader = match self.log.read_next() {
				Ok(reader) => reader,
				Err(Error::Corruption(_)) if validation_mode => {
					log::info!(target: "parity-db", "Bad log header");
					self.log.clear_logs()?;
					return Ok(false);
				}
				Err(e) => return Err(e),
			};
			if let Some(mut reader) = reader {
				log::debug!(
					target: "parity-db",
					"Enacting log {}",
					reader.record_id(),
				);
				if validation_mode {
					// Validate all records before applying anything
					loop {
						match reader.next()? {
							LogAction::BeginRecord(_) => {
								log::debug!(target: "parity-db", "Unexpected log header");
								std::mem::drop(reader);
								self.log.clear_logs()?;
								return Ok(false);
							},
							LogAction::EndRecord => {
								break;
							},
							LogAction::InsertIndex(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertIndex(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Eror replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_logs()?;
									return Ok(false);
								}
							},
							LogAction::InsertValue(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertValue(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Eror replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_logs()?;
									return Ok(false);
								}
							},
							LogAction::DropTable(_) => {
								continue;
							}
						}
					}
					reader.reset()?;
					reader.next()?;
				}
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
				log::debug!(
					target: "parity-db",
					"Enacted log record {}, {} bytes",
					reader.record_id(),
					reader.read_bytes(),
				);
				let record_id = reader.record_id();
				let bytes = reader.read_bytes();
				let cleared = reader.drain();
				self.last_enacted.store(record_id, Ordering::SeqCst);
				if record_id == self.next_reindex.load(Ordering::SeqCst) {
					self.signal_reindex_worker();
				}
				Some((record_id, cleared, bytes))
			} else {
				None
			}
		};

		if let Some((record_id, cleared, bytes)) = cleared {
			self.log.end_read(cleared, record_id);
			{
				if !validation_mode {
					let mut queue = self.log_queue_bytes.lock();
					*queue -= bytes;
					if *queue <= MAX_LOG_QUEUE_BYTES && (*queue + bytes) > MAX_LOG_QUEUE_BYTES {
						self.log_cv.notify_all();
					}
					log::debug!(target: "parity-db", "Log queue size: {} bytes", *queue);
				}
			}
			Ok(true)
		} else {
			Ok(false)
		}
	}

	fn flush_logs(&self) -> Result<bool> {
		let (flush_next, read_next) = self.log.flush_one()?;
		if read_next {
			self.signal_commit_worker();
		}
		Ok(flush_next)
	}

	fn replay_all_logs(&self) -> Result<()> {
		log::info!(target: "parity-db", "Replaying database log...");
		while self.enact_logs(true)? { }
		while self.flush_logs()? {
			while self.enact_logs(true)? { }
		}
		// Need one more pass to enact the last log.
		while self.enact_logs(true)? { }
		// Re-read any cached metadata
		for c in self.columns.iter() {
			c.refresh_metadata()?;
		}
		log::info!(target: "parity-db", "Done.");
		Ok(())
	}

	fn collect_reindex(&self) -> Result<bool> {
		// Process any pending reindexs
		for (i, c) in self.columns.iter().enumerate() {
			let (drop_index, batch) = c.reindex(&self.log)?;
			if !batch.is_empty() {
				log::debug!(
					target: "parity-db",
					"Added pending reindex",
				);
				let mut queue = self.reindex_queue.lock();
				queue.push_back(PendingReindex {
					drop_index,
					batch,
					column: i as u8,
				});
				self.signal_log_worker();
				return Ok(true);
			}
		}
		return Ok(false);
	}

	fn shutdown(&self) {
		self.shutdown.store(true, Ordering::SeqCst);
		self.signal_flush_worker();
		self.signal_log_worker();
		self.signal_commit_worker();
		self.signal_reindex_worker();
		self.log.shutdown();
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	reindex_thread: Option<std::thread::JoinHandle<Result<()>>>,
	commit_thread: Option<std::thread::JoinHandle<Result<()>>>,
	flush_thread: Option<std::thread::JoinHandle<Result<()>>>,
	log_thread: Option<std::thread::JoinHandle<Result<()>>>,
}

impl Db {
	pub fn open(path: &std::path::Path, columns: u8) -> Result<Db> {
		let db = Arc::new(DbInner::open(path, columns)?);
		db.replay_all_logs()?;
		let reindex_db = db.clone();
		let reindex_thread = std::thread::spawn(move ||
			Self::reindex_worker(reindex_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		let commit_worker_db = db.clone();
		let commit_thread = std::thread::spawn(move ||
			Self::commit_worker(commit_worker_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		let flush_worker_db = db.clone();
		let flush_thread = std::thread::spawn(move ||
			Self::flush_worker(flush_worker_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		let log_worker_db = db.clone();
		let log_thread = std::thread::spawn(move ||
			Self::log_worker(log_worker_db).map_err(|e| {
				log::error!(target: "parity-db", "DB ERROR: {:?}", e);
				panic!(e)
			})
		);
		Ok(Db {
			inner: db,
			commit_thread: Some(commit_thread),
			flush_thread: Some(flush_thread),
			log_thread: Some(log_thread),
			reindex_thread: Some(reindex_thread),
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

	fn commit_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.commit_work.lock();
				while !*work {
					db.commit_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			more_work = db.enact_logs(false)?;
		}
		log::debug!(target: "parity-db", "Commit worker shutdown");
		Ok(())
	}

	fn log_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.log_work.lock();
				while !*work {
					db.log_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			let more_commits = db.process_commits()?;
			let more_reindex = db.process_reindex()?;
			more_work = more_commits || more_reindex;
		}
		log::debug!(target: "parity-db", "Log worker shutdown");
		Ok(())
	}

	fn reindex_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.reindex_work.lock();
				while !*work {
					db.need_reindex_cv.wait(&mut work)
				};
				*work = false;
			}

			more_work = db.collect_reindex()?;
		}
		log::debug!(target: "parity-db", "Reindex worker shutdown");
		Ok(())
	}

	fn flush_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			// Wait for a task
			if !more_work {
				let mut work = db.flush_work.lock();
				while !*work {
					db.flush_worker_cv.wait(&mut work)
				};
				*work = false;
			}
			more_work = db.flush_logs()?;
		}
		log::debug!(target: "parity-db", "Flush worker shutdown");
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		self.reindex_thread.take().map(|t| t.join());
		self.log_thread.take().map(|t| t.join());
		self.flush_thread.take().map(|t| t.join());
		self.commit_thread.take().map(|t| t.join());
	}
}

