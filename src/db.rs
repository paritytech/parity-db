// Copyright 2015-2021 Parity Technologies (UK) Ltd.
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

/// The database objects is split into `Db` and `DbInner`.
/// `Db` creates shared `DbInner` instance and manages background
/// worker threads that all use the inner object.
///
/// There are 4 worker threads:
/// log_worker: Processes commit queue and reindexing. For each commit
/// in the queue, log worker creates a write-ahead record using `Log`.
/// Additionally, if there are active reindexing, it creates log records
/// for batches of relocated index entries.
/// flush_worker: Flushes log records to disk by calling `fsync` on the
/// log files.
/// commit_worker: Reads flushed log records and applies operations to the
/// index and value tables.
/// cleanup_worker: Flush tables by calling `fsync`, and cleanup log.
/// Each background worker is signalled with a conditional variable once
/// there is some work to be done.

use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::collections::{HashMap, VecDeque, BTreeMap};
use parking_lot::{RwLock, Mutex, Condvar};
use fs2::FileExt;
use crate::{
	Key,
	table::key::TableKey,
	error::{Error, Result},
	column::{ColId, Column, IterState},
	log::{Log, LogAction},
	index::PlanOutcome,
	options::Options,
	btree::commit_overlay::{BTreeChangeSet},
};

// These are in memory, so we use usize
const MAX_COMMIT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
// These are disk-backed, so we use u64
const MAX_LOG_QUEUE_BYTES: i64 = 128 * 1024 * 1024;
const MIN_LOG_SIZE: u64 = 64 * 1024 * 1024;
const KEEP_LOGS: usize = 16;

/// Value is just a vector of bytes. Value sizes up to 4Gb are allowed.
pub type Value = Vec<u8>;

// Commit data passed to `commit`
#[derive(Default)]
struct Commit {
	// Commit ID. This is not the same as log record id, as some records
	// are originated within the DB. E.g. reindex.
	id: u64,
	// Size of user data pending insertion (keys + values) or
	// removal (keys)
	bytes: usize,
	// Operations.
	changeset: CommitChangeSet,
}

// Pending commits. This may not grow beyond `MAX_COMMIT_QUEUE_BYTES` bytes.
#[derive(Default)]
struct CommitQueue {
	// Log record.
	record_id: u64,
	// Total size of all commits in the queue.
	bytes: usize,
	// FIFO queue.
	commits: VecDeque<Commit>,
}

pub(crate) struct DbInner {
	columns: Vec<Column>,
	options: Options,
	shutdown: AtomicBool,
	log: Log,
	commit_queue: Mutex<CommitQueue>,
	commit_queue_full_cv: Condvar,
	log_worker_cv: Condvar,
	log_work: Mutex<bool>,
	commit_worker_cv: Condvar,
	commit_work: Mutex<bool>,
	// Overlay of most recent values in the commit queue.
	commit_overlay: RwLock<Vec<CommitOverlay>>,
	log_cv: Condvar,
	log_queue_bytes: Mutex<i64>, // This may underflow occasionally, but is bound for 0 eventually
	flush_worker_cv: Condvar,
	flush_work: Mutex<bool>,
	cleanup_worker_cv: Condvar,
	cleanup_work: Mutex<bool>,
	last_enacted: AtomicU64,
	next_reindex: AtomicU64,
	bg_err: Mutex<Option<Arc<Error>>>,
	_lock_file: std::fs::File,
}

impl DbInner {
	fn open(options: &Options, create: bool, skip_checks: bool) -> Result<DbInner> {
		if create {
			std::fs::create_dir_all(&options.path)?
		};
		let mut lock_path: std::path::PathBuf = options.path.clone();
		lock_path.push("lock");
		let lock_file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(lock_path.as_path())?;
		if !skip_checks {
			lock_file.try_lock_exclusive().map_err(|e| Error::Locked(e))?;
		}

		let metadata = options.load_and_validate_metadata(create)?;
		let mut columns = Vec::with_capacity(metadata.columns.len());
		let mut commit_overlay = Vec::with_capacity(metadata.columns.len());
		let log = Log::open(&options)?;
		let last_enacted = log.replay_record_id().unwrap_or(2) - 1;
		for c in 0 .. metadata.columns.len() {
			let column = Column::open(c as ColId, &options, &metadata)?;
			commit_overlay.push(CommitOverlay::new());
			columns.push(column);
		}
		log::debug!(target: "parity-db", "Opened db {:?}, metadata={:?}", options, metadata);
		let mut options = options.clone();
		if options.salt.is_none() {
			options.salt = Some(metadata.salt);
		}

		Ok(DbInner {
			columns,
			options: options.clone(),
			shutdown: std::sync::atomic::AtomicBool::new(false),
			log,
			commit_queue: Mutex::new(Default::default()),
			commit_queue_full_cv: Condvar::new(),
			log_worker_cv: Condvar::new(),
			log_work: Mutex::new(false),
			commit_worker_cv: Condvar::new(),
			commit_work: Mutex::new(false),
			commit_overlay: RwLock::new(commit_overlay),
			log_queue_bytes: Mutex::new(0),
			log_cv: Condvar::new(),
			flush_worker_cv: Condvar::new(),
			flush_work: Mutex::new(false),
			cleanup_worker_cv: Condvar::new(),
			cleanup_work: Mutex::new(false),
			next_reindex: AtomicU64::new(1),
			last_enacted: AtomicU64::new(last_enacted),
			bg_err: Mutex::new(None),
			_lock_file: lock_file,
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

	fn signal_flush_worker(&self) {
		let mut work = self.flush_work.lock();
		*work = true;
		self.flush_worker_cv.notify_one();
	}

	fn signal_cleanup_worker(&self) {
		let mut work = self.cleanup_work.lock();
		*work = true;
		self.cleanup_worker_cv.notify_one();
	}

	fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		if self.columns[col as usize].indexed() {
			return self.btree_get(col, key);
		}
		let key = self.columns[col as usize].hash(key);
		let overlay = self.commit_overlay.read();
		// Check commit overlay first
		if let Some(v) = overlay.get(col as usize).and_then(|o| o.get(&key)) {
			return Ok(v);
		}
		// Go into tables and log overlay.
		let log = self.log.overlays();
		let key = TableKey::Partial(key);
		self.columns[col as usize].get(&key, log)
	}

	fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		let key = self.columns[col as usize].hash(key);
		let overlay = self.commit_overlay.read();
		// Check commit overlay first
		if let Some(l) = overlay.get(col as usize).and_then(|o| o.get_size(&key)) {
			return Ok(l);
		}
		// Go into tables and log overlay.
		let log = self.log.overlays();
		let key = TableKey::Partial(key);
		self.columns[col as usize].get_size(&key, log)
	}

	fn btree_get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		let overlay = self.commit_overlay.read();
		if let Some(l) = overlay.get(col as usize).and_then(|o| o.btree_get(key)) {
			return Ok(l.cloned());
		}
		// We lock log as btree structure changed while reading it would be an issue.
		let log = self.log.overlays().read();
		self.columns[col as usize].btree_get(key, &*log)
	}

	pub fn btree_iter(&self, col: ColId) -> Result<crate::btree::BTreeIterator> {
		let log = self.log.overlays();
		crate::btree::BTreeIterator::new(self, col, log)
	}

	pub(crate) fn btree_iter_next(&self, iter: &mut crate::BTreeIterator) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
		// we impose lock to ensure the internal btree stays in sync
		// with the node structure.
		// TODO long term replace with a layer keeping diff in the iter and
		// remove LogQuery implementation from &Vec<Overlays>.
		// (could also have log per column but ~).
		let col = iter.col;
		let log = self.log.overlays().read();
		let record_id = log.btree_last_record_id(iter.col);
		let commit_overlay = self.commit_overlay.read();
		let next_commit_overlay = commit_overlay.get(col as usize).and_then(|o| o.btree_next(&iter.overlay_last_key, iter.from_seek));
		// droping lock to overlay, there is no consistency.
		std::mem::drop(commit_overlay);
		let column = &self.columns[col as usize];
		let next_backend = if let Some(n) = iter.pending_next_backend.take() {
			n
		} else {
			iter.next_backend(record_id, column, &*log)?
		};

		match (next_commit_overlay, next_backend) {
			(Some((commit_key, commit_value)), Some((backend_key, backend_value))) => {
				match commit_key.cmp(&backend_key) {
					std::cmp::Ordering::Less => {
						if let Some(value) = commit_value {
							iter.overlay_last_key = Some(commit_key.clone());
							iter.from_seek = false;
							iter.pending_next_backend = Some(Some((backend_key, backend_value)));
							return Ok(Some((commit_key, value)));
						} else {
							iter.overlay_last_key = Some(commit_key);
							iter.from_seek = false;
							iter.pending_next_backend = Some(Some((backend_key, backend_value)));
							std::mem::drop(log);
							return self.btree_iter_next(iter);
						}
					},
					std::cmp::Ordering::Greater => {
						return Ok(Some((backend_key, backend_value)));
					},
					std::cmp::Ordering::Equal => {
						if let Some(value) = commit_value {
							iter.overlay_last_key = Some(commit_key);
							iter.from_seek = false;
							return Ok(Some((backend_key, value)));
						} else {
							iter.overlay_last_key = Some(commit_key);
							iter.from_seek = false;
							std::mem::drop(log);
							return self.btree_iter_next(iter);
						}
					},
				}
			},
			(Some((commit_key, commit_value)), None) => {
				if let Some(value) = commit_value {
					iter.overlay_last_key = Some(commit_key.clone());
					iter.from_seek = false;
					iter.pending_next_backend = Some(None);
					return Ok(Some((commit_key, value)));
				} else {
					iter.overlay_last_key = Some(commit_key);
					iter.from_seek = false;
					iter.pending_next_backend = Some(None);
					std::mem::drop(log);
					return self.btree_iter_next(iter);
				}
			},
			(None, Some((backend_key, backend_value))) => {
				return Ok(Some((backend_key, backend_value)));
			},
			(None, None) => {
				iter.pending_next_backend = Some(None);
				return Ok(None);
			},
		}
	}

	pub(crate) fn btree_iter_seek(&self, iter: &mut crate::BTreeIterator, key: &[u8], after: bool) -> Result<()> {
		// seek require log do not change
		let log = self.log.overlays().read();
		let record_id = log.btree_last_record_id(iter.col);
		iter.from_seek = !after;
		iter.overlay_last_key = Some(key.to_vec());
		iter.pending_next_backend = None;
		let column = &self.columns[iter.col as usize];
		iter.seek_backend(key.to_vec(), record_id, column, &*log, after)
	}

	// Commit simply adds the the data to the queue and to the overlay and
	// exits as early as possible.
	fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item=(ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		let mut commit: CommitChangeSet = Default::default();
		for (c, k, v) in tx.into_iter() {
			if self.options.columns[c as usize].btree_index {
				commit.btree_indexed.entry(c)
					.or_insert_with(|| BTreeChangeSet::new(c))
					.push(k.as_ref(), v)
			} else {
				commit.indexed.entry(c)
					.or_insert_with(|| IndexedChangeSet::new(c))
					.push(k.as_ref(), v, &self.options)
			}
		}

		self.commit_raw(commit)
	}

	fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		{
			let mut queue = self.commit_queue.lock();
			if queue.bytes > MAX_COMMIT_QUEUE_BYTES {
				log::debug!(target: "parity-db", "Waiting, qb={}", queue.bytes);
				self.commit_queue_full_cv.wait(&mut queue);
			}
			{
				let bg_err = self.bg_err.lock();
				if let Some(err) = &*bg_err {
					return Err(Error::Background(err.clone()));
				}
			}

			let mut overlay = self.commit_overlay.write();

			queue.record_id += 1;
			let record_id = queue.record_id + 1;

			let mut bytes = 0;
			for (c, indexed) in &commit.indexed {
				indexed.copy_to_overlay(&mut overlay[*c as usize], record_id, &mut bytes, &self.options);
			}

			for (c, iterset) in &commit.btree_indexed {
				iterset.copy_to_overlay(&mut overlay[*c as usize].btree_indexed, record_id, &mut bytes, &self.options);
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

	fn process_commits(&self, test_state: &TestDbTarget) -> Result<bool> {
		{
			// Wait if the queue is too big.
			let mut queue = self.log_queue_bytes.lock();
			if !self.shutdown.load(Ordering::Relaxed) && *queue > MAX_LOG_QUEUE_BYTES {
				log::debug!(target: "parity-db", "Waiting, log_bytes={}", queue);
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

		if let Some(mut commit) = commit {
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
			for (c, key_values) in commit.changeset.indexed.iter() {
				key_values.write_plan(&self.columns[*c as usize], &mut writer, &mut ops, &mut reindex)?;
			}

			for (c, btree) in commit.changeset.btree_indexed.iter() {
				btree.write_plan(&self.columns[*c as usize], &mut writer, &mut ops)?;
			}

			// Collect final changes to value tables
			for c in self.columns.iter() {
				c.complete_plan(&mut writer)?;
			}
			let record_id = writer.record_id();
			let l = writer.drain();

			let bytes = {
				let bytes = self.log.end_record(l)?;
				let mut logged_bytes = self.log_queue_bytes.lock();
				*logged_bytes += bytes as i64;
				self.signal_flush_worker();
				bytes
			};

			{
				// Cleanup the commit overlay.
				let mut overlay = self.commit_overlay.write();
				for (c, key_values) in commit.changeset.indexed.iter() {
					key_values.clean_overlay(&mut overlay[*c as usize], commit.id);
				}
				for (c, iterset) in commit.changeset.btree_indexed.iter_mut() {
					iterset.clean_overlay(&mut overlay[*c as usize].btree_indexed, commit.id);
				}
			}

			if reindex {
				self.start_reindex(record_id);
			}

			match test_state {
				TestDbTarget::LogOverlay(c) => {
					c.notify_one();
				},
				_ => (),
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
		let next_reindex = self.next_reindex.load(Ordering::SeqCst);
		if next_reindex == 0 || next_reindex > self.last_enacted.load(Ordering::SeqCst) {
			return Ok(false)
		}
		// Process any pending reindexes
		for column in self.columns.iter() {
			let (drop_index, batch) = column.reindex(&self.log)?;
			if !batch.is_empty() || drop_index.is_some() {
				let mut next_reindex = false;
				let mut writer = self.log.begin_record();
				log::debug!(
					target: "parity-db",
					"Creating reindex record {}",
					writer.record_id(),
				);
				for (key, address) in batch.into_iter() {
					let key = TableKey::Partial(key);
					match column.write_reindex_plan(&key, address, &mut writer)? {
						PlanOutcome::NeedReindex => {
							next_reindex = true
						},
						_ => {},
					}
				}
				if let Some(table) = drop_index {
					writer.drop_table(table);
				}
				let record_id = writer.record_id();
				let l = writer.drain();

				let mut logged_bytes = self.log_queue_bytes.lock();
				let bytes = self.log.end_record(l)?;
				log::debug!(
					target: "parity-db",
					"Created reindex record {}, {} bytes",
					record_id,
					bytes,
				);
				*logged_bytes += bytes as i64;
				if next_reindex {
					self.start_reindex(record_id);
				}
				self.signal_flush_worker();
				return Ok(true)
			}
		}
		self.next_reindex.store(0, Ordering::SeqCst);
		Ok(false)
	}

	fn enact_logs(&self, validation_mode: bool) -> Result<bool> {
		let cleared = {
			let reader = match self.log.read_next(validation_mode) {
				Ok(reader) => reader,
				Err(Error::Corruption(_)) if validation_mode => {
					log::debug!(target: "parity-db", "Bad log header");
					self.log.clear_replay_logs()?;
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
					if reader.record_id() != self.last_enacted.load(Ordering::Relaxed) + 1 {
						log::warn!(
							target: "parity-db",
							"Log sequence error. Expected record {}, got {}",
							self.last_enacted.load(Ordering::Relaxed) + 1,
							reader.record_id(),
						);
						std::mem::drop(reader);
						self.log.clear_replay_logs()?;
						return Ok(false);
					}
					// Validate all records before applying anything
					loop {
						let next = match reader.next() {
							Ok(next) => next,
							Err(e) => {
								log::debug!(target: "parity-db", "Error reading log: {:?}", e);
								std::mem::drop(reader);
								self.log.clear_replay_logs()?;
								return Ok(false);
							}
						};
						match next {
							LogAction::BeginRecord => {
								log::debug!(target: "parity-db", "Unexpected log header");
								std::mem::drop(reader);
								self.log.clear_replay_logs()?;
								return Ok(false);
							},
							LogAction::EndRecord => {
								break;
							},
							LogAction::InsertIndex(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertIndex(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_replay_logs()?;
									return Ok(false);
								}
							},
							LogAction::InsertValue(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col].validate_plan(LogAction::InsertValue(insertion), &mut reader) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_replay_logs()?;
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
						LogAction::BeginRecord => {
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
							log::debug!(
								target: "parity-db",
								"Dropping index {}",
								id,
							);
							self.columns[id.col() as usize].drop_index(id)?;
							// Check if there's another reindex on the next iteration
							self.start_reindex(reader.record_id());
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
				Some((record_id, cleared, bytes))
			} else {
				log::debug!(target: "parity-db", "End of log");
				None
			}
		};

		if let Some((record_id, cleared, bytes)) = cleared {
			self.log.end_read(cleared, record_id);
			{
				if !validation_mode {
					let mut queue = self.log_queue_bytes.lock();
					if *queue < bytes as i64 {
						log::warn!(
							target: "parity-db",
							"Detected log undeflow record {}, {} bytes, {} queued, reindex = {}",
							record_id,
							bytes,
							*queue,
							self.next_reindex.load(Ordering::SeqCst),
						);
					}
					*queue -= bytes as i64;
					if *queue <= MAX_LOG_QUEUE_BYTES && (*queue + bytes as i64) > MAX_LOG_QUEUE_BYTES {
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

	fn flush_logs(&self, min_log_size: u64) -> Result<bool> {
		let (flush_next, read_next, cleanup_next) = self.log.flush_one(min_log_size)?;
		if read_next {
			self.signal_commit_worker();
		}
		if cleanup_next {
			self.signal_cleanup_worker();
		}
		Ok(flush_next)
	}

	fn cleanup_logs(&self) -> Result<bool> {
		let keep_logs = if self.options.sync_data { 0 } else { KEEP_LOGS };
		let num_cleanup = self.log.num_dirty_logs();
		if num_cleanup > keep_logs {
			if self.options.sync_data {
				for c in self.columns.iter() {
					c.flush()?;
				}
			}
			self.log.clean_logs(num_cleanup - keep_logs)
		} else {
			Ok(false)
		}
	}

	fn clean_all_logs(&self) -> Result<()> {
		for c in self.columns.iter() {
			c.flush()?;
		}
		let num_cleanup = self.log.num_dirty_logs();
		self.log.clean_logs(num_cleanup)?;
		Ok(())
	}

	fn replay_all_logs(&mut self) -> Result<()> {
		while let Some(id) = self.log.replay_next()? {
			log::debug!(target: "parity-db", "Replaying database log {}", id);
			while self.enact_logs(true)? { }
		}
		// Re-read any cached metadata
		for c in self.columns.iter() {
			c.refresh_metadata()?;
		}
		log::debug!(target: "parity-db", "Replay is complete.");
		Ok(())
	}

	fn shutdown(&self) {
		self.shutdown.store(true, Ordering::SeqCst);
		self.log_cv.notify_all();
		self.signal_flush_worker();
		self.signal_log_worker();
		self.signal_commit_worker();
		self.signal_cleanup_worker();
	}

	fn kill_logs(&self) -> Result<()> {
		log::debug!(target: "parity-db", "Processing leftover commits");
		// Finish logged records and proceed to log and enact queued commits.
		while self.enact_logs(false)? {};
		self.flush_logs(0)?;
		while self.process_commits(&TestDbTarget::Standard)? {};
		while self.enact_logs(false)? {};
		self.flush_logs(0)?;
		while self.enact_logs(false)? {};
		self.clean_all_logs()?;
		self.log.kill_logs()?;
		if self.options.stats {
			let mut path = self.options.path.clone();
			path.push("stats.txt");
			match std::fs::File::create(path) {
				Ok(file) => {
					let mut writer = std::io::BufWriter::new(file);
					self.collect_stats(&mut writer, None)
				}
				Err(e) => log::warn!(target: "parity-db", "Error creating stats file: {:?}", e),
			}
		}
		Ok(())
	}

	fn collect_stats(&self, writer: &mut impl std::io::Write, column: Option<u8>) {
		if let Some(col) = column {
			self.columns[col as usize].write_stats(writer);
		} else {
			for c in self.columns.iter() {
				c.write_stats(writer);
			}
		}
	}

	fn clear_stats(&self, column: Option<u8>) {
		if let Some(col) = column {
			self.columns[col as usize].clear_stats();
		} else {
			for c in self.columns.iter() {
				c.clear_stats();
			}
		}
	}

	fn store_err(&self, result: Result<()>) {
		if let Err(e) = result {
			log::warn!(target: "parity-db", "Background worker error: {}", e);
			let mut err =  self.bg_err.lock();
			if err.is_none() {
				*err = Some(Arc::new(e));
				self.shutdown();
			}
			self.commit_queue_full_cv.notify_one();
		}
	}

	fn iter_column_while(&self, c: ColId, f: impl FnMut(IterState) -> bool) -> Result<()> {
		self.columns[c as usize].iter_while(&self.log, f)
	}

	pub(crate) fn column(&self, c: ColId) -> &Column {
		&self.columns[c as usize]
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	commit_thread: Option<std::thread::JoinHandle<()>>,
	flush_thread: Option<std::thread::JoinHandle<()>>,
	log_thread: Option<std::thread::JoinHandle<()>>,
	cleanup_thread: Option<std::thread::JoinHandle<()>>,
	do_drop: bool,
}

impl Db {
	#[cfg(test)]
	pub fn column(&self, at: usize) -> &Column {
		&self.inner.columns[at]
	}

	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Result<Db> {
		let options = Options::with_columns(path, num_columns);

		Self::open_inner(&options, true, false, TestDbTarget::Standard, false)
	}

	/// Open the database with given options.
	pub fn open(options: &Options) -> Result<Db> {
		Self::open_inner(options, false, false, TestDbTarget::Standard, false)
	}

	/// Create the database using given options.
	pub fn open_or_create(options: &Options) -> Result<Db> {
		Self::open_inner(options, true, false, TestDbTarget::Standard, false)
	}

	pub fn open_read_only(options: &Options) -> Result<Db> {
		Self::open_inner(options, false, true, TestDbTarget::Standard, false)
	}

	pub fn open_inner(options: &Options, create: bool, read_only: bool, test_state: TestDbTarget, skip_check_lock: bool) -> Result<Db> {
		assert!(options.is_valid());
		let mut db = DbInner::open(options, create, skip_check_lock)?;
		// This needs to be call before log thread: so first reindexing
		// will run in correct state.
		db.replay_all_logs()?;
		let db = Arc::new(db);
		if read_only {
			return Ok(Db {
				inner: db,
				commit_thread: None,
				flush_thread: None,
				log_thread: None,
				cleanup_thread: None,
				do_drop: false,
			})
		}
		let commit_worker_db = db.clone();
		let commit_notify = if let TestDbTarget::DbFile(c) = &test_state {
			Some(c.clone())
		} else {
			None
		};

		let commit_thread = std::thread::spawn(move ||
			commit_worker_db.store_err(Self::commit_worker(commit_worker_db.clone(), commit_notify))
		);
		let flush_worker_db = db.clone();
		let min_log_size = if matches!(test_state, TestDbTarget::DbFile(_)) {
			0
		} else {
			MIN_LOG_SIZE
		};
		let flush_thread = std::thread::spawn(move ||
			flush_worker_db.store_err(Self::flush_worker(flush_worker_db.clone(), min_log_size))
		);
		let log_thread = if !matches!(&test_state, &TestDbTarget::CommitOverlay) {
			let log_worker_db = db.clone();
			let log_test_state = test_state.clone();
			Some(std::thread::spawn(move ||
				log_worker_db.store_err(Self::log_worker(log_worker_db.clone(), log_test_state))
			))
		} else {
			None
		};
		let cleanup_worker_db = db.clone();
		let cleanup_thread = std::thread::spawn(move ||
			cleanup_worker_db.store_err(Self::cleanup_worker(cleanup_worker_db.clone()))
		);
		Ok(Db {
			inner: db,
			commit_thread: Some(commit_thread),
			flush_thread: Some(flush_thread),
			log_thread,
			cleanup_thread: Some(cleanup_thread),
			do_drop: matches!(test_state, TestDbTarget::Standard),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		self.inner.get(col, key)
	}

	pub fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		self.inner.get_size(col, key)
	}

	pub fn iter(&self, col: ColId) -> Result<crate::btree::BTreeIterator> {
		self.inner.btree_iter(col)
	}

	pub fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item=(ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		self.inner.commit(tx)
	}

	pub(crate) fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		self.inner.commit_raw(commit)
	}

	pub fn num_columns(&self) -> u8 {
		self.inner.columns.len() as u8
	}

	pub(crate) fn iter_column_while(&self, c: ColId, f: impl FnMut(IterState) -> bool) -> Result<()> {
		self.inner.iter_column_while(c, f)
	}

	fn commit_worker(db: Arc<DbInner>, notify: Option<TestSynch>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				let mut work = db.commit_work.lock();
				while !*work {
					db.commit_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			more_work = db.enact_logs(false)?;
			if more_work {
				notify.as_ref().map(|c| c.notify_one());
			}
		}
		log::debug!(target: "parity-db", "Commit worker shutdown");
		Ok(())
	}

	fn log_worker(db: Arc<DbInner>, test_state: TestDbTarget) -> Result<()> {
		// Start with pending reindex.
		let mut more_work = db.process_reindex()?;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				let mut work = db.log_work.lock();
				while !*work {
					db.log_worker_cv.wait(&mut work)
				};
				*work = false;
			}

			let more_commits = db.process_commits(&test_state)?;
			let more_reindex = db.process_reindex()?;
			more_work = more_commits || more_reindex;
		}
		log::debug!(target: "parity-db", "Log worker shutdown");
		Ok(())
	}

	fn flush_worker(db: Arc<DbInner>, min_log_size: u64) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) {
			if !more_work {
				let mut work = db.flush_work.lock();
				while !*work {
					db.flush_worker_cv.wait(&mut work)
				};
				*work = false;
			}
			more_work = db.flush_logs(min_log_size)?;
		}
		log::debug!(target: "parity-db", "Flush worker shutdown");
		Ok(())
	}

	fn cleanup_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = true;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				let mut work = db.cleanup_work.lock();
				while !*work {
					db.cleanup_worker_cv.wait(&mut work)
				};
				*work = false;
			}
			more_work = db.cleanup_logs()?;
		}
		log::debug!(target: "parity-db", "Cleanup worker shutdown");
		Ok(())
	}

	pub fn collect_stats(&self, writer: &mut impl std::io::Write, column: Option<u8>) {
		self.inner.collect_stats(writer, column)
	}

	pub fn clear_stats(&self, column: Option<u8>) {
		self.inner.clear_stats(column)
	}

	pub fn check_from_index(&self, check_param: check::CheckOptions) -> Result<()> {
		if let Some(col) = check_param.column.clone() {
			self.inner.columns[col as usize].check_from_index(&self.inner.log, &check_param, col)?;
		} else {
			for (ix, c) in self.inner.columns.iter().enumerate() {
				c.check_from_index(&self.inner.log, &check_param, ix as ColId)?;
			}
		}
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		if self.do_drop {
			self.inner.shutdown();
			self.log_thread.take().map(|t| t.join());
			self.flush_thread.take().map(|t| t.join());
			self.commit_thread.take().map(|t| t.join());
			self.cleanup_thread.take().map(|t| t.join());
			if let Err(e) = self.inner.kill_logs() {
				log::warn!(target: "parity-db", "Shutdown error: {:?}", e);
			}
		}
	}
}

pub(crate) type IndexedCommitOverlay = HashMap<Key, (u64, Option<Value>), crate::IdentityBuildHasher>;
pub(crate) type BTreeCommitOverlay = BTreeMap<Vec<u8>, (u64, Option<Value>)>;

struct CommitOverlay {
	indexed: IndexedCommitOverlay,
	btree_indexed: BTreeCommitOverlay,
}

impl CommitOverlay {
	fn new() -> Self {
		CommitOverlay {
			indexed: Default::default(),
			btree_indexed: Default::default(),
		}
	}
}

impl CommitOverlay {
	fn get_ref(&self, key: &[u8]) -> Option<Option<&Value>> {
		self.indexed.get(key).map(|(_, v)| v.as_ref())
	}

	fn get(&self, key: &[u8]) -> Option<Option<Value>> {
		self.get_ref(key).map(|v| v.cloned())
	}

	fn get_size(&self, key: &[u8]) -> Option<Option<u32>> {
		self.get_ref(key).map(|res| res.as_ref().map(|b| b.len() as u32))
	}

	fn btree_get(&self, key: &[u8]) -> Option<Option<&Value>> {
		self.btree_indexed.get(key).map(|(_, v)| v.as_ref())
	}

	fn btree_next(&self, last_key: &Option<Vec<u8>>, from_seek: bool) -> Option<(Value, Option<Value>)> {
		if let Some(key) = last_key.as_ref() {
			let mut iter = self.btree_indexed.range::<Vec<u8>, _>(key..);
			if let Some((k, (_, v))) = iter.next() {
				if from_seek || k != key {
					return Some((k.clone(), v.clone()));
				}
			} else {
				return None;
			}
			iter.next().map(|(k, (_, v))| (k.clone(), v.clone()))
		} else {
			self.btree_indexed.range::<Vec<u8>, _>(..).next().map(|(k, (_, v))| (k.clone(), v.clone()))
		}
	}
}

#[derive(Default)]
pub struct CommitChangeSet {
	pub(crate) indexed: HashMap<ColId, IndexedChangeSet>,
	pub(crate) btree_indexed: HashMap<ColId, BTreeChangeSet>,
}

pub struct IndexedChangeSet {
	pub col: ColId,
	pub changes: Vec<(Key, Option<Value>)>,
}

impl IndexedChangeSet {
	pub(crate) fn new(col: ColId) -> Self {
		IndexedChangeSet { col, changes: Default::default() }
	}

	fn push(&mut self, key: &[u8], v: Option<Value>, options: &Options) {
		let mut k = Key::default();
		if options.columns[self.col as usize].uniform {
			k.copy_from_slice(&key[0..32]);
		} else {
			let salt = options.salt.as_ref().map(|s| &s[..]).unwrap_or(&[]);
			k.copy_from_slice(blake2_rfc::blake2b::blake2b(32, salt, &key).as_bytes());
		}
		self.changes.push((k, v));
	}

	fn copy_to_overlay(&self, overlay: &mut CommitOverlay, record_id: u64, bytes: &mut usize, options: &Options) {
		let ref_counted = options.columns[self.col as usize].ref_counted;
		for (k, v) in self.changes.iter() {
			*bytes += k.len();
			*bytes += v.as_ref().map_or(0, |v|v.len());
			// Don't add removed ref-counted values to overlay.
			if !ref_counted || v.is_some() {
				overlay.indexed.insert(*k, (record_id, v.clone()));
			}
		}
	}

	fn write_plan(
		&self,
		column: &Column,
		writer: &mut crate::log::LogWriter,
		ops: &mut u64,
		reindex: &mut bool,
	) -> Result<()> {
		for (key, value) in self.changes.iter() {
			let key = TableKey::Partial(*key);
			match column.write_plan(&key, value.as_ref().map(|v| v.as_slice()), writer)? {
				// Reindex has triggered another reindex.
				PlanOutcome::NeedReindex => {
					*reindex = true;
				},
				_ => {},
			}
			*ops += 1;
		}
		Ok(())
	}

	fn clean_overlay(&self, overlay: &mut CommitOverlay, record_id: u64) {
		use std::collections::hash_map::Entry;
		for (key, _) in self.changes.iter() {
			if let Entry::Occupied(e) = overlay.indexed.entry(*key) {
				if e.get().0 == record_id {
					e.remove_entry();
				}
			}
		}
	}
}

/// Verification operation utilities.
pub mod check {
	pub enum CheckDisplay {
		None,
		Full,
		Short(u64),
	}

	pub struct CheckOptions {
		pub column: Option<u8>,
		pub from: Option<u64>,
		pub bound: Option<u64>,
		pub display: CheckDisplay,
	}

	impl CheckOptions {
		pub fn new(
			column: Option<u8>,
			from: Option<u64>,
			bound: Option<u64>,
			display_content: bool,
			truncate_value_display: Option<u64>,
		) -> Self {
			let display = if display_content {
				match truncate_value_display {
					Some(t) => CheckDisplay::Short(t),
					None => CheckDisplay::Full,
				}
			} else {
				CheckDisplay::None
			};
			CheckOptions {
				column,
				from,
				bound,
				display,
			}
		}
	}
}

#[derive(Clone)]
pub enum TestDbTarget {
	// no thread, so data is in commit overlay when testing
	CommitOverlay,
	// log worker run, not the others workers.
	LogOverlay(TestSynch),
	// runing all.
	DbFile(TestSynch),
	// Default run mode
	Standard,
}

impl TestDbTarget {
	pub fn wait(&self) {
		match self {
			TestDbTarget::LogOverlay(condvar)
				| TestDbTarget::DbFile(condvar) => {
					condvar.wait()
				},
			_ => (),
		}
	}

	pub fn notify_one(&self) {
		match self {
			TestDbTarget::LogOverlay(condvar)
				| TestDbTarget::DbFile(condvar) => {
					condvar.notify_one()
				},
			_ => (),
		}
	}
}

#[derive(Clone)]
pub struct TestSynch(Arc<(Mutex<()>, Condvar)>);

impl Default for TestSynch {
	fn default() -> Self {
		TestSynch(Arc::new((Mutex::new(()), Condvar::new())))
	}
}

impl TestSynch {
	pub fn wait(&self) {
		let mut lock = (self.0).0.lock();
		(self.0).1.wait(&mut lock);
	}
	pub fn notify_one(&self) {
		(self.0).1.notify_one();
	}
}

#[cfg(test)]
mod tests {
	use super::{Db, Options, TestDbTarget};
	use tempfile::tempdir;

	#[test]
	fn test_db_open_should_fail() {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		assert!(
			Db::open(&options).is_err(),
			"Database does not exist, so it should fail to open"
		);
		assert!(Db::open(&options).map(|_| ()).unwrap_err().to_string().contains("use open_or_create"));
	}

	#[test]
	fn test_db_open_or_create() {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		assert!(
			Db::open_or_create(&options).is_ok(),
			"New database should be created"
		);
		assert!(
			Db::open(&options).is_ok(),
			"Existing database should be reopened"
		);
	}

	#[test]
	fn test_indexed_btree_1() {
		test_indexed_btree_inner(TestDbTarget::CommitOverlay, false);
		test_indexed_btree_inner(TestDbTarget::LogOverlay(Default::default()), false);
		test_indexed_btree_inner(TestDbTarget::DbFile(Default::default()), false);
		test_indexed_btree_inner(TestDbTarget::Standard, false);
		test_indexed_btree_inner(TestDbTarget::CommitOverlay, true);
		test_indexed_btree_inner(TestDbTarget::LogOverlay(Default::default()), true);
		test_indexed_btree_inner(TestDbTarget::DbFile(Default::default()), true);
		test_indexed_btree_inner(TestDbTarget::Standard, true);
	}
	fn test_indexed_btree_inner(db_test: TestDbTarget, long_key: bool) {
		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = Options::with_columns(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let (key1, key2, key3, key4) = if !long_key {
			(
				b"key1".to_vec(),
				b"key2".to_vec(),
				b"key3".to_vec(),
				b"key4".to_vec(),
			)
		} else {
			let key2 = vec![2; 272];
			let mut key3 = key2.clone();
			key3[271] = 3;
			(
				vec![1; 953],
				key2,
				key3,
				vec![4; 79],
			)
		};

		let db = Db::open_inner(&options, true, false, db_test.clone(), false).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), Some(b"value1".to_vec())),
		]).unwrap();
		db_test.wait();

		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek(&[]).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), None),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		]).unwrap();
		db_test.wait();

		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(db.get(col_nb, &key2).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(col_nb, &key3).unwrap(), Some(b"value3".to_vec()));
		iter.seek(key2.as_slice()).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key2.clone(), b"value2".to_vec())));
		assert_eq!(iter.next().unwrap(), Some((key3.clone(), b"value3".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key2.clone(), Some(b"value2b".to_vec())),
			(col_nb, key4.clone(), Some(b"value4".to_vec())),
			(col_nb, key3.clone(), None),
		]).unwrap();
		db_test.wait();

		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(db.get(col_nb, &key3).unwrap(), None);
		assert_eq!(db.get(col_nb, &key2).unwrap(), Some(b"value2b".to_vec()));
		assert_eq!(db.get(col_nb, &key4).unwrap(), Some(b"value4".to_vec()));
		let mut key22 = key2.clone();
		key22.push(2);
		iter.seek(key22.as_slice()).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key4.clone(), b"value4".to_vec())));
		assert_eq!(iter.next().unwrap(), None);
	}

	#[test]
	fn test_indexed_btree_2() {
		test_indexed_btree_inner_2(TestDbTarget::CommitOverlay);
		test_indexed_btree_inner_2(TestDbTarget::LogOverlay(Default::default()));
	}
	fn test_indexed_btree_inner_2(db_test: TestDbTarget) {
		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = Options::with_columns(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let written = TestDbTarget::DbFile(Default::default());
		let db = Db::open_inner(&options, true, false, written.clone(), false).unwrap();
		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), Some(b"value1".to_vec())),
		]).unwrap();
		written.wait();

		let db = Db::open_inner(&options, true, false, db_test.clone(), true).unwrap();
		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek(&[]).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), None),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		]).unwrap();
		db_test.wait();

		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(db.get(col_nb, &key2).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(col_nb, &key3).unwrap(), Some(b"value3".to_vec()));
		iter.seek(key2.as_slice()).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key2.clone(), b"value2".to_vec())));
		assert_eq!(iter.next().unwrap(), Some((key3.clone(), b"value3".to_vec())));
		assert_eq!(iter.next().unwrap(), None);
	}
}
