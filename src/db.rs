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
	btree::{commit_overlay::BTreeChangeSet, BTreeIterator, BTreeTable},
	column::{hash_key, ColId, Column, IterState, ReindexBatch},
	error::{Error, Result},
	index::PlanOutcome,
	log::{Log, LogAction},
	options::Options,
	Key,
};
use fs2::FileExt;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
use std::sync::{
	atomic::{AtomicBool, AtomicU64, Ordering},
	Arc,
};

// Max size of commit queue. (Keys + Values). If the queue is
// full `commit` will block.
// These are in memory, so we use usize
const MAX_COMMIT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
// Max size of log overlay. If the overlay is full, processing
// of commit queue is blocked.
const MAX_LOG_QUEUE_BYTES: i64 = 128 * 1024 * 1024;
// Minimum size of log file before it is considered full.
const MIN_LOG_SIZE_BYTES: u64 = 64 * 1024 * 1024;
// Number of log files to keep after flush.
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

struct DbInner {
	columns: Vec<Column>,
	options: Options,
	shutdown: AtomicBool,
	log: Log,
	commit_queue: Mutex<CommitQueue>,
	commit_queue_full_cv: Condvar,
	log_worker_wait: WaitCondvar<bool>,
	commit_worker_wait: Arc<WaitCondvar<bool>>,
	// Overlay of most recent values in the commit queue.
	commit_overlay: RwLock<Vec<CommitOverlay>>,
	// This may underflow occasionally, but is bound for 0 eventually.
	log_queue_wait: WaitCondvar<i64>,
	flush_worker_wait: Arc<WaitCondvar<bool>>,
	cleanup_worker_wait: WaitCondvar<bool>,
	last_enacted: AtomicU64,
	next_reindex: AtomicU64,
	bg_err: Mutex<Option<Arc<Error>>>,
	db_version: u32,
	_lock_file: std::fs::File,
}

struct WaitCondvar<S> {
	cv: Condvar,
	work: Mutex<S>,
}

impl<S: Default> WaitCondvar<S> {
	fn new() -> Self {
		WaitCondvar { cv: Condvar::new(), work: Mutex::new(S::default()) }
	}
}

impl WaitCondvar<bool> {
	fn signal(&self) {
		let mut work = self.work.lock();
		*work = true;
		self.cv.notify_one();
	}

	pub fn wait(&self) {
		let mut work = self.work.lock();
		while !*work {
			self.cv.wait(&mut work)
		}
		*work = false;
	}
}

impl DbInner {
	fn open(options: &Options, inner_options: &InternalOptions) -> Result<DbInner> {
		if inner_options.create {
			std::fs::create_dir_all(&options.path)?;
		} else if !options.path.is_dir() {
			return Err(Error::DatabaseNotFound)
		}

		let mut lock_path: std::path::PathBuf = options.path.clone();
		lock_path.push("lock");
		let lock_file = std::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(lock_path.as_path())?;
		if !inner_options.skip_check_lock {
			lock_file.try_lock_exclusive().map_err(Error::Locked)?;
		}

		let metadata = options.load_and_validate_metadata(inner_options.create)?;
		let mut columns = Vec::with_capacity(metadata.columns.len());
		let mut commit_overlay = Vec::with_capacity(metadata.columns.len());
		let log = Log::open(options)?;
		let last_enacted = log.replay_record_id().unwrap_or(2) - 1;
		for c in 0..metadata.columns.len() {
			let column = Column::open(c as ColId, options, &metadata)?;
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
			options,
			shutdown: std::sync::atomic::AtomicBool::new(false),
			log,
			commit_queue: Mutex::new(Default::default()),
			commit_queue_full_cv: Condvar::new(),
			log_worker_wait: WaitCondvar::new(),
			commit_worker_wait: Arc::new(WaitCondvar::new()),
			commit_overlay: RwLock::new(commit_overlay),
			log_queue_wait: WaitCondvar::new(),
			flush_worker_wait: Arc::new(WaitCondvar::new()),
			cleanup_worker_wait: WaitCondvar::new(),
			next_reindex: AtomicU64::new(1),
			last_enacted: AtomicU64::new(last_enacted),
			bg_err: Mutex::new(None),
			db_version: metadata.version,
			_lock_file: lock_file,
		})
	}

	fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		match &self.columns[col as usize] {
			Column::Hash(column) => {
				let key = column.hash_key(key);
				let overlay = self.commit_overlay.read();
				// Check commit overlay first
				if let Some(v) = overlay.get(col as usize).and_then(|o| o.get(&key)) {
					return Ok(v)
				}
				// Go into tables and log overlay.
				let log = self.log.overlays();
				column.get(&key, log)
			},
			Column::Tree(column) => {
				let overlay = self.commit_overlay.read();
				if let Some(l) = overlay.get(col as usize).and_then(|o| o.btree_get(key)) {
					return Ok(l.cloned())
				}
				// We lock log, if btree structure changed while reading that would be an issue.
				let log = self.log.overlays().read();
				column.with_locked(|btree| BTreeTable::get(key, &*log, btree))
			},
		}
	}

	fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		match &self.columns[col as usize] {
			Column::Hash(column) => {
				let key = column.hash_key(key);
				let overlay = self.commit_overlay.read();
				// Check commit overlay first
				if let Some(l) = overlay.get(col as usize).and_then(|o| o.get_size(&key)) {
					return Ok(l)
				}
				// Go into tables and log overlay.
				let log = self.log.overlays();
				column.get_size(&key, log)
			},
			Column::Tree(column) => {
				let overlay = self.commit_overlay.read();
				if let Some(l) = overlay.get(col as usize).and_then(|o| o.btree_get(key)) {
					return Ok(l.map(|v| v.len() as u32))
				}
				let log = self.log.overlays().read();
				let l = column.with_locked(|btree| BTreeTable::get(key, &*log, btree))?;
				Ok(l.map(|v| v.len() as u32))
			},
		}
	}

	fn btree_iter(&self, col: ColId) -> Result<BTreeIterator> {
		match &self.columns[col as usize] {
			Column::Hash(_column) =>
				Err(Error::InvalidConfiguration("Not an indexed column.".to_string())),
			Column::Tree(column) => {
				let log = self.log.overlays();
				BTreeIterator::new(column, col, log, &self.commit_overlay)
			},
		}
	}

	// Commit simply adds the data to the queue and to the overlay and
	// exits as early as possible.
	fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item = (ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		let mut commit: CommitChangeSet = Default::default();
		for (c, k, v) in tx.into_iter() {
			if self.options.columns[c as usize].btree_index {
				commit
					.btree_indexed
					.entry(c)
					.or_insert_with(|| BTreeChangeSet::new(c))
					.push(k.as_ref(), v)
			} else {
				commit.indexed.entry(c).or_insert_with(|| IndexedChangeSet::new(c)).push(
					k.as_ref(),
					v,
					&self.options,
					self.db_version,
				)
			}
		}

		self.commit_raw(commit)
	}

	fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		let mut queue = self.commit_queue.lock();
		if queue.bytes > MAX_COMMIT_QUEUE_BYTES {
			log::debug!(target: "parity-db", "Waiting, queue size={}", queue.bytes);
			self.commit_queue_full_cv.wait(&mut queue);
		}
		{
			let bg_err = self.bg_err.lock();
			if let Some(err) = &*bg_err {
				return Err(Error::Background(err.clone()))
			}
		}

		let mut overlay = self.commit_overlay.write();

		queue.record_id += 1;
		let record_id = queue.record_id + 1;

		let mut bytes = 0;
		for (c, indexed) in &commit.indexed {
			indexed.copy_to_overlay(
				&mut overlay[*c as usize],
				record_id,
				&mut bytes,
				&self.options,
			);
		}

		for (c, iterset) in &commit.btree_indexed {
			iterset.copy_to_overlay(
				&mut overlay[*c as usize].btree_indexed,
				record_id,
				&mut bytes,
				&self.options,
			);
		}

		let commit = Commit { id: record_id, changeset: commit, bytes };

		log::debug!(
			target: "parity-db",
			"Queued commit {}, {} bytes",
			commit.id,
			bytes,
		);
		queue.commits.push_back(commit);
		queue.bytes += bytes;
		self.log_worker_wait.signal();
		Ok(())
	}

	fn process_commits(&self) -> Result<bool> {
		{
			// Wait if the queue is full.
			let mut queue = self.log_queue_wait.work.lock();
			if !self.shutdown.load(Ordering::Relaxed) && *queue > MAX_LOG_QUEUE_BYTES {
				log::debug!(target: "parity-db", "Waiting, log_bytes={}", queue);
				self.log_queue_wait.cv.wait(&mut queue);
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
				if queue.bytes <= MAX_COMMIT_QUEUE_BYTES &&
					(queue.bytes + commit.bytes) > MAX_COMMIT_QUEUE_BYTES
				{
					// Past the waiting threshold.
					log::debug!(
						target: "parity-db",
						"Waking up commit queue worker",
					);
					self.commit_queue_full_cv.notify_all();
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
				key_values.write_plan(
					&self.columns[*c as usize],
					&mut writer,
					&mut ops,
					&mut reindex,
				)?;
			}

			for (c, btree) in commit.changeset.btree_indexed.iter_mut() {
				match &self.columns[*c as usize] {
					Column::Hash(_column) =>
						return Err(Error::InvalidConfiguration(
							"Not an indexed column.".to_string(),
						)),
					Column::Tree(column) => {
						btree.write_plan(column, &mut writer, &mut ops)?;
					},
				}
			}

			// Collect final changes to value tables
			for c in self.columns.iter() {
				c.complete_plan(&mut writer)?;
			}
			let record_id = writer.record_id();
			let l = writer.drain();

			let bytes = {
				let bytes = self.log.end_record(l)?;
				let mut logged_bytes = self.log_queue_wait.work.lock();
				*logged_bytes += bytes as i64;
				self.flush_worker_wait.signal();
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
			let column = if let Column::Hash(c) = column { c } else { continue };
			let ReindexBatch { drop_index, batch } = column.reindex(&self.log)?;
			if !batch.is_empty() || drop_index.is_some() {
				let mut next_reindex = false;
				let mut writer = self.log.begin_record();
				log::debug!(
					target: "parity-db",
					"Creating reindex record {}",
					writer.record_id(),
				);
				for (key, address) in batch.into_iter() {
					if let PlanOutcome::NeedReindex =
						column.write_reindex_plan(&key, address, &mut writer)?
					{
						next_reindex = true
					}
				}
				if let Some(table) = drop_index {
					writer.drop_table(table);
				}
				let record_id = writer.record_id();
				let l = writer.drain();

				let mut logged_bytes = self.log_queue_wait.work.lock();
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
				self.flush_worker_wait.signal();
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
					return Ok(false)
				},
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
						return Ok(false)
					}
					// Validate all records before applying anything
					loop {
						let next = match reader.next() {
							Ok(next) => next,
							Err(e) => {
								log::debug!(target: "parity-db", "Error reading log: {:?}", e);
								std::mem::drop(reader);
								self.log.clear_replay_logs()?;
								return Ok(false)
							},
						};
						match next {
							LogAction::BeginRecord => {
								log::debug!(target: "parity-db", "Unexpected log header");
								std::mem::drop(reader);
								self.log.clear_replay_logs()?;
								return Ok(false)
							},
							LogAction::EndRecord => break,
							LogAction::InsertIndex(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col]
									.validate_plan(LogAction::InsertIndex(insertion), &mut reader)
								{
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_replay_logs()?;
									return Ok(false)
								}
							},
							LogAction::InsertValue(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns[col]
									.validate_plan(LogAction::InsertValue(insertion), &mut reader)
								{
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									std::mem::drop(reader);
									self.log.clear_replay_logs()?;
									return Ok(false)
								}
							},
							LogAction::DropTable(_) => continue,
						}
					}
					reader.reset()?;
					reader.next()?;
				}
				loop {
					match reader.next()? {
						LogAction::BeginRecord =>
							return Err(Error::Corruption("Bad log record".into())),
						LogAction::EndRecord => break,
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
							match &self.columns[id.col() as usize] {
								Column::Hash(col) => {
									col.drop_index(id)?;
									// Check if there's another reindex on the next iteration
									self.start_reindex(reader.record_id());
								},
								Column::Tree(_) => (),
							}
						},
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
					let mut queue = self.log_queue_wait.work.lock();
					if *queue < bytes as i64 {
						log::warn!(
							target: "parity-db",
							"Detected log underflow record {}, {} bytes, {} queued, reindex = {}",
							record_id,
							bytes,
							*queue,
							self.next_reindex.load(Ordering::SeqCst),
						);
					}
					*queue -= bytes as i64;
					if *queue <= MAX_LOG_QUEUE_BYTES &&
						(*queue + bytes as i64) > MAX_LOG_QUEUE_BYTES
					{
						self.log_queue_wait.cv.notify_one();
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
			self.commit_worker_wait.signal();
		}
		if cleanup_next {
			self.cleanup_worker_wait.signal();
		}
		Ok(flush_next)
	}

	fn clean_logs(&self) -> Result<bool> {
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
			while self.enact_logs(true)? {}
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
		self.log_queue_wait.cv.notify_one();
		self.flush_worker_wait.signal();
		self.log_worker_wait.signal();
		self.commit_worker_wait.signal();
		self.cleanup_worker_wait.signal();
	}

	fn kill_logs(&self) -> Result<()> {
		{
			if let Some(err) = self.bg_err.lock().as_ref() {
				// On error the log reader may be left in inconsistent state. So it is important
				// to no attempt any further log enactment.
				log::debug!(target: "parity-db", "Shutdown with error state {}", err);
				return Ok(())
			}
		}
		log::debug!(target: "parity-db", "Processing leftover commits");
		// Finish logged records and proceed to log and enact queued commits.
		while self.enact_logs(false)? {}
		self.flush_logs(0)?;
		while self.process_commits()? {}
		while self.enact_logs(false)? {}
		self.flush_logs(0)?;
		while self.enact_logs(false)? {}
		self.clean_all_logs()?;
		self.log.kill_logs()?;
		if self.options.stats {
			let mut path = self.options.path.clone();
			path.push("stats.txt");
			match std::fs::File::create(path) {
				Ok(file) => {
					let mut writer = std::io::BufWriter::new(file);
					self.collect_stats(&mut writer, None)
				},
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
			let mut err = self.bg_err.lock();
			if err.is_none() {
				*err = Some(Arc::new(e));
				self.shutdown();
			}
			self.commit_queue_full_cv.notify_all();
		}
	}

	fn iter_column_while(&self, c: ColId, f: impl FnMut(IterState) -> bool) -> Result<()> {
		match &self.columns[c as usize] {
			Column::Hash(column) => column.iter_while(&self.log, f),
			Column::Tree(_) => unimplemented!(),
		}
	}
}

pub struct Db {
	inner: Arc<DbInner>,
	commit_thread: Option<std::thread::JoinHandle<()>>,
	flush_thread: Option<std::thread::JoinHandle<()>>,
	log_thread: Option<std::thread::JoinHandle<()>>,
	cleanup_thread: Option<std::thread::JoinHandle<()>>,
	join_on_shutdown: bool,
}

impl Db {
	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Result<Db> {
		let options = Options::with_columns(path, num_columns);
		let inner_options = InternalOptions { create: true, ..Default::default() };
		Self::open_inner(&options, &inner_options)
	}

	/// Open the database with given options.
	pub fn open(options: &Options) -> Result<Db> {
		let inner_options = InternalOptions::default();
		Self::open_inner(options, &inner_options)
	}

	/// Create the database using given options.
	pub fn open_or_create(options: &Options) -> Result<Db> {
		let inner_options = InternalOptions { create: true, ..Default::default() };
		Self::open_inner(options, &inner_options)
	}

	pub fn open_read_only(options: &Options) -> Result<Db> {
		let inner_options = InternalOptions { read_only: true, ..Default::default() };
		Self::open_inner(options, &inner_options)
	}

	fn open_inner(options: &Options, inner_options: &InternalOptions) -> Result<Db> {
		assert!(options.is_valid());
		let mut db = DbInner::open(options, inner_options)?;
		// This needs to be call before log thread: so first reindexing
		// will run in correct state.
		db.replay_all_logs()?;
		let db = Arc::new(db);
		if inner_options.read_only {
			return Ok(Db {
				inner: db,
				commit_thread: None,
				flush_thread: None,
				log_thread: None,
				cleanup_thread: None,
				join_on_shutdown: inner_options.commit_stages.join_on_shutdown(),
			})
		}
		let start_threads =
			matches!(inner_options.commit_stages, EnableCommitPipelineStages::Standard);
		let commit_thread = if start_threads {
			let commit_worker_db = db.clone();
			Some(std::thread::spawn(move || {
				commit_worker_db.store_err(Self::commit_worker(commit_worker_db.clone()))
			}))
		} else {
			None
		};
		let flush_thread = if start_threads {
			let flush_worker_db = db.clone();
			let min_log_size =
				if matches!(inner_options.commit_stages, EnableCommitPipelineStages::DbFile) {
					0
				} else {
					MIN_LOG_SIZE_BYTES
				};
			Some(std::thread::spawn(move || {
				flush_worker_db.store_err(Self::flush_worker(flush_worker_db.clone(), min_log_size))
			}))
		} else {
			None
		};
		let log_thread = if start_threads {
			let log_worker_db = db.clone();
			Some(std::thread::spawn(move || {
				log_worker_db.store_err(Self::log_worker(log_worker_db.clone()))
			}))
		} else {
			None
		};
		let cleanup_thread = if start_threads {
			let cleanup_worker_db = db.clone();
			Some(std::thread::spawn(move || {
				cleanup_worker_db.store_err(Self::cleanup_worker(cleanup_worker_db.clone()))
			}))
		} else {
			None
		};
		Ok(Db {
			inner: db,
			commit_thread,
			flush_thread,
			log_thread,
			cleanup_thread,
			join_on_shutdown: inner_options.commit_stages.join_on_shutdown(),
		})
	}

	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		self.inner.get(col, key)
	}

	pub fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		self.inner.get_size(col, key)
	}

	pub fn iter(&self, col: ColId) -> Result<BTreeIterator> {
		self.inner.btree_iter(col)
	}

	pub fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item = (ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		self.inner.commit(tx)
	}

	pub fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		self.inner.commit_raw(commit)
	}

	pub fn num_columns(&self) -> u8 {
		self.inner.columns.len() as u8
	}

	pub fn iter_column_while(&self, c: ColId, f: impl FnMut(IterState) -> bool) -> Result<()> {
		self.inner.iter_column_while(c, f)
	}

	fn commit_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				db.commit_worker_wait.wait();
			}

			more_work = db.enact_logs(false)?;
		}
		log::debug!(target: "parity-db", "Commit worker shutdown");
		Ok(())
	}

	fn log_worker(db: Arc<DbInner>) -> Result<()> {
		// Start with pending reindex.
		let mut more_work = db.process_reindex()?;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				db.log_worker_wait.wait();
			}

			let more_commits = db.process_commits()?;
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
				db.flush_worker_wait.wait();
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
				db.cleanup_worker_wait.wait();
			}
			more_work = db.clean_logs()?;
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

	pub fn dump(&self, check_param: check::CheckOptions) -> Result<()> {
		if let Some(col) = check_param.column {
			self.inner.columns[col as usize].dump(&self.inner.log, &check_param, col)?;
		} else {
			for (ix, c) in self.inner.columns.iter().enumerate() {
				c.dump(&self.inner.log, &check_param, ix as ColId)?;
			}
		}
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		if self.join_on_shutdown {
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

pub type IndexedCommitOverlay = HashMap<Key, (u64, Option<Value>), crate::IdentityBuildHasher>;
pub type BTreeCommitOverlay = BTreeMap<Vec<u8>, (u64, Option<Value>)>;

pub struct CommitOverlay {
	indexed: IndexedCommitOverlay,
	btree_indexed: BTreeCommitOverlay,
}

impl CommitOverlay {
	fn new() -> Self {
		CommitOverlay { indexed: Default::default(), btree_indexed: Default::default() }
	}

	#[cfg(test)]
	fn is_empty(&self) -> bool {
		self.indexed.is_empty() && self.btree_indexed.is_empty()
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

	pub fn btree_next(
		&self,
		last_key: &Option<Vec<u8>>,
		from_seek: bool,
	) -> Option<(Value, Option<Value>)> {
		if let Some(key) = last_key.as_ref() {
			let mut iter = self.btree_indexed.range::<Vec<u8>, _>(key..);
			if let Some((k, (_, v))) = iter.next() {
				if from_seek || k != key {
					return Some((k.clone(), v.clone()))
				}
			} else {
				return None
			}
			iter.next().map(|(k, (_, v))| (k.clone(), v.clone()))
		} else {
			self.btree_indexed
				.range::<Vec<u8>, _>(..)
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone()))
		}
	}
}

#[derive(Default)]
pub struct CommitChangeSet {
	pub indexed: HashMap<ColId, IndexedChangeSet>,
	pub btree_indexed: HashMap<ColId, BTreeChangeSet>,
}

pub struct IndexedChangeSet {
	pub col: ColId,
	pub changes: Vec<(Key, Option<Value>)>,
}

impl IndexedChangeSet {
	pub fn new(col: ColId) -> Self {
		IndexedChangeSet { col, changes: Default::default() }
	}

	fn push(&mut self, key: &[u8], v: Option<Value>, options: &Options, db_version: u32) {
		let salt = options.salt.unwrap_or_default();
		let k = hash_key(key, &salt, options.columns[self.col as usize].uniform, db_version);
		self.changes.push((k, v));
	}

	fn copy_to_overlay(
		&self,
		overlay: &mut CommitOverlay,
		record_id: u64,
		bytes: &mut usize,
		options: &Options,
	) {
		let ref_counted = options.columns[self.col as usize].ref_counted;
		for (k, v) in self.changes.iter() {
			*bytes += k.len();
			*bytes += v.as_ref().map_or(0, |v| v.len());
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
		let column = match column {
			Column::Hash(column) => column,
			Column::Tree(_) => {
				log::warn!(target: "parity-db", "Skipping unindex commit in indexed column");
				return Ok(())
			},
		};
		for (key, value) in self.changes.iter() {
			if let PlanOutcome::NeedReindex =
				column.write_plan(key, value.as_ref().map(|v| v.as_slice()), writer)?
			{
				// Reindex has triggered another reindex.
				*reindex = true;
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
			CheckOptions { column, from, bound, display }
		}
	}
}

#[derive(Default)]
struct InternalOptions {
	create: bool,
	read_only: bool,
	commit_stages: EnableCommitPipelineStages,
	skip_check_lock: bool,
}

// This is used in tests to disable certain commit stages.
#[derive(Debug, Clone, Copy)]
enum EnableCommitPipelineStages {
	// No threads started, data stays in commit overlay.
	#[allow(dead_code)]
	CommitOverlay,
	// Log worker run, data processed up to the log overlay.
	#[allow(dead_code)]
	LogOverlay,
	// Runing all.
	#[allow(dead_code)]
	DbFile,
	// Default run mode.
	Standard,
}

impl Default for EnableCommitPipelineStages {
	fn default() -> Self {
		EnableCommitPipelineStages::Standard
	}
}

impl EnableCommitPipelineStages {
	#[cfg(test)]
	fn run_stages(&self, db: &Db) {
		let db = &db.inner;
		match self {
			EnableCommitPipelineStages::DbFile | EnableCommitPipelineStages::LogOverlay => {
				while db.process_commits().unwrap() {}
				while db.process_reindex().unwrap() {}
			},
			_ => (),
		}
		match self {
			EnableCommitPipelineStages::DbFile => {
				let _ = db.log.flush_one(0).unwrap();
				let _ = db.log.flush_one(0).unwrap();
				while db.enact_logs(false).unwrap() {}
				let _ = db.log.flush_one(0).unwrap();
				let _ = db.clean_logs().unwrap();
			},
			_ => (),
		}
	}

	#[cfg(test)]
	fn check_empty_overlay(&self, db: &DbInner, col: ColId) -> bool {
		match self {
			EnableCommitPipelineStages::DbFile | EnableCommitPipelineStages::LogOverlay => {
				if let Some(overlay) = db.commit_overlay.read().get(col as usize) {
					if !overlay.is_empty() {
						let mut replayed = 5;
						while !overlay.is_empty() {
							if replayed > 0 {
								replayed -= 1;
								// the signal is triggered just before cleaning the overlay, so
								// we wait a bit.
								// Warning this is still rather flaky and should be ignored
								// or removed.
								std::thread::sleep(std::time::Duration::from_millis(100));
							} else {
								return false
							}
						}
					}
				}
			},
			_ => (),
		}
		true
	}

	fn join_on_shutdown(&self) -> bool {
		matches!(self, EnableCommitPipelineStages::Standard)
	}
}

#[cfg(test)]
mod tests {
	use super::{Db, EnableCommitPipelineStages, InternalOptions, Options};
	use std::collections::BTreeMap;
	use tempfile::tempdir;

	#[test]
	fn test_db_open_should_fail() {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		assert!(matches!(Db::open(&options), Err(crate::Error::DatabaseNotFound)));
	}

	#[test]
	fn test_db_open_fail_then_recursively_create() {
		let tmp = tempdir().unwrap();
		let (db_path_first, db_path_last) = {
			let mut db_path_first = tmp.path().to_owned();
			db_path_first.push("nope");

			let mut db_path_last = db_path_first.to_owned();

			for p in ["does", "not", "yet", "exist"] {
				db_path_last.push(p);
			}

			(db_path_first, db_path_last)
		};

		assert!(
			!db_path_first.exists(),
			"That directory should not have existed at this point (dir: {:?})",
			db_path_first
		);

		let options = Options::with_columns(&db_path_last, 5);
		assert!(matches!(Db::open(&options), Err(crate::Error::DatabaseNotFound)));

		assert!(!db_path_first.exists(), "That directory should remain non-existent. Did the `open(create: false)` nonetheless create a directory? (dir: {:?})", db_path_first);
		assert!(Db::open_or_create(&options).is_ok(), "New database should be created");

		assert!(
			db_path_first.is_dir(),
			"A directory should have been been created (dir: {:?})",
			db_path_first
		);
		assert!(
			db_path_last.is_dir(),
			"A directory should have been been created (dir: {:?})",
			db_path_last
		);
	}

	#[test]
	fn test_db_open_or_create() {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		assert!(Db::open_or_create(&options).is_ok(), "New database should be created");
		assert!(Db::open(&options).is_ok(), "Existing database should be reopened");
	}

	#[test]
	fn test_indexed_keyvalues() {
		test_indexed_keyvalues_inner(EnableCommitPipelineStages::CommitOverlay);
		test_indexed_keyvalues_inner(EnableCommitPipelineStages::LogOverlay);
		test_indexed_keyvalues_inner(EnableCommitPipelineStages::DbFile);
		test_indexed_keyvalues_inner(EnableCommitPipelineStages::Standard);
	}
	fn test_indexed_keyvalues_inner(db_test: EnableCommitPipelineStages) {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		let col_nb = 0;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		inner_options.commit_stages = db_test;
		let db = Db::open_inner(&options, &inner_options).unwrap();
		assert!(db.get(col_nb, key1.as_slice()).unwrap().is_none());

		db.commit(vec![(col_nb, key1.clone(), Some(b"value1".to_vec()))]).unwrap();
		db_test.run_stages(&db);
		assert!(db_test.check_empty_overlay(&db.inner, col_nb));

		assert_eq!(db.get(col_nb, key1.as_slice()).unwrap(), Some(b"value1".to_vec()));

		db.commit(vec![
			(col_nb, key1.clone(), None),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);
		assert!(db_test.check_empty_overlay(&db.inner, col_nb));

		assert!(db.get(col_nb, key1.as_slice()).unwrap().is_none());
		assert_eq!(db.get(col_nb, key2.as_slice()).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(col_nb, key3.as_slice()).unwrap(), Some(b"value3".to_vec()));

		db.commit(vec![
			(col_nb, key2.clone(), Some(b"value2b".to_vec())),
			(col_nb, key3.clone(), None),
		])
		.unwrap();
		db_test.run_stages(&db);
		assert!(db_test.check_empty_overlay(&db.inner, col_nb));

		assert!(db.get(col_nb, key1.as_slice()).unwrap().is_none());
		assert_eq!(db.get(col_nb, key2.as_slice()).unwrap(), Some(b"value2b".to_vec()));
		assert_eq!(db.get(col_nb, key3.as_slice()).unwrap(), None);
	}

	#[test]
	fn test_indexed_overlay_against_backend() {
		let tmp = tempdir().unwrap();
		let options = Options::with_columns(tmp.path(), 5);
		let col_nb = 0;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let db_test = EnableCommitPipelineStages::DbFile;
		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		inner_options.commit_stages = db_test;
		let db = Db::open_inner(&options, &inner_options).unwrap();

		db.commit(vec![
			(col_nb, key1.clone(), Some(b"value1".to_vec())),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);
		std::mem::drop(db);

		// issue with some file reopening when no delay
		std::thread::sleep(std::time::Duration::from_millis(100));

		let db_test = EnableCommitPipelineStages::CommitOverlay;
		let mut inner_options = InternalOptions::default();
		inner_options.create = false;
		inner_options.commit_stages = db_test;
		inner_options.skip_check_lock = true;
		let db = Db::open_inner(&options, &inner_options).unwrap();
		assert_eq!(db.get(col_nb, key1.as_slice()).unwrap(), Some(b"value1".to_vec()));
		assert_eq!(db.get(col_nb, key2.as_slice()).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(col_nb, key3.as_slice()).unwrap(), Some(b"value3".to_vec()));
		db.commit(vec![
			(col_nb, key2.clone(), Some(b"value2b".to_vec())),
			(col_nb, key3.clone(), None),
		])
		.unwrap();
		db_test.run_stages(&db);

		assert_eq!(db.get(col_nb, key1.as_slice()).unwrap(), Some(b"value1".to_vec()));
		assert_eq!(db.get(col_nb, key2.as_slice()).unwrap(), Some(b"value2b".to_vec()));
		assert_eq!(db.get(col_nb, key3.as_slice()).unwrap(), None);
	}

	#[test]
	fn test_indexed_btree_1() {
		test_indexed_btree_inner(EnableCommitPipelineStages::CommitOverlay, false);
		test_indexed_btree_inner(EnableCommitPipelineStages::LogOverlay, false);
		test_indexed_btree_inner(EnableCommitPipelineStages::DbFile, false);
		test_indexed_btree_inner(EnableCommitPipelineStages::Standard, false);
		test_indexed_btree_inner(EnableCommitPipelineStages::CommitOverlay, true);
		test_indexed_btree_inner(EnableCommitPipelineStages::LogOverlay, true);
		test_indexed_btree_inner(EnableCommitPipelineStages::DbFile, true);
		test_indexed_btree_inner(EnableCommitPipelineStages::Standard, true);
	}
	fn test_indexed_btree_inner(db_test: EnableCommitPipelineStages, long_key: bool) {
		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = Options::with_columns(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let (key1, key2, key3, key4) = if !long_key {
			(b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec(), b"key4".to_vec())
		} else {
			let key2 = vec![2; 272];
			let mut key3 = key2.clone();
			key3[271] = 3;
			(vec![1; 953], key2, key3, vec![4; 79])
		};

		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		inner_options.commit_stages = db_test;
		let db = Db::open_inner(&options, &inner_options).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![(col_nb, key1.clone(), Some(b"value1".to_vec()))]).unwrap();
		db_test.run_stages(&db);

		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek(&[]).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), None),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);

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
		])
		.unwrap();
		db_test.run_stages(&db);

		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(db.get(col_nb, &key3).unwrap(), None);
		assert_eq!(db.get(col_nb, &key2).unwrap(), Some(b"value2b".to_vec()));
		assert_eq!(db.get(col_nb, &key4).unwrap(), Some(b"value4".to_vec()));
		let mut key22 = key2.clone();
		key22.push(2);
		iter.seek(key22.as_slice()).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key4, b"value4".to_vec())));
		assert_eq!(iter.next().unwrap(), None);
	}

	#[test]
	fn test_indexed_btree_2() {
		test_indexed_btree_inner_2(EnableCommitPipelineStages::CommitOverlay);
		test_indexed_btree_inner_2(EnableCommitPipelineStages::LogOverlay);
	}
	fn test_indexed_btree_inner_2(db_test: EnableCommitPipelineStages) {
		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = Options::with_columns(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		inner_options.commit_stages = EnableCommitPipelineStages::DbFile;
		let db = Db::open_inner(&options, &inner_options).unwrap();
		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![(col_nb, key1.clone(), Some(b"value1".to_vec()))]).unwrap();
		EnableCommitPipelineStages::DbFile.run_stages(&db);
		std::mem::drop(db);

		// issue with some file reopening when no delay
		std::thread::sleep(std::time::Duration::from_millis(100));

		let mut inner_options = InternalOptions::default();
		inner_options.create = false;
		inner_options.commit_stages = db_test;
		inner_options.skip_check_lock = true;
		let db = Db::open_inner(&options, &inner_options).unwrap();

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek(&[]).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![
			(col_nb, key1.clone(), None),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);

		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(db.get(col_nb, &key2).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(col_nb, &key3).unwrap(), Some(b"value3".to_vec()));
		iter.seek(key2.as_slice()).unwrap();
		assert_eq!(iter.next().unwrap(), Some((key2.clone(), b"value2".to_vec())));
		assert_eq!(iter.next().unwrap(), Some((key3, b"value3".to_vec())));
		assert_eq!(iter.next().unwrap(), None);
	}

	fn test_basic(change_set: &[(Vec<u8>, Option<Vec<u8>>)]) {
		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = Options::with_columns(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;
		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		let db_test = EnableCommitPipelineStages::DbFile;
		inner_options.commit_stages = db_test;
		let db = Db::open_inner(&options, &inner_options).unwrap();

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(iter.next().unwrap(), None);

		db.commit(change_set.iter().map(|(k, v)| (col_nb, k.clone(), v.clone())))
			.unwrap();
		db_test.run_stages(&db);

		let state: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
			change_set.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		for (key, value) in state.iter() {
			assert_eq!(&db.get(col_nb, key).unwrap(), value);
		}
	}

	#[test]
	fn test_random() {
		fdlimit::raise_fd_limit();
		for i in 0..100 {
			test_random_inner(60, 60, i);
			std::thread::sleep(std::time::Duration::from_millis(30));
		}
		for i in 0..50 {
			test_random_inner(20, 60, i);
			std::thread::sleep(std::time::Duration::from_millis(30));
		}
	}
	fn test_random_inner(size: usize, key_size: usize, seed: u64) {
		use rand::{RngCore, SeedableRng};
		let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
		let mut data = Vec::<(Vec<u8>, Option<Vec<u8>>)>::new();
		for i in 0..size {
			let nb_delete: u32 = rng.next_u32(); // should be out of loop, yet it makes alternance insert/delete in some case.
			let nb_delete = (nb_delete as usize % size) / 2;
			let mut key = vec![0u8; key_size];
			rng.fill_bytes(&mut key[..]);
			let value = if i > size - nb_delete {
				let random_key = rng.next_u32();
				let random_key = (random_key % 4) > 0;
				if !random_key {
					key = data[i - size / 2].0.clone();
				}
				None
			} else {
				Some(key.clone())
			};
			let var_keysize = rng.next_u32();
			let var_keysize = var_keysize as usize % (key_size / 2);
			key.truncate(key_size - var_keysize);
			data.push((key, value));
		}
		test_basic(&data[..]);
	}

	#[test]
	fn test_simple() {
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
		]);
		test_basic(&[
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key5".to_vec(), Some(b"value5".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key11".to_vec(), Some(b"value31".to_vec())),
			(b"key12".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key51".to_vec(), Some(b"value31".to_vec())),
			(b"key52".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key31".to_vec(), Some(b"value31".to_vec())),
			(b"key32".to_vec(), Some(b"value32".to_vec())),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key6".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key0".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key2".to_vec(), Some(b"value3".to_vec())),
			(b"key3".to_vec(), Some(b"value4".to_vec())),
			(b"key4".to_vec(), Some(b"value7".to_vec())),
			(b"key5".to_vec(), Some(b"value2".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key4".to_vec(), Some(b"value3".to_vec())),
			(b"key5".to_vec(), Some(b"value4".to_vec())),
			(b"key6".to_vec(), Some(b"value4".to_vec())),
			(b"key7".to_vec(), Some(b"value2".to_vec())),
			(b"key8".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value5".to_vec())),
			(b"key4".to_vec(), Some(b"value3".to_vec())),
			(b"key5".to_vec(), Some(b"value4".to_vec())),
			(b"key7".to_vec(), Some(b"value2".to_vec())),
			(b"key8".to_vec(), Some(b"value1".to_vec())),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
			(b"key3".to_vec(), None),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
			(b"key3".to_vec(), None),
			(b"key2".to_vec(), None),
			(b"key4".to_vec(), None),
		]);
		test_basic(&[
			(b"key5".to_vec(), Some(b"value5".to_vec())),
			(b"key3".to_vec(), Some(b"value3".to_vec())),
			(b"key4".to_vec(), Some(b"value4".to_vec())),
			(b"key2".to_vec(), Some(b"value2".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key5".to_vec(), None),
			(b"key3".to_vec(), None),
			(b"key2".to_vec(), None),
			(b"key4".to_vec(), None),
			(b"key1".to_vec(), None),
		]);
		test_basic(&[
			([5u8; 250].to_vec(), Some(b"value5".to_vec())),
			([5u8; 200].to_vec(), Some(b"value3".to_vec())),
			([5u8; 100].to_vec(), Some(b"value4".to_vec())),
			([5u8; 150].to_vec(), Some(b"value2".to_vec())),
			([5u8; 101].to_vec(), Some(b"value1".to_vec())),
			([5u8; 250].to_vec(), None),
			([5u8; 101].to_vec(), None),
		]);
	}

	#[test]
	fn test_btree_iter() {
		let col_nb = 0;
		let mut data_start = Vec::new();
		for i in 0u8..100 {
			let mut key = b"key0".to_vec();
			key[3] = i;
			let mut value = b"val0".to_vec();
			value[3] = i;
			data_start.push((col_nb, key, Some(value)));
		}
		let mut data_change = Vec::new();
		for i in 0u8..100 {
			let mut key = b"key0".to_vec();
			if i % 2 == 0 {
				key[2] = i;
				let mut value = b"val0".to_vec();
				value[2] = i;
				data_change.push((col_nb, key, Some(value)));
			} else if i % 3 == 0 {
				key[3] = i;
				data_change.push((col_nb, key, None));
			} else {
				key[3] = i;
				let mut value = b"val0".to_vec();
				value[2] = i;
				data_change.push((col_nb, key, Some(value)));
			}
		}

		let start_state: BTreeMap<Vec<u8>, Vec<u8>> =
			data_start.iter().cloned().map(|(_c, k, v)| (k, v.unwrap())).collect();
		let mut end_state: BTreeMap<Vec<u8>, Vec<u8>> = start_state.clone();
		for (_c, k, v) in data_change.iter() {
			if let Some(v) = v {
				end_state.insert(k.clone(), v.clone());
			} else {
				end_state.remove(k);
			}
		}

		for stage in [
			EnableCommitPipelineStages::CommitOverlay,
			EnableCommitPipelineStages::LogOverlay,
			EnableCommitPipelineStages::DbFile,
			EnableCommitPipelineStages::Standard,
		] {
			for i in 0..10 {
				test_btree_iter_inner(
					stage,
					&data_start,
					&data_change,
					&start_state,
					&end_state,
					i * 5,
				);
			}
			let data_start = vec![
				(0, b"key1".to_vec(), Some(b"val1".to_vec())),
				(0, b"key3".to_vec(), Some(b"val3".to_vec())),
			];
			let data_change = vec![(0, b"key2".to_vec(), Some(b"val2".to_vec()))];
			let start_state: BTreeMap<Vec<u8>, Vec<u8>> =
				data_start.iter().cloned().map(|(_c, k, v)| (k, v.unwrap())).collect();
			let mut end_state: BTreeMap<Vec<u8>, Vec<u8>> = start_state.clone();
			for (_c, k, v) in data_change.iter() {
				if let Some(v) = v {
					end_state.insert(k.clone(), v.clone());
				} else {
					end_state.remove(k);
				}
			}
			test_btree_iter_inner(stage, &data_start, &data_change, &start_state, &end_state, 1);
		}
	}
	fn test_btree_iter_inner(
		db_test: EnableCommitPipelineStages,
		data_start: &Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
		data_change: &Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
		start_state: &BTreeMap<Vec<u8>, Vec<u8>>,
		end_state: &BTreeMap<Vec<u8>, Vec<u8>>,
		commit_at: usize,
	) {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 5);
		let col_nb = 0;
		options.columns[col_nb as usize].btree_index = true;
		let mut inner_options = InternalOptions::default();
		inner_options.create = true;
		inner_options.commit_stages = db_test;
		let db = Db::open_inner(&options, &inner_options).unwrap();

		db.commit(data_start.iter().cloned()).unwrap();
		db_test.run_stages(&db);

		let mut iter = db.iter(col_nb).unwrap();
		let mut iter_state = start_state.iter();
		let mut last_key = Vec::new();
		for _ in 0..commit_at {
			let next = iter.next().unwrap();
			if let Some((k, _)) = next.as_ref() {
				last_key = k.clone();
			}
			assert_eq!(iter_state.next(), next.as_ref().map(|(k, v)| (k, v)));
		}

		db.commit(data_change.iter().cloned()).unwrap();
		db_test.run_stages(&db);

		let mut iter_state = end_state.range(last_key.clone()..);
		for _ in commit_at..100 {
			let mut state_next = iter_state.next();
			if let Some((k, _v)) = state_next.as_ref() {
				if *k == &last_key {
					state_next = iter_state.next();
				}
			}
			let iter_next = iter.next().unwrap();
			assert_eq!(state_next, iter_next.as_ref().map(|(k, v)| (k, v)));
		}
	}
}
