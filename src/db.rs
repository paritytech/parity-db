// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! The database objects is split into `Db` and `DbInner`.
//! `Db` creates shared `DbInner` instance and manages background
//! worker threads that all use the inner object.
//!
//! There are 4 worker threads:
//! log_worker: Processes commit queue and reindexing. For each commit
//! in the queue, log worker creates a write-ahead record using `Log`.
//! Additionally, if there are active reindexing, it creates log records
//! for batches of relocated index entries.
//! flush_worker: Flushes log records to disk by calling `fsync` on the
//! log files.
//! commit_worker: Reads flushed log records and applies operations to the
//! index and value tables.
//! cleanup_worker: Flush tables by calling `fsync`, and cleanup log.
//! Each background worker is signalled with a conditional variable once
//! there is some work to be done.

use crate::{
	btree::{commit_overlay::BTreeChangeSet, BTreeIterator, BTreeTable},
	column::{hash_key, ColId, Column, IterState, ReindexBatch},
	error::{try_io, Error, Result},
	hash::IdentityBuildHasher,
	index::PlanOutcome,
	log::{Log, LogAction},
	options::{Options, CURRENT_VERSION},
	parking_lot::{Condvar, Mutex, RwLock},
	stats::StatSummary,
	ColumnOptions, Key,
};
use fs2::FileExt;
use std::{
	borrow::Borrow,
	collections::{BTreeMap, HashMap, VecDeque},
	ops::Bound,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering},
		Arc,
	},
	thread,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RcValue(Arc<Value>);

pub type RcKey = RcValue;

impl RcValue {
	pub fn value(&self) -> &Value {
		&self.0
	}
}

impl AsRef<[u8]> for RcValue {
	fn as_ref(&self) -> &[u8] {
		self.0.as_ref()
	}
}

impl Borrow<[u8]> for RcValue {
	fn borrow(&self) -> &[u8] {
		self.value().borrow()
	}
}

impl Borrow<Vec<u8>> for RcValue {
	fn borrow(&self) -> &Vec<u8> {
		self.value().borrow()
	}
}

impl From<Value> for RcValue {
	fn from(value: Value) -> Self {
		Self(value.into())
	}
}

#[cfg(test)]
impl<const N: usize> TryFrom<RcValue> for [u8; N] {
	type Error = <[u8; N] as TryFrom<Vec<u8>>>::Error;

	fn try_from(value: RcValue) -> std::result::Result<Self, Self::Error> {
		value.value().clone().try_into()
	}
}

// Commit data passed to `commit`
#[derive(Debug, Default)]
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
#[derive(Debug, Default)]
struct CommitQueue {
	// Log record.
	record_id: u64,
	// Total size of all commits in the queue.
	bytes: usize,
	// FIFO queue.
	commits: VecDeque<Commit>,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
	fn open(options: &Options, opening_mode: OpeningMode) -> Result<DbInner> {
		if opening_mode == OpeningMode::Create {
			try_io!(std::fs::create_dir_all(&options.path));
		} else if !options.path.is_dir() {
			return Err(Error::DatabaseNotFound)
		}

		let mut lock_path: std::path::PathBuf = options.path.clone();
		lock_path.push("lock");
		let lock_file = try_io!(std::fs::OpenOptions::new()
			.create(true)
			.read(true)
			.write(true)
			.open(lock_path.as_path()));
		lock_file.try_lock_exclusive().map_err(Error::Locked)?;

		let metadata = options.load_and_validate_metadata(opening_mode == OpeningMode::Create)?;
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
			shutdown: AtomicBool::new(false),
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
					return Ok(v.map(|i| i.value().clone()))
				}
				// Go into tables and log overlay.
				let log = self.log.overlays();
				column.get(&key, log)
			},
			Column::Tree(column) => {
				let overlay = self.commit_overlay.read();
				if let Some(l) = overlay.get(col as usize).and_then(|o| o.btree_get(key)) {
					return Ok(l.map(|i| i.value().clone()))
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
					return Ok(l.map(|v| v.value().len() as u32))
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
		self.commit_changes(tx.into_iter().map(|(c, k, v)| {
			(
				c,
				match v {
					Some(v) => Operation::Set(k.as_ref().to_vec(), v),
					None => Operation::Dereference(k.as_ref().to_vec()),
				},
			)
		}))
	}

	fn commit_changes<I>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item = (ColId, Operation<Vec<u8>, Vec<u8>>)>,
	{
		let mut commit: CommitChangeSet = Default::default();
		for (col, change) in tx.into_iter() {
			if self.options.columns[col as usize].btree_index {
				commit
					.btree_indexed
					.entry(col)
					.or_insert_with(|| BTreeChangeSet::new(col))
					.push(change)
			} else {
				commit.indexed.entry(col).or_insert_with(|| IndexedChangeSet::new(col)).push(
					change,
					&self.options,
					self.db_version,
				)
			}
		}

		self.commit_raw(commit)
	}

	fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		let mut queue = self.commit_queue.lock();

		#[cfg(any(test, feature = "instrumentation"))]
		let might_wait_because_the_queue_is_full = self.options.with_background_thread;
		#[cfg(not(any(test, feature = "instrumentation")))]
		let might_wait_because_the_queue_is_full = true;
		if might_wait_because_the_queue_is_full && queue.bytes > MAX_COMMIT_QUEUE_BYTES {
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
			)?;
		}

		for (c, iterset) in &commit.btree_indexed {
			iterset.copy_to_overlay(
				&mut overlay[*c as usize].btree_indexed,
				record_id,
				&mut bytes,
				&self.options,
			)?;
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
		#[cfg(any(test, feature = "instrumentation"))]
		let might_wait_because_the_queue_is_full = self.options.with_background_thread;
		#[cfg(not(any(test, feature = "instrumentation")))]
		let might_wait_because_the_queue_is_full = true;
		if might_wait_because_the_queue_is_full {
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
		log::trace!(target: "parity-db", "Scheduled reindex at record {}", record_id);
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
					self.log.clear_replay_logs();
					return Ok(false)
				},
				Err(e) => return Err(e),
			};
			if let Some(mut reader) = reader {
				log::debug!(
					target: "parity-db",
					"Enacting log record {}",
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
						drop(reader);
						self.log.clear_replay_logs();
						return Ok(false)
					}
					// Validate all records before applying anything
					loop {
						let next = match reader.next() {
							Ok(next) => next,
							Err(e) => {
								log::debug!(target: "parity-db", "Error reading log: {:?}", e);
								drop(reader);
								self.log.clear_replay_logs();
								return Ok(false)
							},
						};
						match next {
							LogAction::BeginRecord => {
								log::debug!(target: "parity-db", "Unexpected log header");
								drop(reader);
								self.log.clear_replay_logs();
								return Ok(false)
							},
							LogAction::EndRecord => break,
							LogAction::InsertIndex(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns.get(col).map_or_else(
									|| Err(Error::Corruption(format!("Invalid column id {col}"))),
									|col| {
										col.validate_plan(
											LogAction::InsertIndex(insertion),
											&mut reader,
										)
									},
								) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									drop(reader);
									self.log.clear_replay_logs();
									return Ok(false)
								}
							},
							LogAction::InsertValue(insertion) => {
								let col = insertion.table.col() as usize;
								if let Err(e) = self.columns.get(col).map_or_else(
									|| Err(Error::Corruption(format!("Invalid column id {col}"))),
									|col| {
										col.validate_plan(
											LogAction::InsertValue(insertion),
											&mut reader,
										)
									},
								) {
									log::warn!(target: "parity-db", "Error replaying log: {:?}. Reverting", e);
									drop(reader);
									self.log.clear_replay_logs();
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
		let has_flushed = self.log.flush_one(min_log_size)?;
		if has_flushed {
			self.commit_worker_wait.signal();
		}
		Ok(has_flushed)
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
				self.log.clean_logs(self.log.num_dirty_logs())?;
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
					if let Err(e) = self.write_stats_text(&mut writer, None) {
						log::warn!(target: "parity-db", "Error writing stats file: {:?}", e)
					}
				},
				Err(e) => log::warn!(target: "parity-db", "Error creating stats file: {:?}", e),
			}
		}
		Ok(())
	}

	fn write_stats_text(&self, writer: &mut impl std::io::Write, column: Option<u8>) -> Result<()> {
		if let Some(col) = column {
			self.columns[col as usize].write_stats_text(writer)
		} else {
			for c in self.columns.iter() {
				c.write_stats_text(writer)?;
			}
			Ok(())
		}
	}

	fn clear_stats(&self, column: Option<u8>) -> Result<()> {
		if let Some(col) = column {
			self.columns[col as usize].clear_stats()
		} else {
			for c in self.columns.iter() {
				c.clear_stats()?;
			}
			Ok(())
		}
	}

	fn stats(&self) -> StatSummary {
		StatSummary { columns: self.columns.iter().map(|c| c.stats()).collect() }
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
	commit_thread: Option<thread::JoinHandle<()>>,
	flush_thread: Option<thread::JoinHandle<()>>,
	log_thread: Option<thread::JoinHandle<()>>,
	cleanup_thread: Option<thread::JoinHandle<()>>,
}

impl Db {
	pub fn with_columns(path: &std::path::Path, num_columns: u8) -> Result<Db> {
		let options = Options::with_columns(path, num_columns);
		Self::open_inner(&options, OpeningMode::Create)
	}

	/// Open the database with given options.
	pub fn open(options: &Options) -> Result<Db> {
		Self::open_inner(options, OpeningMode::Write)
	}

	/// Create the database using given options.
	pub fn open_or_create(options: &Options) -> Result<Db> {
		Self::open_inner(options, OpeningMode::Create)
	}

	/// Read the database using given options
	pub fn open_read_only(options: &Options) -> Result<Db> {
		Self::open_inner(options, OpeningMode::ReadOnly)
	}

	fn open_inner(options: &Options, opening_mode: OpeningMode) -> Result<Db> {
		assert!(options.is_valid());
		let mut db = DbInner::open(options, opening_mode)?;
		// This needs to be call before log thread: so first reindexing
		// will run in correct state.
		if let Err(e) = db.replay_all_logs() {
			log::debug!(target: "parity-db", "Error during log replay, doing log cleanup");
			db.log.clean_logs(db.log.num_dirty_logs())?;
			db.log.kill_logs()?;
			return Err(e)
		}
		let db = Arc::new(db);
		#[cfg(any(test, feature = "instrumentation"))]
		let start_threads = opening_mode != OpeningMode::ReadOnly && options.with_background_thread;
		#[cfg(not(any(test, feature = "instrumentation")))]
		let start_threads = opening_mode != OpeningMode::ReadOnly;
		let commit_thread = if start_threads {
			let commit_worker_db = db.clone();
			Some(thread::spawn(move || {
				commit_worker_db.store_err(Self::commit_worker(commit_worker_db.clone()))
			}))
		} else {
			None
		};
		let flush_thread = if start_threads {
			let flush_worker_db = db.clone();
			#[cfg(any(test, feature = "instrumentation"))]
			let min_log_size = if options.always_flush { 0 } else { MIN_LOG_SIZE_BYTES };
			#[cfg(not(any(test, feature = "instrumentation")))]
			let min_log_size = MIN_LOG_SIZE_BYTES;
			Some(thread::spawn(move || {
				flush_worker_db.store_err(Self::flush_worker(flush_worker_db.clone(), min_log_size))
			}))
		} else {
			None
		};
		let log_thread = if start_threads {
			let log_worker_db = db.clone();
			Some(thread::spawn(move || {
				log_worker_db.store_err(Self::log_worker(log_worker_db.clone()))
			}))
		} else {
			None
		};
		let cleanup_thread = if start_threads {
			let cleanup_worker_db = db.clone();
			Some(thread::spawn(move || {
				cleanup_worker_db.store_err(Self::cleanup_worker(cleanup_worker_db.clone()))
			}))
		} else {
			None
		};
		Ok(Db { inner: db, commit_thread, flush_thread, log_thread, cleanup_thread })
	}

	/// Get a value in a specified column by key. Returns `None` if the key does not exist.
	pub fn get(&self, col: ColId, key: &[u8]) -> Result<Option<Value>> {
		self.inner.get(col, key)
	}

	/// Get value size by key. Returns `None` if the key does not exist.
	pub fn get_size(&self, col: ColId, key: &[u8]) -> Result<Option<u32>> {
		self.inner.get_size(col, key)
	}

	/// Iterate over all ordered key-value pairs. Only supported for columns configured with
	/// `btree_indexed`.
	pub fn iter(&self, col: ColId) -> Result<BTreeIterator> {
		self.inner.btree_iter(col)
	}

	/// Commit a set of changes to the database.
	pub fn commit<I, K>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item = (ColId, K, Option<Value>)>,
		K: AsRef<[u8]>,
	{
		self.inner.commit(tx)
	}

	/// Commit a set of changes to the database.
	pub fn commit_changes<I>(&self, tx: I) -> Result<()>
	where
		I: IntoIterator<Item = (ColId, Operation<Vec<u8>, Vec<u8>>)>,
	{
		self.inner.commit_changes(tx)
	}

	pub(crate) fn commit_raw(&self, commit: CommitChangeSet) -> Result<()> {
		self.inner.commit_raw(commit)
	}

	/// Returns the number of columns in the database.
	pub fn num_columns(&self) -> u8 {
		self.inner.columns.len() as u8
	}

	/// Iterate a column and call a function for each value. This is only supported for columns with
	/// `btree_index` set to `false`. Iteration order is unspecified. Note that the
	/// `key` field in the state is the hash of the original key.
	/// Unlinke `get` the iteration may not include changes made in recent `commit` calls.
	pub fn iter_column_while(&self, c: ColId, f: impl FnMut(IterState) -> bool) -> Result<()> {
		self.inner.iter_column_while(c, f)
	}

	fn commit_worker(db: Arc<DbInner>) -> Result<()> {
		let mut more_work = false;
		while !db.shutdown.load(Ordering::SeqCst) || more_work {
			if !more_work {
				db.cleanup_worker_wait.signal();
				if !db.log.has_log_files_to_read() {
					db.commit_worker_wait.wait();
				}
			}

			more_work = db.enact_logs(false)?;
		}
		log::debug!(target: "parity-db", "Commit worker shutdown");
		Ok(())
	}

	fn log_worker(db: Arc<DbInner>) -> Result<()> {
		// Start with pending reindex.
		let mut more_reindex = db.process_reindex()?;
		let mut more_commits = false;
		// Process all commits but allow reindex to be interrupted.
		while !db.shutdown.load(Ordering::SeqCst) || more_commits {
			if !more_commits && !more_reindex {
				db.log_worker_wait.wait();
			}

			more_commits = db.process_commits()?;
			more_reindex = db.process_reindex()?;
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

	pub fn write_stats_text(
		&self,
		writer: &mut impl std::io::Write,
		column: Option<u8>,
	) -> Result<()> {
		self.inner.write_stats_text(writer, column)
	}

	pub fn clear_stats(&self, column: Option<u8>) -> Result<()> {
		self.inner.clear_stats(column)
	}

	/// Print database contents in text form to stderr.
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

	/// Get database statistics.
	pub fn stats(&self) -> StatSummary {
		self.inner.stats()
	}

	/// Add a new column with options specified by `new_column_options`.
	pub fn add_column(options: &mut Options, new_column_options: ColumnOptions) -> Result<()> {
		// We open the DB before to check metadata validity and make sure there are no pending WAL
		// logs.
		let db = Db::open(options)?;
		let salt = db.inner.options.salt;
		drop(db);

		options.columns.push(new_column_options);
		options.write_metadata_with_version(
			&options.path,
			&salt.expect("`salt` is always `Some` after opening the DB; qed"),
			Some(CURRENT_VERSION),
		)?;

		Ok(())
	}

	#[cfg(feature = "instrumentation")]
	pub fn process_reindex(&self) -> Result<()> {
		self.inner.process_reindex()?;
		Ok(())
	}

	#[cfg(feature = "instrumentation")]
	pub fn process_commits(&self) -> Result<()> {
		self.inner.process_commits()?;
		Ok(())
	}

	#[cfg(feature = "instrumentation")]
	pub fn flush_logs(&self) -> Result<()> {
		self.inner.flush_logs(0)?;
		Ok(())
	}

	#[cfg(feature = "instrumentation")]
	pub fn enact_logs(&self) -> Result<()> {
		while self.inner.enact_logs(false)? {}
		Ok(())
	}

	#[cfg(feature = "instrumentation")]
	pub fn clean_logs(&self) -> Result<()> {
		self.inner.clean_logs()?;
		Ok(())
	}
}

impl Drop for Db {
	fn drop(&mut self) {
		self.inner.shutdown();
		if let Some(t) = self.log_thread.take() {
			if let Err(e) = t.join() {
				log::warn!(target: "parity-db", "Log thread shutdown error: {:?}", e);
			}
		}
		if let Some(t) = self.flush_thread.take() {
			if let Err(e) = t.join() {
				log::warn!(target: "parity-db", "Flush thread shutdown error: {:?}", e);
			}
		}
		if let Some(t) = self.commit_thread.take() {
			if let Err(e) = t.join() {
				log::warn!(target: "parity-db", "Commit thread shutdown error: {:?}", e);
			}
		}
		if let Some(t) = self.cleanup_thread.take() {
			if let Err(e) = t.join() {
				log::warn!(target: "parity-db", "Cleanup thread shutdown error: {:?}", e);
			}
		}
		if let Err(e) = self.inner.kill_logs() {
			log::warn!(target: "parity-db", "Shutdown error: {:?}", e);
		}
	}
}

pub type IndexedCommitOverlay = HashMap<Key, (u64, Option<RcValue>), IdentityBuildHasher>;
pub type BTreeCommitOverlay = BTreeMap<RcValue, (u64, Option<RcValue>)>;

#[derive(Debug)]
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
	fn get_ref(&self, key: &[u8]) -> Option<Option<&RcValue>> {
		self.indexed.get(key).map(|(_, v)| v.as_ref())
	}

	fn get(&self, key: &[u8]) -> Option<Option<RcValue>> {
		self.get_ref(key).map(|v| v.cloned())
	}

	fn get_size(&self, key: &[u8]) -> Option<Option<u32>> {
		self.get_ref(key).map(|res| res.as_ref().map(|b| b.value().len() as u32))
	}

	fn btree_get(&self, key: &[u8]) -> Option<Option<&RcValue>> {
		self.btree_indexed.get(key).map(|(_, v)| v.as_ref())
	}

	pub fn btree_next(
		&self,
		last_key: &crate::btree::LastKey,
	) -> Option<(RcValue, Option<RcValue>)> {
		use crate::btree::LastKey;
		match &last_key {
			LastKey::Start => self
				.btree_indexed
				.range::<Vec<u8>, _>(..)
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
			LastKey::End => None,
			LastKey::At(key) => self
				.btree_indexed
				.range::<Vec<u8>, _>((Bound::Excluded(key), Bound::Unbounded))
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
			LastKey::Seeked(key) => self
				.btree_indexed
				.range::<Value, _>(key..)
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
		}
	}

	pub fn btree_prev(
		&self,
		last_key: &crate::btree::LastKey,
	) -> Option<(RcValue, Option<RcValue>)> {
		use crate::btree::LastKey;
		match &last_key {
			LastKey::End => self
				.btree_indexed
				.range::<Vec<u8>, _>(..)
				.rev()
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
			LastKey::Start => None,
			LastKey::At(key) => self
				.btree_indexed
				.range::<Vec<u8>, _>(..key)
				.rev()
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
			LastKey::Seeked(key) => self
				.btree_indexed
				.range::<Vec<u8>, _>(..=key)
				.rev()
				.next()
				.map(|(k, (_, v))| (k.clone(), v.clone())),
		}
	}
}

/// Different operations allowed for a commit.
/// Behavior may differs depending on column configuration.
#[derive(Debug, PartialEq, Eq)]
pub enum Operation<Key, Value> {
	/// Insert or update the value for a given key.
	Set(Key, Value),

	/// Dereference at a given key, resulting in
	/// either removal of a key value or decrement of its
	/// reference count counter.
	Dereference(Key),

	/// Increment the reference count counter of an existing value for a given key.
	/// If no value exists for the key, this operation is skipped.
	Reference(Key),
}

impl<Key: Ord, Value: Eq> PartialOrd<Self> for Operation<Key, Value> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl<Key: Ord, Value: Eq> Ord for Operation<Key, Value> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.key().cmp(other.key())
	}
}

impl<Key, Value> Operation<Key, Value> {
	pub fn key(&self) -> &Key {
		match self {
			Operation::Set(k, _) | Operation::Dereference(k) | Operation::Reference(k) => k,
		}
	}

	pub fn into_key(self) -> Key {
		match self {
			Operation::Set(k, _) | Operation::Dereference(k) | Operation::Reference(k) => k,
		}
	}
}

impl<K: AsRef<[u8]>, Value> Operation<K, Value> {
	pub fn to_key_vec(self) -> Operation<Vec<u8>, Value> {
		match self {
			Operation::Set(k, v) => Operation::Set(k.as_ref().to_vec(), v),
			Operation::Dereference(k) => Operation::Dereference(k.as_ref().to_vec()),
			Operation::Reference(k) => Operation::Reference(k.as_ref().to_vec()),
		}
	}
}

#[derive(Debug, Default)]
pub struct CommitChangeSet {
	pub indexed: HashMap<ColId, IndexedChangeSet>,
	pub btree_indexed: HashMap<ColId, BTreeChangeSet>,
}

#[derive(Debug)]
pub struct IndexedChangeSet {
	pub col: ColId,
	pub changes: Vec<Operation<Key, RcValue>>,
}

impl IndexedChangeSet {
	pub fn new(col: ColId) -> Self {
		IndexedChangeSet { col, changes: Default::default() }
	}

	fn push<K: AsRef<[u8]>>(
		&mut self,
		change: Operation<K, Vec<u8>>,
		options: &Options,
		db_version: u32,
	) {
		let salt = options.salt.unwrap_or_default();
		let hash_key = |key: &[u8]| -> Key {
			hash_key(key, &salt, options.columns[self.col as usize].uniform, db_version)
		};

		self.push_change_hashed(match change {
			Operation::Set(k, v) => Operation::Set(hash_key(k.as_ref()), v.into()),
			Operation::Dereference(k) => Operation::Dereference(hash_key(k.as_ref())),
			Operation::Reference(k) => Operation::Reference(hash_key(k.as_ref())),
		})
	}

	fn push_change_hashed(&mut self, change: Operation<Key, RcValue>) {
		self.changes.push(change);
	}

	fn copy_to_overlay(
		&self,
		overlay: &mut CommitOverlay,
		record_id: u64,
		bytes: &mut usize,
		options: &Options,
	) -> Result<()> {
		let ref_counted = options.columns[self.col as usize].ref_counted;
		for change in self.changes.iter() {
			match &change {
				Operation::Set(k, v) => {
					*bytes += k.len();
					*bytes += v.value().len();
					overlay.indexed.insert(*k, (record_id, Some(v.clone())));
				},
				Operation::Dereference(k) => {
					// Don't add removed ref-counted values to overlay.
					if !ref_counted {
						overlay.indexed.insert(*k, (record_id, None));
					}
				},
				Operation::Reference(..) => {
					// Don't add (we allow remove value in overlay when using rc: some
					// indexing on top of it is expected).
					if !ref_counted {
						return Err(Error::InvalidInput(format!("No Rc for column {}", self.col)))
					}
				},
			}
		}
		Ok(())
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
		for change in self.changes.iter() {
			if let PlanOutcome::NeedReindex = column.write_plan(change, writer)? {
				// Reindex has triggered another reindex.
				*reindex = true;
			}
			*ops += 1;
		}
		Ok(())
	}

	fn clean_overlay(&self, overlay: &mut CommitOverlay, record_id: u64) {
		use std::collections::hash_map::Entry;
		for change in self.changes.iter() {
			match change {
				Operation::Set(k, _) | Operation::Dereference(k) => {
					if let Entry::Occupied(e) = overlay.indexed.entry(*k) {
						if e.get().0 == record_id {
							e.remove_entry();
						}
					}
				},
				Operation::Reference(..) => (),
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

#[derive(Eq, PartialEq, Clone, Copy)]
enum OpeningMode {
	Create,
	Write,
	ReadOnly,
}

#[cfg(test)]
mod tests {
	use super::{Db, Options};
	use crate::{
		column::ColId,
		db::{DbInner, OpeningMode},
		ColumnOptions, Value,
	};
	use rand::Rng;
	use std::{
		collections::{BTreeMap, HashMap, HashSet},
		path::Path,
	};
	use tempfile::tempdir;

	// This is used in tests to disable certain commit stages.
	#[derive(Eq, PartialEq, Debug, Clone, Copy)]
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

	impl EnableCommitPipelineStages {
		fn options(&self, path: &Path, num_columns: u8) -> Options {
			Options {
				path: path.into(),
				sync_wal: true,
				sync_data: true,
				stats: true,
				salt: None,
				columns: (0..num_columns).map(|_| Default::default()).collect(),
				compression_threshold: HashMap::new(),
				with_background_thread: *self == Self::Standard,
				always_flush: *self == Self::DbFile,
			}
		}

		fn run_stages(&self, db: &Db) {
			let db = &db.inner;
			if *self == EnableCommitPipelineStages::DbFile ||
				*self == EnableCommitPipelineStages::LogOverlay
			{
				while db.process_commits().unwrap() {}
				while db.process_reindex().unwrap() {}
			}
			if *self == EnableCommitPipelineStages::DbFile {
				let _ = db.log.flush_one(0).unwrap();
				while db.enact_logs(false).unwrap() {}
				let _ = db.clean_logs().unwrap();
			}
		}

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
	}

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
			"That directory should not have existed at this point (dir: {db_path_first:?})"
		);

		let options = Options::with_columns(&db_path_last, 5);
		assert!(matches!(Db::open(&options), Err(crate::Error::DatabaseNotFound)));

		assert!(!db_path_first.exists(), "That directory should remain non-existent. Did the `open(create: false)` nonetheless create a directory? (dir: {db_path_first:?})");
		assert!(Db::open_or_create(&options).is_ok(), "New database should be created");

		assert!(
			db_path_first.is_dir(),
			"A directory should have been been created (dir: {db_path_first:?})"
		);
		assert!(
			db_path_last.is_dir(),
			"A directory should have been been created (dir: {db_path_last:?})"
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
		let options = db_test.options(tmp.path(), 5);
		let col_nb = 0;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();
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
		let db_test = EnableCommitPipelineStages::DbFile;
		let options = db_test.options(tmp.path(), 5);
		let col_nb = 0;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		db.commit(vec![
			(col_nb, key1.clone(), Some(b"value1".to_vec())),
			(col_nb, key2.clone(), Some(b"value2".to_vec())),
			(col_nb, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);
		drop(db);

		// issue with some file reopening when no delay
		std::thread::sleep(std::time::Duration::from_millis(100));

		let db_test = EnableCommitPipelineStages::CommitOverlay;
		let options = db_test.options(tmp.path(), 5);
		let db = Db::open_inner(&options, OpeningMode::Write).unwrap();
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
	fn test_add_column() {
		let tmp = tempdir().unwrap();
		let db_test = EnableCommitPipelineStages::DbFile;
		let mut options = db_test.options(tmp.path(), 1);
		options.salt = Some(options.salt.unwrap_or_default());

		let old_col_id = 0;
		let new_col_id = 1;
		let new_col_indexed_id = 2;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		db.commit(vec![
			(old_col_id, key1.clone(), Some(b"value1".to_vec())),
			(old_col_id, key2.clone(), Some(b"value2".to_vec())),
			(old_col_id, key3.clone(), Some(b"value3".to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);

		drop(db);

		Db::add_column(&mut options, ColumnOptions { btree_index: false, ..Default::default() })
			.unwrap();

		Db::add_column(&mut options, ColumnOptions { btree_index: true, ..Default::default() })
			.unwrap();

		let mut options = db_test.options(tmp.path(), 3);
		options.columns[new_col_indexed_id as usize].btree_index = true;

		let db_test = EnableCommitPipelineStages::DbFile;
		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		// Expected number of columns
		assert_eq!(db.num_columns(), 3);

		let new_key1 = b"abcdef".to_vec();
		let new_key2 = b"123456".to_vec();

		// Write to new columns.
		db.commit(vec![
			(new_col_id, new_key1.clone(), Some(new_key1.to_vec())),
			(new_col_id, new_key2.clone(), Some(new_key2.to_vec())),
			(new_col_indexed_id, new_key1.clone(), Some(new_key1.to_vec())),
			(new_col_indexed_id, new_key2.clone(), Some(new_key2.to_vec())),
		])
		.unwrap();
		db_test.run_stages(&db);

		drop(db);

		// Reopen DB and fetch all keys we inserted.
		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		assert_eq!(db.get(old_col_id, key1.as_slice()).unwrap(), Some(b"value1".to_vec()));
		assert_eq!(db.get(old_col_id, key2.as_slice()).unwrap(), Some(b"value2".to_vec()));
		assert_eq!(db.get(old_col_id, key3.as_slice()).unwrap(), Some(b"value3".to_vec()));

		// Fetch from new columns
		assert_eq!(db.get(new_col_id, new_key1.as_slice()).unwrap(), Some(new_key1.to_vec()));
		assert_eq!(db.get(new_col_id, new_key2.as_slice()).unwrap(), Some(new_key2.to_vec()));
		assert_eq!(
			db.get(new_col_indexed_id, new_key1.as_slice()).unwrap(),
			Some(new_key1.to_vec())
		);
		assert_eq!(
			db.get(new_col_indexed_id, new_key2.as_slice()).unwrap(),
			Some(new_key2.to_vec())
		);
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
		let mut options = db_test.options(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let (key1, key2, key3, key4) = if !long_key {
			(b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec(), b"key4".to_vec())
		} else {
			let key2 = vec![2; 272];
			let mut key3 = key2.clone();
			key3[271] = 3;
			(vec![1; 953], key2, key3, vec![4; 79])
		};

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(iter.next().unwrap(), None);
		assert_eq!(iter.prev().unwrap(), None);

		db.commit(vec![(col_nb, key1.clone(), Some(b"value1".to_vec()))]).unwrap();
		db_test.run_stages(&db);

		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek_to_first().unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);
		assert_eq!(iter.prev().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.prev().unwrap(), None);
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.next().unwrap(), None);

		iter.seek_to_first().unwrap();
		assert_eq!(iter.next().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.prev().unwrap(), None);

		iter.seek(&[0xff]).unwrap();
		assert_eq!(iter.prev().unwrap(), Some((key1.clone(), b"value1".to_vec())));
		assert_eq!(iter.prev().unwrap(), None);

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

		iter.seek(key3.as_slice()).unwrap();
		assert_eq!(iter.prev().unwrap(), Some((key3.clone(), b"value3".to_vec())));
		assert_eq!(iter.prev().unwrap(), Some((key2.clone(), b"value2".to_vec())));
		assert_eq!(iter.prev().unwrap(), None);

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
		let mut options = db_test.options(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let key1 = b"key1".to_vec();
		let key2 = b"key2".to_vec();
		let key3 = b"key3".to_vec();

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();
		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), None);
		assert_eq!(iter.next().unwrap(), None);

		db.commit(vec![(col_nb, key1.clone(), Some(b"value1".to_vec()))]).unwrap();
		EnableCommitPipelineStages::DbFile.run_stages(&db);
		drop(db);

		// issue with some file reopening when no delay
		std::thread::sleep(std::time::Duration::from_millis(100));

		let db = Db::open_inner(&options, OpeningMode::Write).unwrap();

		let mut iter = db.iter(col_nb).unwrap();
		assert_eq!(db.get(col_nb, &key1).unwrap(), Some(b"value1".to_vec()));
		iter.seek_to_first().unwrap();
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

		iter.seek_to_last().unwrap();
		assert_eq!(iter.prev().unwrap(), Some((key3, b"value3".to_vec())));
		assert_eq!(iter.prev().unwrap(), Some((key2.clone(), b"value2".to_vec())));
		assert_eq!(iter.prev().unwrap(), None);
	}

	#[test]
	fn test_indexed_btree_3() {
		test_indexed_btree_inner_3(EnableCommitPipelineStages::CommitOverlay);
		test_indexed_btree_inner_3(EnableCommitPipelineStages::LogOverlay);
		test_indexed_btree_inner_3(EnableCommitPipelineStages::DbFile);
		test_indexed_btree_inner_3(EnableCommitPipelineStages::Standard);
	}

	fn test_indexed_btree_inner_3(db_test: EnableCommitPipelineStages) {
		use rand::SeedableRng;

		use std::collections::BTreeSet;

		let mut rng = rand::rngs::SmallRng::seed_from_u64(0);

		let tmp = tempdir().unwrap();
		let col_nb = 0u8;
		let mut options = db_test.options(tmp.path(), 5);
		options.columns[col_nb as usize].btree_index = true;

		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		db.commit(
			(0u64..1024)
				.map(|i| (0, i.to_be_bytes().to_vec(), Some(i.to_be_bytes().to_vec())))
				.chain((0u64..1024).step_by(2).map(|i| (0, i.to_be_bytes().to_vec(), None))),
		)
		.unwrap();
		let expected = (0u64..1024).filter(|i| i % 2 == 1).collect::<BTreeSet<_>>();
		let mut iter = db.iter(0).unwrap();

		for _ in 0..100 {
			let at = rng.gen_range(0u64..=1024);
			iter.seek(&at.to_be_bytes()).unwrap();

			let mut prev_run: bool = rng.gen();
			let at = if prev_run {
				let take = rng.gen_range(1..100);
				let got = std::iter::from_fn(|| iter.next().unwrap())
					.map(|(k, _)| u64::from_be_bytes(k.try_into().unwrap()))
					.take(take)
					.collect::<Vec<_>>();
				let expected = expected.range(at..).take(take).copied().collect::<Vec<_>>();
				assert_eq!(got, expected);
				if got.is_empty() {
					prev_run = false;
				}
				if got.len() < take {
					prev_run = false;
				}
				expected.last().copied().unwrap_or(at)
			} else {
				at
			};

			let at = {
				let take = rng.gen_range(1..100);
				let got = std::iter::from_fn(|| iter.prev().unwrap())
					.map(|(k, _)| u64::from_be_bytes(k.try_into().unwrap()))
					.take(take)
					.collect::<Vec<_>>();
				let expected = if prev_run {
					expected.range(..at).rev().take(take).copied().collect::<Vec<_>>()
				} else {
					expected.range(..=at).rev().take(take).copied().collect::<Vec<_>>()
				};
				assert_eq!(got, expected);
				prev_run = !got.is_empty();
				if take > got.len() {
					prev_run = false;
				}
				expected.last().copied().unwrap_or(at)
			};

			let take = rng.gen_range(1..100);
			let mut got = std::iter::from_fn(|| iter.next().unwrap())
				.map(|(k, _)| u64::from_be_bytes(k.try_into().unwrap()))
				.take(take)
				.collect::<Vec<_>>();
			let mut expected = expected.range(at..).take(take).copied().collect::<Vec<_>>();
			if prev_run {
				expected = expected.split_off(1);
				if got.len() == take {
					got.pop();
				}
			}
			assert_eq!(got, expected);
		}

		let take = rng.gen_range(20..100);
		iter.seek_to_last().unwrap();
		let got = std::iter::from_fn(|| iter.prev().unwrap())
			.map(|(k, _)| u64::from_be_bytes(k.try_into().unwrap()))
			.take(take)
			.collect::<Vec<_>>();
		let expected = expected.iter().rev().take(take).copied().collect::<Vec<_>>();
		assert_eq!(got, expected);
	}

	fn test_basic(change_set: &[(Vec<u8>, Option<Vec<u8>>)]) {
		test_basic_inner(change_set, false, false);
		test_basic_inner(change_set, false, true);
		test_basic_inner(change_set, true, false);
		test_basic_inner(change_set, true, true);
	}

	fn test_basic_inner(
		change_set: &[(Vec<u8>, Option<Vec<u8>>)],
		btree_index: bool,
		ref_counted: bool,
	) {
		let tmp = tempdir().unwrap();
		let col_nb = 1u8;
		let db_test = EnableCommitPipelineStages::DbFile;
		let mut options = db_test.options(tmp.path(), 2);
		options.columns[col_nb as usize].btree_index = btree_index;
		options.columns[col_nb as usize].ref_counted = ref_counted;
		options.columns[col_nb as usize].preimage = ref_counted;
		// ref counted and commit overlay currently don't support removal
		assert!(!(ref_counted && db_test == EnableCommitPipelineStages::CommitOverlay));
		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		let iter = btree_index.then(|| db.iter(col_nb).unwrap());
		assert_eq!(iter.and_then(|mut i| i.next().unwrap()), None);

		db.commit(change_set.iter().map(|(k, v)| (col_nb, k.clone(), v.clone())))
			.unwrap();
		db_test.run_stages(&db);

		let mut keys = HashSet::new();
		let mut expected_count: u64 = 0;
		for (k, v) in change_set.iter() {
			if v.is_some() {
				if keys.insert(k) {
					expected_count += 1;
				}
			} else if keys.remove(k) {
				expected_count -= 1;
			}
		}
		if ref_counted {
			let mut state: BTreeMap<Vec<u8>, Option<(Vec<u8>, usize)>> = Default::default();
			for (k, v) in change_set.iter() {
				let mut remove = false;
				let mut insert = false;
				match state.get_mut(k) {
					Some(Some((_, counter))) =>
						if v.is_some() {
							*counter += 1;
						} else if *counter == 1 {
							remove = true;
						} else {
							*counter -= 1;
						},
					Some(None) | None =>
						if v.is_some() {
							insert = true;
						},
				}
				if insert {
					state.insert(k.clone(), v.clone().map(|v| (v, 1)));
				}
				if remove {
					state.remove(k);
				}
			}
			for (key, value) in state {
				assert_eq!(db.get(col_nb, &key).unwrap(), value.map(|v| v.0));
			}
		} else {
			let stats = db.stats();
			// btree do not have stats implemented
			if let Some(stats) = stats.columns[col_nb as usize].as_ref() {
				assert_eq!(stats.total_values, expected_count);
			}

			let state: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
				change_set.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
			for (key, value) in state.iter() {
				assert_eq!(&db.get(col_nb, key).unwrap(), value);
			}
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
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key1".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key1".to_vec(), None),
			(b"key1".to_vec(), None),
		]);
		test_basic(&[
			(b"key1".to_vec(), Some(b"value1".to_vec())),
			(b"key1".to_vec(), Some(b"value2".to_vec())),
		]);
		test_basic(&[(b"key1".to_vec(), Some(b"value1".to_vec()))]);
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
		let mut end_state = start_state.clone();
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
			let mut end_state = start_state.clone();
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
		data_start: &[(u8, Vec<u8>, Option<Value>)],
		data_change: &[(u8, Vec<u8>, Option<Value>)],
		start_state: &BTreeMap<Vec<u8>, Vec<u8>>,
		end_state: &BTreeMap<Vec<u8>, Vec<u8>>,
		commit_at: usize,
	) {
		let tmp = tempdir().unwrap();
		let mut options = db_test.options(tmp.path(), 5);
		let col_nb = 0;
		options.columns[col_nb as usize].btree_index = true;
		let db = Db::open_inner(&options, OpeningMode::Create).unwrap();

		db.commit(data_start.iter().cloned()).unwrap();
		db_test.run_stages(&db);

		let mut iter = db.iter(col_nb).unwrap();
		let mut iter_state = start_state.iter();
		let mut last_key = Value::new();
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
		let mut iter_state_rev = end_state.iter().rev();
		let mut iter = db.iter(col_nb).unwrap();
		iter.seek_to_last().unwrap();
		for _ in 0..100 {
			let next = iter.prev().unwrap();
			assert_eq!(iter_state_rev.next(), next.as_ref().map(|(k, v)| (k, v)));
		}
	}

	#[cfg(feature = "instrumentation")]
	#[test]
	fn test_recover_from_log_on_error() {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.always_flush = true;
		options.with_background_thread = false;

		// We do 2 commits and we fail while enacting the second one
		{
			let db = Db::open_or_create(&options).unwrap();
			db.commit::<_, Vec<u8>>(vec![(0, vec![0], Some(vec![0]))]).unwrap();
			db.process_commits().unwrap();
			db.flush_logs().unwrap();
			db.enact_logs().unwrap();
			db.commit::<_, Vec<u8>>(vec![(0, vec![1], Some(vec![1]))]).unwrap();
			db.process_commits().unwrap();
			db.flush_logs().unwrap();
			crate::set_number_of_allowed_io_operations(4);

			// Set the background error explicitly as background threads are disabled in tests.
			let err = db.enact_logs();
			assert!(err.is_err());
			db.inner.store_err(err);
			crate::set_number_of_allowed_io_operations(usize::MAX);
		}

		// Open the databases and check that both values are there.
		{
			let db = Db::open(&options).unwrap();
			assert_eq!(db.get(0, &[0]).unwrap(), Some(vec![0]));
			assert_eq!(db.get(0, &[1]).unwrap(), Some(vec![1]));
		}
	}

	#[cfg(feature = "instrumentation")]
	#[test]
	fn test_partial_log_recovery() {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.columns[0].btree_index = true;
		options.always_flush = true;
		options.with_background_thread = false;

		// We do 2 commits and we fail while writing the second one
		{
			let db = Db::open_or_create(&options).unwrap();
			db.commit::<_, Vec<u8>>(vec![(0, vec![0], Some(vec![0]))]).unwrap();
			db.process_commits().unwrap();
			db.commit::<_, Vec<u8>>(vec![(0, vec![1], Some(vec![1]))]).unwrap();
			crate::set_number_of_allowed_io_operations(4);
			assert!(db.process_commits().is_err());
			crate::set_number_of_allowed_io_operations(usize::MAX);
			db.flush_logs().unwrap();
		}

		// We open a first time, the first value is there
		{
			let db = Db::open(&options).unwrap();
			assert_eq!(db.get(0, &[0]).unwrap(), Some(vec![0]));
		}

		// We open a second time, the first value should be still there
		{
			let db = Db::open(&options).unwrap();
			assert!(db.get(0, &[0]).unwrap().is_some());
		}
	}

	#[cfg(feature = "instrumentation")]
	#[test]
	fn test_continue_reindex() {
		let _ = env_logger::try_init();
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.columns[0].preimage = true;
		options.columns[0].uniform = true;
		options.always_flush = true;
		options.with_background_thread = false;
		options.salt = Some(Default::default());

		{
			// Force a reindex by committing more than 64 values with the same 16 bit prefix
			let db = Db::open_or_create(&options).unwrap();
			let commit: Vec<_> = (0..65u32)
				.map(|index| {
					let mut key = [0u8; 32];
					key[2] = (index as u8) << 1;
					(0, key.to_vec(), Some(vec![index as u8]))
				})
				.collect();
			db.commit(commit).unwrap();

			db.process_commits().unwrap();
			db.flush_logs().unwrap();
			db.enact_logs().unwrap();
			// i16 now contains 64 values and i17 contains a single value that did not fit

			// Simulate interrupted reindex by processing it first and then restoring the old index
			// file. Make a copy of the index file first.
			std::fs::copy(tmp.path().join("index_00_16"), tmp.path().join("index_00_16.bak"))
				.unwrap();
			db.process_reindex().unwrap();
			db.flush_logs().unwrap();
			db.enact_logs().unwrap();
			db.clean_logs().unwrap();
			std::fs::rename(tmp.path().join("index_00_16.bak"), tmp.path().join("index_00_16"))
				.unwrap();
		}

		// Reopen the database which should load the reindex.
		{
			let db = Db::open(&options).unwrap();
			db.process_reindex().unwrap();
			let mut entries = 0;
			db.iter_column_while(0, |_| {
				entries += 1;
				true
			})
			.unwrap();

			assert_eq!(entries, 65);
			assert_eq!(db.inner.columns[0].index_bits(), Some(17));
		}
	}
}
