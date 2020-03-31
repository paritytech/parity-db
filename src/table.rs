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

use std::convert::TryInto;
use crate::{
	error::Result,
	column::ColId,
	log::{Log, LogReader, LogWriter},
	display::hex,
};

pub const KEY_LEN: usize = 32;

const MAX_AHEAD: usize = 32;
const META_SIZE: isize = 8 * 1024; // typical SSD page size
const EMPTY: Key = [0u8; KEY_LEN];
const MAX_REBALANCE_BATCH: u32 = 65536;

pub type Key = [u8; KEY_LEN];
pub type Value = Vec<u8>;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TableId(u32);

impl TableId {
	pub fn new(col: ColId, entry_size: u16, index_bits: u8) -> TableId {
		TableId(((col as u32) << 24) | ((entry_size as u32) << 8) | index_bits as u32)
	}

	pub fn from_u32(id: u32) -> TableId {
		TableId(id)
	}

	pub fn col(&self) -> ColId {
		(self.0 >> 24) as ColId
	}

	pub fn index_bits(&self) -> ColId {
		(self.0 & 0xff) as ColId
	}

	pub fn entry_size(&self) -> u16 {
		((self.0 >> 8) & 0xffff) as u16
	}

	pub fn file_name(&self) -> String {
		format!("c{:02}_{}_{}", self.col(), self.index_bits(), self.entry_size())
	}

	pub fn total_entries(&self) -> u64 {
		(1u64 << self.index_bits()) + MAX_AHEAD as u64
	}

	pub fn value_size(&self) -> u16 {
		self.entry_size() - KEY_LEN as u16 - 2
	}

	pub fn file_size(&self) -> u64 {
		self.total_entries() * self.entry_size() as u64 + META_SIZE as u64
	}

	pub fn as_u32(&self) -> u32 {
		self.0
	}
}

impl std::fmt::Display for TableId {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "c{:02}_{}_{}", self.col(), self.index_bits(), self.entry_size())
	}
}

pub enum RebalanceProgress {
	InProgress((u64, u64)),
	Inactive,
}

pub enum PlanOutcome {
	Written,
	NeedRebalance,
	Skipped,
}

pub struct Table {
	pub id: TableId,
	map: memmap::MmapMut,
	entries: std::sync::atomic::AtomicU64,
}

impl Table {
	pub fn open_existing(path: &std::path::Path, id: TableId) -> Result<Option<Table>> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = match std::fs::OpenOptions::new().read(true).write(true).open(path) {
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			}
			Err(e) => return Err(e.into()),
			Ok(file) => file,
		};

		log::debug!(target: "parity-db", "Opened existing table {}", id);
		Ok(Some(Self::from_file(file, id)?))
	}

	pub fn create_new(path: &std::path::Path, id: TableId) -> Result<Table> {
		let mut path: std::path::PathBuf = path.into();
		path.push(id.file_name());

		let file = std::fs::OpenOptions::new().write(true).read(true).create_new(true).open(path)?;
		log::debug!(target: "parity-db", "Created new table {}", id);
		Self::from_file(file, id)
	}

	fn from_file(file: std::fs::File, id: TableId) -> Result<Table> {
		//TODO: check for potential overflows on 32-bit platforms
		file.set_len(id.file_size())?;
		let map = unsafe { memmap::MmapMut::map_mut(&file)? };
		Ok(Table {
			id,
			map,
			entries: std::sync::atomic::AtomicU64::new(0),
		})
	}

	fn key_at(&self, index: u64) -> Key {
		let offset = META_SIZE + index as isize * self.id.entry_size() as isize;
		let key = unsafe { *((self.map.as_ptr().offset(offset)) as *const Key) };
		key
	}

	fn value_at(&self, index: u64) -> &[u8] {
		unsafe {
			let mut ptr = self.map.as_ptr();
			let offset = META_SIZE + index as isize * self.id.entry_size() as isize + KEY_LEN as isize;
			ptr = ptr.offset(offset);
			let len = u16::from_le(std::ptr::read(ptr as *const u16));
			ptr = ptr.offset(2);
			std::slice::from_raw_parts(ptr, len as usize)
		}
	}

	fn key_index(&self, key: &Key) -> u64 {
		//Interpret first 64 bits of the key as LE integer
		let key = unsafe { u64::from_be(std::ptr::read_unaligned(key.as_ptr() as *const u64)) };
		key >> (64 - self.id.index_bits())
	}

	pub fn get(&self, key: &Key, log: &Log) -> Option<Value> {
		let mut index = self.key_index(key);
		for _ in 0 .. MAX_AHEAD {
			let target_key = self.logged_key(index, log);
			if target_key == EMPTY {
				return None;
			}
			match target_key.cmp(key) {
				std::cmp::Ordering::Less => (),
				std::cmp::Ordering::Greater => {
					return None;
				},
				std::cmp::Ordering::Equal => return Some(self.logged_value(index, log).to_vec()),
			}
			index += 1;
		}
		None
	}

	fn planned_value<'a>(&'a self, index: u64, log: &'a LogWriter) -> &'a[u8] {
		log.overlay_at(self.id, index)
			.map(|buf| &buf[KEY_LEN+2..])
			.unwrap_or_else(|| self.value_at(index))

	}

	fn planned_key(&self, index: u64, log: &LogWriter) -> Key {
		log.overlay_at(self.id, index)
			.map(|buf| buf[..KEY_LEN].try_into().unwrap())
			.unwrap_or_else(|| self.key_at(index))
	}

	fn logged_value<'a>(&'a self, index: u64, log: &'a Log) -> &'a[u8] {
		log.overlay_at(self.id, index)
			.map(|buf| &buf[KEY_LEN+2..])
			.unwrap_or_else(|| self.value_at(index))

	}

	fn logged_key(&self, index: u64, log: &Log) -> Key {
		log.overlay_at(self.id, index)
			.map(|buf| buf[..KEY_LEN].try_into().unwrap())
			.unwrap_or_else(|| self.key_at(index))
	}

	pub fn write_plan(&self, key: &Key, value: Option<&[u8]>, log: &mut LogWriter, overwrite: bool) -> Result<PlanOutcome> {
		let plan_insertion = |key: &Key, data: &[u8]| -> Vec<u8>  {
			let mut buf =Vec::with_capacity(KEY_LEN + 2 + data.len());
			buf.extend_from_slice(key);
			if *key != EMPTY {
				let size: u16 = data.len() as u16;
				buf.extend_from_slice(&size.to_le_bytes());
				buf.extend_from_slice(data);
			}
			buf
		};

		match value {
			Some(v) => {
				// Find a slot to insert at
				let start_index = self.key_index(key);
				for index in start_index .. start_index + MAX_AHEAD as u64 {
					let target_key = self.planned_key(index, log);
					if target_key == EMPTY {
						log::trace!(
							target: "parity-db",
							"{}: Inserting into empty slot {}: {}",
							self.id,
							index,
							hex(key),
						);
						// Insert new here
						log.insert(self.id, index, plan_insertion(key, v))?;
						self.entries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
						return Ok(PlanOutcome::Written);
					}
					match target_key.cmp(key) {
						std::cmp::Ordering::Less => {
							log::trace!(
								target: "parity-db",
								"{}: Moving past {} for insertion, designated {}, occupied with {}",
								self.id,
								index,
								start_index,
								hex(&target_key),
							);
							continue
						},
						std::cmp::Ordering::Greater => {
							// Evict and insert here
							// Scan ahead for empty space but not too far from start_index.
							// This guarantess each entry is no further than MAX_AHEAD from its
							// designated location.
							let mut relocation_end = index + 1;
							while relocation_end < start_index + MAX_AHEAD as u64
								&& self.planned_key(relocation_end, log) != EMPTY
							{
								log::trace!(
									target: "parity-db",
									"{}: Moving past {} for eviction, designated {}, occupied with {}",
									self.id,
									relocation_end,
									start_index,
									hex(&self.planned_key(relocation_end, log)),
								);
								relocation_end += 1
							}
							if relocation_end == start_index + MAX_AHEAD as u64 {
								// No empty space ahead.
								log::trace!(
									target: "parity-db",
									"{}: No empty spot for eviction, designated {}, inserting {}, checked up to {}: {}",
									self.id,
									start_index,
									index,
									relocation_end,
									hex(key),
								);
								return Ok(PlanOutcome::NeedRebalance);
							}

							log::trace!(
								target: "parity-db",
								"{}: Inserting into occupied slot {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							// Relocate following entries walking backwards
							for i in (index..relocation_end).rev() {
								// TODO: 0-copy here
								let pk = self.planned_key(i, log);
								let insertion = plan_insertion(&pk, &self.planned_value(i, log));
								log.insert(self.id, i + 1, insertion)?;
								log::trace!(
									target: "parity-db",
									"{}: Relocating occupied slot {}->{}, designated {}: {}",
									self.id,
									i,
									i + 1,
									self.key_index(&pk),
									hex(&pk),
								);
							}

							log.insert(self.id, index, plan_insertion(key, v))?;
							self.entries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
							return Ok(PlanOutcome::Written);
						}
						std::cmp::Ordering::Equal if overwrite => {
							// Replace here
							log::trace!(
								target: "parity-db",
								"{}: Replacing {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							log.insert(self.id, index, plan_insertion(key, v))?;
							return Ok(PlanOutcome::Written);
						}
						std::cmp::Ordering::Equal => {
							return Ok(PlanOutcome::Skipped);
						}
					}
				}
				// No empty space
				log::trace!(
					target: "parity-db",
					"{}: No empty spot for insertion, designated {}, checked up to {}: {}",
					self.id,
					start_index,
					start_index + MAX_AHEAD as u64,
					hex(key),
				);
				return Ok(PlanOutcome::NeedRebalance);
			},
			None =>  {
				// Delete
				let start_index = self.key_index(key);
				for index in start_index .. start_index + MAX_AHEAD as u64 {
					let target_key = self.planned_key(index, log);
					if target_key == EMPTY {
						// Does not exist
						return Ok(PlanOutcome::Skipped);
					}
					match target_key.cmp(key) {
						std::cmp::Ordering::Less => continue,
						std::cmp::Ordering::Greater => return Ok(PlanOutcome::Skipped),
						std::cmp::Ordering::Equal => {
							log::trace!(
								target: "parity-db",
								"{}: Deleting {}, designated {}: {}",
								self.id,
								index,
								start_index,
								hex(key),
							);
							let max_entry = self.id.total_entries();
							// Deleting here
							log.insert(self.id, index, plan_insertion(&EMPTY, &[]))?;
							let mut relocation_end = index + 1;
							while relocation_end < max_entry {
								let target_key = self.planned_key(relocation_end, log);
								if target_key == EMPTY || self.key_index(&target_key) >= relocation_end {
									break;
								}
								relocation_end += 1;
							}
							self.entries.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
							// Relocate following entries
							for i in (index + 1)..relocation_end {
								let insertion = {
									let k = self.planned_key(i, log);
									let v = self.planned_value(i, log);
									log::trace!(
										target: "parity-db",
										"{}: Relocating freed slot {}->{}, designated {}: {}",
										self.id,
										i,
										i - 1,
										self.key_index(&k),
										hex(&k),
									);
									plan_insertion(&k, v)
								};
								log.insert(self.id, i - 1, insertion)?;
							}
							log.insert(self.id, relocation_end - 1, plan_insertion(&EMPTY, &[]))?;
							return Ok(PlanOutcome::Written);
						}
					}
				}
				return Ok(PlanOutcome::Skipped);
			}
		}
	}

	pub fn enact_plan(&mut self, index: u64, log: &mut LogReader) -> Result<()> {
		let mut offset = META_SIZE as isize + (index * self.id.entry_size() as u64) as isize;
		let mut key = Key::default();
		log.read(&mut key)?;
		unsafe {
			std::ptr::copy_nonoverlapping(key.as_ptr(), self.map.as_mut_ptr().offset(offset), KEY_LEN);
			if key != EMPTY {
				offset += KEY_LEN as isize;
				let mut s = [0u8; 2];
				log.read(&mut s)?;
				let size = u16::from_le_bytes(s);
				std::ptr::copy_nonoverlapping(s.as_ptr(), self.map.as_mut_ptr().offset(offset), 2);
				offset += 2;
				log.read(&mut self.map[offset as usize..(offset as usize + size as usize)])?;
			}
		}
		log::trace!(target: "parity-db", "{}: Enacted {}", self.id, index);
		Ok(())
	}

	pub fn rebalance_from(&mut self, source: &Table, start_index: u64, log: &mut LogWriter) -> Result<u64> {
		let mut source_index = start_index;
		let mut count = 0;
		log::trace!(target: "parity-db", "{}: Starting rebalance at {}", self.id, source_index);
		while source_index < source.id.total_entries() && count < MAX_REBALANCE_BATCH {
			let source_key = source.planned_key(source_index, log);
			if source_key != EMPTY {
				// TODO: remove allocation here
				// Alternatively make sure record that triggered rebalance is flushed before doing rebalance
				let v = source.planned_value(source_index, log).to_vec();
				match self.write_plan(&source_key, Some(&v), log, false)? {
					PlanOutcome::NeedRebalance => panic!("Table requires double rebalance"),
					_ => {},
				}
				count += 1;
			}
			source_index += 1;
		}
		log::trace!(target: "parity-db", "{}: End rebalance batch {} ({})", self.id, source_index, count);
		Ok(source_index)
	}

	pub fn entries(&self) -> u64 {
		self.entries.load(std::sync::atomic::Ordering::Relaxed)
	}

	pub fn drop(self, path: &std::path::Path) -> Result<()> {
		let mut path: std::path::PathBuf = path.into();
		path.push(self.id.file_name());
		std::mem::drop(self.map);
		std::fs::remove_file(path.as_path())?;
		log::debug!(target: "parity-db", "{}: Dropped table", self.id);
		Ok(())
	}
}


