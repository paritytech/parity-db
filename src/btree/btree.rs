// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Btree overlay definition and methods.

use super::*;
use crate::{
	btree::BTreeTable,
	column::Column,
	db::{RcKey, RcValue},
	error::Result,
	log::{LogQuery, LogWriter},
	table::key::TableKeyQuery,
	Operation,
};

#[derive(Debug)]
pub struct BTree {
	pub(super) depth: u32,
	pub(super) root_index: Option<Address>,
	pub(super) record_id: u64,
}

impl BTree {
	pub fn new(root_index: Option<Address>, depth: u32, record_id: u64) -> Self {
		BTree { root_index, depth, record_id }
	}

	pub fn open(values: TablesRef, log: &impl LogQuery, record_id: u64) -> Result<Self> {
		let btree_header = BTreeTable::btree_header(log, values)?;

		let root_index =
			if btree_header.root == NULL_ADDRESS { None } else { Some(btree_header.root) };
		Ok(BTree::new(root_index, btree_header.depth, record_id))
	}

	pub fn write_sorted_changes(
		&mut self,
		mut changes: &[Operation<RcKey, RcValue>],
		btree: TablesRef,
		log: &mut LogWriter,
	) -> Result<()> {
		let mut root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), btree, log)?;
		let changes = &mut changes;

		while !changes.is_empty() {
			match root.change(None, self.depth, changes, btree, log)? {
				(Some((sep, right)), _) => {
					// add one level
					self.depth += 1;
					let left = std::mem::take(&mut root);
					let left_index = self.root_index.take();
					let new_index = BTreeTable::write_node_plan(btree, left, log, left_index)?;
					let new_index = if new_index.is_some() { new_index } else { left_index };
					root.set_child(0, Node::new_child(new_index));
					root.set_child(1, right);
					root.set_separator(0, sep);
				},
				(_, true) =>
					if let Some((node_index, node)) = root.need_remove_root(btree, log)? {
						self.depth -= 1;
						if let Some(index) = self.root_index.take() {
							BTreeTable::write_plan_remove_node(btree, log, index)?;
						}
						self.root_index = node_index;
						root = node;
					},
				_ => (),
			}
			*changes = &changes[1..];
		}

		if root.changed {
			let new_index = BTreeTable::write_node_plan(btree, root, log, self.root_index)?;

			if new_index.is_some() {
				self.root_index = new_index;
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub fn is_balanced(&self, tables: TablesRef, log: &impl LogQuery) -> Result<bool> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), tables, log)?;
		root.is_balanced(tables, log, 0)
	}

	pub fn get(
		&self,
		key: &[u8],
		values: TablesRef,
		log: &impl LogQuery,
	) -> Result<Option<Vec<u8>>> {
		let root = BTree::fetch_root(self.root_index.unwrap_or(NULL_ADDRESS), values, log)?;
		if let Some(address) = root.get(key, values, log)? {
			let key_query = TableKeyQuery::Fetch(None);
			let r = Column::get_value(key_query, address, values, log)?;
			Ok(r.map(|r| r.1))
		} else {
			Ok(None)
		}
	}

	pub fn fetch_root(root: Address, tables: TablesRef, log: &impl LogQuery) -> Result<Node> {
		if root == NULL_ADDRESS {
			Ok(Node::default())
		} else {
			let root = BTreeTable::get_encoded_entry(root, log, tables)?;
			Node::from_encoded(root)
		}
	}
}
