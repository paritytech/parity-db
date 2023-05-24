// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

pub type NodeAddress = u64;
pub type Children = Vec<NodeAddress>;

#[derive(Debug, PartialEq, Eq)]
pub enum NodeRef {
	New(NewNode),
	Existing(NodeAddress),
}

#[derive(Debug, PartialEq, Eq)]
pub struct NewNode {
	pub data: Vec<u8>,
	pub children: Vec<NodeRef>,
}
