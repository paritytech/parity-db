// Copyright 2021-2021 Parity Technologies (UK) Ltd.
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

//! Admin functionality for parity-db.
//! Experimental, some functionality may not
//! guarantee db durability.

// Note standard substrate db uses:
// NUM_COLUMN 12
// Column 1 (state):
// 
//		state_col.ref_counted = true;
//		state_col.preimage = true;
//		state_col.uniform = true;
//
// Passing parameter to define this can be a bit tedious,
// defining a column config file could be good.
// TODO actually file metadata exists, just need to parse
// it a bit more (currently only validate same conf).

use std::path::PathBuf;

use structopt::StructOpt;

/// Command line admin client entry point.
/// Uses default column definition.
pub fn run() {

	let cli = Cli::from_args();
	use env_logger::Builder;

	let mut builder = Builder::from_default_env();
	let logs = &cli.shared().log;
	if logs.len() > 0 {
		builder.parse_filters(logs.as_slice().join(",").as_str());
	}
	builder.init();

	let db_path = cli.shared().base_path.clone()
		.unwrap_or_else(|| std::env::current_dir().expect("Cannot resolve current dir"));
	let nb_column = cli.shared().nb_columns;
	let mut options = crate::options::Options::with_columns(db_path.as_path(), nb_column);
	options.sync = !cli.shared().no_sync;
	options.stats = cli.shared().with_stats;
	for i in cli.shared().ref_counted.iter() {
		options.columns[*i as usize].ref_counted = true;
	}
	for i in cli.shared().preimage.iter() {
		options.columns[*i as usize].preimage = true;
	}
	for i in cli.shared().uniform.iter() {
		options.columns[*i as usize].uniform = true;
	}
//	let db = crate::db::DbInner
	println!("{:?}, {:?}", cli, options);
	match cli.subcommand {
		SubCommand::Stats(stat) => {
			let db = crate::db::DbInner::open(&options, false)
				.expect("Invalid db");
			if stat.clear {
				db.do_clear_stats(stat.column.clone());
			} else {
				let mut out = std::io::stdout();
				db.do_collect_stats(&mut out, stat.column.clone());
			}
		},
		SubCommand::Compress(stat) => {
			let mut db = crate::db::DbInner::open(&options, false)
				.expect("Invalid db");
			let compression_target = crate::compress::CompressType::from(stat.compression);

			db.migrate_column(stat.column, compression_target).unwrap();
		},
		SubCommand::Run(_run) => {
			crate::db::Db::open(&options, false)
				.expect("Invalid db");
			loop { }
		},
	}
}

/// Admin cli command for parity-db.
#[derive(Debug, StructOpt)]
pub struct Shared {
	/// Specify db base path.
	#[structopt(long, short = "d", value_name = "PATH", parse(from_os_str))]
	pub base_path: Option<PathBuf>,

	/// Do not sync file on each flush.
	#[structopt(long)]
	pub no_sync: bool,

	/// Register stat from those admin operations.
	#[structopt(long)]
	pub with_stats: bool,

	/// Indicate the number of column.
	#[structopt(long)]
	pub nb_columns: u8,

	/// Set following column as counted reference
	#[structopt(long)]
	pub ref_counted: Vec<u8>,

	/// Set following column as preimage. 
	#[structopt(long)]
	pub preimage: Vec<u8>,

	/// Set following column as uniform. 
	#[structopt(long)]
	pub uniform: Vec<u8>,

	/// Sets a custom logging filter. Syntax is <target>=<level>, e.g. -lsync=debug.
	///
	/// Log levels (least to most verbose) are error, warn, info, debug, and trace.
	/// By default, all targets log `info`. The global log level can be set with -l<level>.
	#[structopt(short = "l", long, value_name = "LOG_PATTERN")]
	pub log: Vec<String>,
}

/// Admin cli command for parity-db.
#[derive(Debug, StructOpt)]
pub struct Cli {

	/// Subcommands.
	#[structopt(subcommand)]
	pub subcommand: SubCommand,

	/// Enable validator mode.
	///
	/// The node will be started with the authority role and actively
	/// participate in any consensus task that it can (e.g. depending on
	/// availability of local keys).
	#[structopt(
		long = "validator"
	)]
	pub validator: bool,
}

#[derive(Debug, StructOpt)]
pub enum SubCommand {
	/// Show stats.
	Stats(Stats),
	/// Compress values.
	Compress(Compress),
	/// Run with worker, allows flushing logs.
	Run(Run),
}

impl Cli {
	fn shared(&self) -> &Shared {
		match &self.subcommand {
			SubCommand::Stats(stats) => {
				&stats.shared
			},
			SubCommand::Compress(stats) => {
				&stats.shared
			},
			SubCommand::Run(run) => {
				&run.shared
			},
		}
	}
}

/// Show stats for columns.
#[derive(Debug, StructOpt)]
pub struct Stats {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Only show stat for the given column.
	#[structopt(long)]
	pub column: Option<u8>,

	/// Clear current stats.
	#[structopt(long)]
	pub clear: bool,
}

/// Show stats for columns.
#[derive(Debug, StructOpt)]
pub struct Compress {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Only show stat for the given column.
	#[structopt(long)]
	pub column: Option<u8>,

	/// Compression target number
	/// (see enum variants in code).
	#[structopt(long)]
	pub compression: u8,
}


/// Run with worker, allows flushing logs.
#[derive(Debug, StructOpt)]
pub struct Run {
	#[structopt(flatten)]
	pub shared: Shared,
}