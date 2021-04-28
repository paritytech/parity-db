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
// TODO Passing parameter to define this is be a bit tedious,
// using a existing metadata file could be good.

use std::path::PathBuf;
use structopt::StructOpt;

mod bench;

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
	for enc in cli.shared().comp.iter() {
		let split = enc.find('=').expect("compress should be 'col_index=compress_as_u8'");
		let (a, b) = enc.split_at(split);
		let col_ix: u8 = a.parse().expect("compress should be 'col_index=compress_as_u8'");
		let c: u8 = b[1..].parse().expect("compress should be 'col_index=compress_as_u8'");
		let compress: crate::compress::CompressionType = c.into();
		options.columns[col_ix as usize].compression = compress.into();
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
			let db = crate::db::DbInner::open(&options, false)
				.expect("Invalid db");
			let compression_target = crate::compress::CompressionType::from(stat.compression);
			let compression_threshold = stat.compression_threshold
				.unwrap_or(4096);

			db.migrate_column(stat.column, compression_target, compression_threshold)
				.unwrap();
		},
		SubCommand::Check(check) => {
			let db = crate::db::DbInner::open(&options, false)
				.expect("Invalid db");
			if !check.index_value {
				// TODO use a enum with structopt and all...
				println!("Require one of the following check flag: index_value");
				return;
			}
			let check_param = crate::db::check::CheckParam::new(
				check.column,
				check.range_start,
				check.range_len,
				check.display,
				check.display_value_max,
				check.remove_on_corrupted,
			);
			db.check_from_index(check_param).unwrap();
		},
		SubCommand::Run(_run) => {
			let _db = crate::db::Db::open(&options, false)
				.expect("Invalid db");
			loop {
				std::thread::sleep(std::time::Duration::from_millis(100));
			}
		},
		SubCommand::Stress(bench) => {
			let args = bench.get_args();
			// avoid deleting folders by mistake.
			options.path.push("test_db");
			if options.path.exists() && !args.append {
				std::fs::remove_dir_all(options.path.as_path()).unwrap();
			}

			use db_bench::Db;
			let db = bench::BenchAdapter::with_options(&options);

			db_bench::run_internal(args, db);
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

	/// Define following column compression.
	#[structopt(long)]
	pub comp: Vec<String>,

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
	/// Check db content.
	Check(Check),
	/// Stress tests.
	Stress(bench::Stress),
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
			SubCommand::Check(check) => {
				&check.shared
			},
			SubCommand::Stress(bench) => {
				&bench.shared
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

	/// Compression threshold to use.
	#[structopt(long)]
	pub compression_threshold: Option<u32>,
}

/// Run with worker, allows flushing logs.
#[derive(Debug, StructOpt)]
pub struct Run {
	#[structopt(flatten)]
	pub shared: Shared,
}

/// Check db.
#[derive(Debug, StructOpt)]
pub struct Check {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Only process a given column.
	#[structopt(long)]
	pub column: Option<u8>,

	/// Parse indexes and
	/// lookup values.
	#[structopt(long)]
	pub index_value: bool,

	/// Remove index on corrupted
	/// value.
	/// Db will probably be missing
	/// data: use for debugging only.
	#[structopt(long)]
	pub remove_on_corrupted: bool,

	/// Start range for operation.
	/// Eg index start chunk in db.
	#[structopt(long)]
	pub range_start: Option<u64>,

	/// End range for operation.
	/// Eg number index chunk number.
	#[structopt(long)]
	pub range_len: Option<u64>,

	/// When active, display parsed index and value content.
	#[structopt(long)]
	pub display: bool,

	/// Max length for value to display.
	#[structopt(long)]
	pub display_value_max: Option<u64>,
}
