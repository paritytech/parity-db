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

//! Experimental admin functionality for parity-db.

use std::path::PathBuf;
use structopt::StructOpt;

mod bench;

/// Command line admin client entry point.
/// Uses default column definition.
pub fn run() -> Result<(), String> {

	let cli = Cli::from_args();
	use env_logger::Builder;

	let mut builder = Builder::from_default_env();
	let mut logs = cli.shared().log.clone();
	if logs.len() == 0 {
		logs.push("info".to_string());
	}
	builder.parse_filters(logs.as_slice().join(",").as_str());
	builder.init();

	let db_path = cli.shared().base_path.clone()
		.unwrap_or_else(|| std::env::current_dir().expect("Cannot resolve current dir"));
	let nb_column = cli.shared().columns.unwrap_or(1);
	let mut metadata_path = db_path.clone();
	metadata_path.push("metadata");
	let mut options = if let Some(metadata) = parity_db::Options::load_metadata(&metadata_path)
		.map_err(|e| format!("Error resolving metas: {:?}", e))? {
		let mut options = parity_db::Options::with_columns(db_path.as_path(), 0);
		options.columns = metadata.columns;
		options.salt = metadata.salt;
		options
	} else {
		parity_db::Options::with_columns(db_path.as_path(), nb_column)
	};
	options.sync_wal = !cli.shared().no_sync;
	options.sync_data = !cli.shared().no_sync;
	options.stats = cli.shared().with_stats;
	println!("Options {:?}, {:?}", cli, options);
	match cli.subcommand {
		SubCommand::Stats(stat) => {
			let db = parity_db::Db::open_read_only(&options)
				.map_err(|e| format!("Invalid db: {:?}", e))?;
			if stat.clear {
				db.clear_stats(stat.column.clone());
			} else {
				let mut out = std::io::stdout();
				db.collect_stats(&mut out, stat.column.clone());
			}
		},
		SubCommand::Migrate(args) => {
			use parity_db::Options;
			let dest_meta = Options::load_metadata(&args.dest_meta)
				.map_err(|e| format!("Error loading dest metadata: {:?}", e))?
				.ok_or_else(|| format!("Error opening dest metadata file"))?;

			let dest_columns = dest_meta.columns;

			if args.clear_dest && std::fs::metadata(&args.dest_path).is_ok() {
				std::fs::remove_dir_all(&args.dest_path).map_err(|e| format!("Error removing dest dir: {:?}", e))?;
			}
			std::fs::create_dir_all(&args.dest_path).map_err(|e| format!("Error creating dest dir: {:?}", e))?;

			let mut dest_options = Options::with_columns(&args.dest_path, dest_columns.len() as u8);
			dest_options.columns = dest_columns;
			dest_options.sync_wal = false;
			dest_options.sync_data = false;

			parity_db::migrate(&db_path, dest_options, args.overwrite, &args.force_columns)
				.map_err(|e| format!("Migration error: {:?}", e))?;

			if args.overwrite && std::fs::metadata(&args.dest_path).is_ok() {
				std::fs::remove_dir_all(&args.dest_path).map_err(|e| format!("Error removing dest dir: {:?}", e))?;
			}
		},
		SubCommand::Check(check) => {
			let db = parity_db::Db::open_read_only(&options)
				.map_err(|e| format!("Invalid db: {:?}", e))?;
			if !check.index_value {
				// Note that we should use enum parameter instead.
				return Err("Requires one of the following check flag: --index-value".to_string());
			}
			let check_param = parity_db::CheckOptions::new(
				check.column,
				check.range_start,
				check.range_end,
				check.display,
				check.display_value_max,
			);
			db.check_from_index(check_param)
				.map_err(|e| format!("Check error: {:?}", e))?;
		},
		SubCommand::Flush(_flush) => {
			let _db = parity_db::Db::open(&options)
				.map_err(|e| format!("Invalid db: {:?}", e))?;
		},
		SubCommand::Stress(bench) => {

			let args = bench.get_args();
			// avoid deleting folders by mistake.
			options.path.push("test_db_stress");
			if options.path.exists() && !args.append {
				std::fs::remove_dir_all(options.path.as_path())
					.map_err(|e| format!("Error clearing stress db: {:?}", e))?;
			}

			use crate::bench::BenchDb;
			let db = bench::BenchAdapter::with_options(&options);

			crate::bench::run_internal(args, db);
		},
	}
	Ok(())
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

	/// Indicate the number of column, when using
	/// a new or temporary db, defaults to one.
	#[structopt(long)]
	pub columns: Option<u8>,

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
	/// Migrate db (update version or change column options).
	Migrate(Migrate),
	/// Run db until all logs are flushed.
	Flush(Flush),
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
			SubCommand::Migrate(stats) => {
				&stats.shared
			},
			SubCommand::Flush(flush) => {
				&flush.shared
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

/// Migrate db (update version or change column options).
#[derive(Debug, StructOpt)]
pub struct Migrate {
	#[structopt(flatten)]
	pub shared: Shared,

	/// Force migration of given columns, even if
	/// column option are unchanged (eg to repack table).
	#[structopt(long)]
	pub force_columns: Vec<u8>,

	/// Overwrite source after each
	/// column processing.
	#[structopt(long)]
	pub overwrite: bool,

	/// Clear destination folder before migration.
	#[structopt(long)]
	pub clear_dest: bool,

	/// Destination or temporary folder for migration.
	#[structopt(long)]
	pub dest_path: PathBuf,

	/// Meta to migrate to.
	#[structopt(long)]
	pub dest_meta: PathBuf,
}

/// Run db until all logs are flushed.
#[derive(Debug, StructOpt)]
pub struct Flush {
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

	/// Start range for operation.
	/// Index start chunk in db.
	#[structopt(long)]
	pub range_start: Option<u64>,

	/// End range for operation.
	/// Index end chunk in db.
	#[structopt(long)]
	pub range_end: Option<u64>,

	/// When active, display parsed index and value content.
	#[structopt(long)]
	pub display: bool,

	/// Max length for value to display (when using --display).
	#[structopt(long)]
	pub display_value_max: Option<u64>,
}
