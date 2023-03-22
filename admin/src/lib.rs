// Copyright 2021-2022 Parity Technologies (UK) Ltd.
// This file is dual-licensed as Apache-2.0 or MIT.

//! Experimental admin functionality for parity-db.

use std::path::PathBuf;

mod bench;

/// Command line admin client entry point.
/// Uses default column definition.
pub fn run() -> Result<(), String> {
	let cli: Cli = clap::Parser::parse();
	use env_logger::Builder;

	let mut builder = Builder::from_default_env();
	let mut logs = cli.shared().log.clone();
	if logs.is_empty() {
		logs.push("info".to_string());
	}
	builder.parse_filters(logs.as_slice().join(",").as_str());
	builder.init();

	let db_path = cli
		.shared()
		.base_path
		.clone()
		.unwrap_or_else(|| std::env::current_dir().expect("Cannot resolve current dir"));
	let nb_column = cli.shared().columns.unwrap_or(1);
	let mut options = if let Some(metadata) = parity_db::Options::load_metadata(&db_path)
		.map_err(|e| format!("Error resolving metadata: {e:?}"))?
	{
		let mut options = parity_db::Options::with_columns(db_path.as_path(), 0);
		options.columns = metadata.columns;
		options.salt = Some(metadata.salt);
		options
	} else {
		let mut options = parity_db::Options::with_columns(db_path.as_path(), nb_column);
		if cli.subcommand.ordered() {
			for c in options.columns.iter_mut() {
				c.btree_index = true;
			}
		}
		options
	};
	options.sync_wal = !cli.shared().no_sync;
	options.sync_data = !cli.shared().no_sync;
	options.stats = cli.shared().with_stats;
	log::debug!("Options: {:?}, {:?}", cli, options);
	match cli.subcommand {
		SubCommand::Stats(stat) => {
			let db = parity_db::Db::open_read_only(&options)
				.map_err(|e| format!("Invalid db: {e:?}"))?;
			if stat.clear {
				db.clear_stats(stat.column).unwrap();
			} else {
				let mut out = std::io::stdout();
				db.write_stats_text(&mut out, stat.column).unwrap();
			}
		},
		SubCommand::Migrate(args) => {
			use parity_db::Options;
			let dest_meta = Options::load_metadata_file(&args.dest_meta)
				.map_err(|e| format!("Error loading dest metadata: {e:?}"))?
				.ok_or_else(|| "Error opening dest metadata file".to_string())?;

			let dest_columns = dest_meta.columns;

			if args.clear_dest && std::fs::metadata(&args.dest_path).is_ok() {
				std::fs::remove_dir_all(&args.dest_path)
					.map_err(|e| format!("Error removing dest dir: {e:?}"))?;
			}
			std::fs::create_dir_all(&args.dest_path)
				.map_err(|e| format!("Error creating dest dir: {e:?}"))?;

			let mut dest_options = Options::with_columns(&args.dest_path, dest_columns.len() as u8);
			dest_options.columns = dest_columns;
			dest_options.sync_wal = false;
			dest_options.sync_data = false;

			parity_db::migrate(&db_path, dest_options, args.overwrite, &args.force_columns)
				.map_err(|e| format!("Migration error: {e:?}"))?;

			if args.overwrite && std::fs::metadata(&args.dest_path).is_ok() {
				std::fs::remove_dir_all(&args.dest_path)
					.map_err(|e| format!("Error removing dest dir: {e:?}"))?;
			}
		},
		SubCommand::Check(check) => {
			let db = parity_db::Db::open_read_only(&options)
				.map_err(|e| format!("Invalid db: {e:?}"))?;
			let check_param = parity_db::CheckOptions::new(
				check.column,
				check.range_start,
				check.range_end,
				check.display,
				check.display_value_max,
			);
			db.dump(check_param).map_err(|e| format!("Check error: {e:?}"))?;
		},
		SubCommand::Flush(_flush) => {
			let _db = parity_db::Db::open(&options).map_err(|e| format!("Invalid db: {e:?}"))?;
		},
		SubCommand::Stress(bench) => {
			let args = bench.get_args();
			// avoid deleting folders by mistake.
			options.path.push("test_db_stress");
			if options.path.exists() && !args.append {
				std::fs::remove_dir_all(options.path.as_path())
					.map_err(|e| format!("Error clearing stress db: {e:?}"))?;
			}

			let mut db_options = options.clone();
			if args.compress {
				for mut c in &mut db_options.columns {
					c.compression = parity_db::CompressionType::Lz4;
				}
			}
			if args.uniform {
				for mut c in &mut db_options.columns {
					c.uniform = true;
				}
			}

			let db = parity_db::Db::open_or_create(&db_options).unwrap();
			bench::run_internal(args, db);
		},
	}
	Ok(())
}

/// Admin cli command for parity-db.
#[derive(Debug, clap::Parser)]
pub struct Shared {
	/// Specify db base path.
	#[clap(long, short = 'd', value_name = "PATH")]
	pub base_path: Option<PathBuf>,

	/// Do not sync file on each flush.
	#[clap(long)]
	pub no_sync: bool,

	/// Register stat from those admin operations.
	#[clap(long)]
	pub with_stats: bool,

	/// Indicate the number of column, when using
	/// a new or temporary db, defaults to one.
	#[clap(long)]
	pub columns: Option<u8>,

	/// Sets a custom logging filter. Syntax is <target>=<level>, e.g. -lsync=debug.
	///
	/// Log levels (least to most verbose) are error, warn, info, debug, and trace.
	/// By default, all targets log `info`. The global log level can be set with -l<level>.
	#[clap(short = 'l', long, value_name = "LOG_PATTERN")]
	pub log: Vec<String>,
}

/// Admin cli command for parity-db.
#[derive(Debug, clap::Parser)]
pub struct Cli {
	/// Subcommands.
	#[clap(subcommand)]
	pub subcommand: SubCommand,

	/// Enable validator mode.
	///
	/// The node will be started with the authority role and actively
	/// participate in any consensus task that it can (e.g. depending on
	/// availability of local keys).
	#[clap(long)]
	pub validator: bool,
}

#[derive(Debug, clap::Subcommand)]
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

impl SubCommand {
	fn ordered(&self) -> bool {
		if let SubCommand::Stress(arg) = self {
			arg.ordered
		} else {
			false
		}
	}
}

impl Cli {
	fn shared(&self) -> &Shared {
		match &self.subcommand {
			SubCommand::Stats(stats) => &stats.shared,
			SubCommand::Migrate(stats) => &stats.shared,
			SubCommand::Flush(flush) => &flush.shared,
			SubCommand::Check(check) => &check.shared,
			SubCommand::Stress(bench) => &bench.shared,
		}
	}
}

/// Show stats for columns.
#[derive(Debug, clap::Parser)]
pub struct Stats {
	#[clap(flatten)]
	pub shared: Shared,

	/// Only show stat for the given column.
	#[clap(long)]
	pub column: Option<u8>,

	/// Clear current stats.
	#[clap(long)]
	pub clear: bool,
}

/// Migrate db (update version or change column options).
#[derive(Debug, clap::Parser)]
pub struct Migrate {
	#[clap(flatten)]
	pub shared: Shared,

	/// Force migration of given columns, even if
	/// column option are unchanged (eg to repack table).
	#[clap(long)]
	pub force_columns: Vec<u8>,

	/// Overwrite source after each
	/// column processing.
	#[clap(long)]
	pub overwrite: bool,

	/// Clear destination folder before migration.
	#[clap(long)]
	pub clear_dest: bool,

	/// Destination or temporary folder for migration.
	#[clap(long)]
	pub dest_path: PathBuf,

	/// Meta to migrate to.
	#[clap(long)]
	pub dest_meta: PathBuf,
}

/// Run db until all logs are flushed.
#[derive(Debug, clap::Parser)]
pub struct Flush {
	#[clap(flatten)]
	pub shared: Shared,
}

/// Check db.
#[derive(Debug, clap::Parser)]
pub struct Check {
	#[clap(flatten)]
	pub shared: Shared,

	/// Only process a given column.
	#[clap(long)]
	pub column: Option<u8>,

	/// Start range for operation.
	/// Index start chunk in db.
	#[clap(long)]
	pub range_start: Option<u64>,

	/// End range for operation.
	/// Index end chunk in db.
	#[clap(long)]
	pub range_end: Option<u64>,

	/// When active, display parsed index and value content.
	#[clap(long)]
	pub display: bool,

	/// Max length for value to display (when using --display).
	#[clap(long)]
	pub display_value_max: Option<u64>,
}
