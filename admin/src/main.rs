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

//! Command line admin client for parity-db.
//! Experimental, some functionality may not
//! guarantee db durability.

#[cfg_attr(any(target_os = "linux", target_os = "macos"),  global_allocator)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
	fdlimit::raise_fd_limit();

	parity_db::admin::run()
}
/*
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

pub use parity_db::{Options, Db};

const USAGE: &str = "
Usage: migrate source_dir dest_meta dest_path [options]

Options:
	--clear-dest           Clear dest dir before attempting migration.
";

#[derive(Clone)]
struct Args {
	source: String,
	dest: String,
	dest_meta: String,
	clear_dest: bool,
}

fn parse<'a, I, T>(mut iter: I) -> T
where
	I: Iterator<Item = &'a str>,
	T: std::str::FromStr,
	<T as std::str::FromStr>::Err: std::fmt::Debug,
{
	iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
	fn parse() -> Args {
		let mut raw_args = std::env::args().skip(1);
		let source = raw_args.next().expect(&format!("Source is not specified. {}", USAGE));
		let dest_meta = raw_args.next().expect(&format!("Destination metadata is not specified. {}", USAGE));
		let dest = raw_args.next().expect(&format!("Destination dir is not specified. {}", USAGE));
		let mut args = Args {
			source,
			dest_meta,
			dest,
			clear_dest: false,
		};
		for raw_arg in raw_args {
			let mut splits = raw_arg[2..].split('=');
			match splits.next().unwrap() {
				"clear-dest" => args.clear_dest = parse(&mut splits),
				other => panic!("unknown option: {}, {}", other, USAGE),
			}
		}
		args
	}
}

pub fn run() -> Result<(), String>{
	env_logger::try_init().unwrap();
	let args = Args::parse();

	let source_path = std::path::Path::new(&args.source);
	let dest_meta = std::path::Path::new(&args.dest_meta);
	let dest_path = std::path::Path::new(&args.dest);

	let dest_meta = Options::load_metadata(dest_meta)
		.map_err(|e| format!("Error loading dest metadata: {:?}", e))?
		.ok_or_else(|| format!("Error opening dest metadata file"))?;

	let dest_columns = dest_meta.columns;

	if args.clear_dest && std::fs::metadata(dest_path).is_ok() {
		std::fs::remove_dir_all(dest_path).map_err(|e| format!("Error removing dest dir: {:?}", e))?;
	}
	std::fs::create_dir_all(dest_path).map_err(|e| format!("Error creating dest dir: {:?}", e))?;
	let mut dest_options = Options::with_columns(dest_path, dest_columns.len() as u8);
	dest_options.columns = dest_columns;
	dest_options.sync_wal = false;
	dest_options.sync_data = false;

	parity_db::migrate(source_path, dest_options)
		.map_err(|e| format!("Migration error: {:?}", e))?;
	Ok(())
}

fn main() {
	fdlimit::raise_fd_limit();
	run().unwrap();
}

*/
