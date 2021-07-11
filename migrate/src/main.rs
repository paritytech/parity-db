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

#[cfg_attr(any(target_os = "linux", target_os = "macos"),  global_allocator)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub use parity_db::Db;

const USAGE: &str = "
Usage: migrate source_dir dest_meta

Options:
	--clear-dest           Clear dest dir befoe attemtimg migration.
";

#[derive(Clone)]
struct Args {
	clear_dest: bool,
}

impl Default for Args {
	fn default() -> Args {
		Args {
			clear_dest: false,
		}
	}
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
		let mut args = Args::default();
		for raw_arg in std::env::args().skip(1) {
			let mut splits = raw_arg[2..].split('=');
			match splits.next().unwrap() {
				"clear-dest" => args.clear_dest = parse(&mut splits),
				other => panic!("unknown option: {}, {}", other, USAGE),
			}
		}
		args
	}
}

pub fn run() {
	env_logger::try_init().unwrap();
	let args = Args::parse();
}

fn main() {
	run()
}


