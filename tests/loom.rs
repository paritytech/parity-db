#![cfg(all(feature = "loom", feature = "instrumentation"))]

use loom::thread;
use parity_db::{Db, Options};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn trivial_commit_concurrency() {
	loom::model(|| {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.always_flush = true;
		options.with_background_thread = false;

		let global_db = Arc::new(Db::open_or_create(&options).unwrap());

		let db = global_db.clone();
		let t1 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>(vec![(0, vec![0], Some(vec![1])), (0, vec![1], Some(vec![1]))])
				.unwrap();
		});

		let db = global_db.clone();
		let t2 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>(vec![(0, vec![0], Some(vec![2])), (0, vec![1], Some(vec![2]))])
				.unwrap();
		});

		t1.join().unwrap();
		t2.join().unwrap();

		// Checks, that either T1 committed before T2 or reverse
		assert_eq!(global_db.get(0, &[0]).unwrap(), global_db.get(0, &[1]).unwrap())
	})
}
