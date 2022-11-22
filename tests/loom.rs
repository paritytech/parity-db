#![cfg(all(feature = "loom", feature = "instrumentation"))]

use loom::thread;
use parity_db::{Db, Options};
use std::{iter::from_fn, sync::Arc};
use tempfile::tempdir;

#[test]
fn simple_commit_and_write_concurrency() {
	exec_simple_commit_and_write_concurrency(false, false);
	exec_simple_commit_and_write_concurrency(false, true);
	exec_simple_commit_and_write_concurrency(true, false);
	exec_simple_commit_and_write_concurrency(true, true);
}

fn exec_simple_commit_and_write_concurrency(is_btree: bool, is_ref_counted: bool) {
	loom::model(move || {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.always_flush = true;
		options.with_background_thread = false;
		options.salt = Some([0; 32]);
		options.columns[0].btree_index = is_btree;
		options.columns[0].ref_counted = is_ref_counted;
		options.columns[0].preimage = is_ref_counted;

		let global_db = Arc::new(Db::open_or_create(&options).unwrap());

		let db = global_db.clone();
		let t1 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>((1..=20).map(|i| (0, vec![2 * i], Some(vec![2 * i]))))
				.unwrap();
			db.process_commits().unwrap();
			db.clean_logs().unwrap();
		});

		let db = global_db.clone();
		let t2 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>((1..=20).map(|i| (0, vec![2 * i + 1], Some(vec![2 * i + 1]))))
				.unwrap();
			db.flush_logs().unwrap();
			db.process_reindex().unwrap();
		});

		let db = global_db.clone();
		let t3 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>((1..=20).map(|i| (0, vec![2 * i], None))).unwrap();
			db.enact_logs().unwrap();
		});

		t1.join().unwrap();
		t2.join().unwrap();
		t3.join().unwrap();

		// Check state consistency
		assert_eq!(global_db.get(0, &[0]).unwrap(), global_db.get(0, &[1]).unwrap())
	})
}

#[test]
fn btree_iteration_concurrency() {
	loom::model(move || {
		let tmp = tempdir().unwrap();
		let mut options = Options::with_columns(tmp.path(), 1);
		options.always_flush = true;
		options.with_background_thread = false;
		options.salt = Some([0; 32]);
		options.columns[0].btree_index = true;

		let global_db = Arc::new(Db::open_or_create(&options).unwrap());

		let db = global_db.clone();
		let t1 = thread::spawn(move || {
			db.commit::<_, Vec<u8>>(vec![
				(0, vec![1], Some(vec![1])),
				(0, vec![10], Some(vec![10])),
			])
			.unwrap();
			db.commit::<_, Vec<u8>>(vec![
				(0, vec![0], Some(vec![0])),
				(0, vec![5], Some(vec![5])),
				(0, vec![15], Some(vec![15])),
			])
			.unwrap();
		});

		let db = global_db.clone();
		let t2 = thread::spawn(move || {
			db.process_commits().unwrap();
			db.flush_logs().unwrap();
			db.enact_logs().unwrap();
			db.clean_logs().unwrap();
		});

		let mut iter = global_db.iter(0).unwrap();
		iter.seek_to_first().unwrap();
		let increasing = from_fn(|| iter.next().unwrap()).collect::<Vec<_>>();
		for i in 1..increasing.len() {
			assert!(increasing[i - 1].0 < increasing[i].0);
		}

		iter.seek_to_last().unwrap();
		let decreasing = from_fn(|| iter.prev().unwrap()).collect::<Vec<_>>();
		for i in 1..decreasing.len() {
			assert!(decreasing[i - 1].0 > decreasing[i].0);
		}

		t1.join().unwrap();
		t2.join().unwrap();
	})
}
