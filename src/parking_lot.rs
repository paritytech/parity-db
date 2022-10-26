#[cfg(feature = "loom")]
pub use self::with_loom::*;

#[cfg(not(feature = "loom"))]
pub use parking_lot::*;

#[cfg(feature = "loom")]
mod with_loom {
	use std::{
		fmt, mem,
		mem::swap,
		ops::{Deref, DerefMut},
	};

	#[derive(Debug, Default)]
	pub struct Mutex<T>(loom::sync::Mutex<T>);

	impl<T> Mutex<T> {
		pub fn new(val: T) -> Self {
			Self(loom::sync::Mutex::new(val))
		}

		pub fn lock(&self) -> MutexGuard<'_, T> {
			MutexGuard(self.0.lock().unwrap())
		}
	}

	#[derive(Debug)]
	pub struct MutexGuard<'a, T>(loom::sync::MutexGuard<'a, T>);

	impl<'a, T> Deref for MutexGuard<'a, T> {
		type Target = T;

		fn deref(&self) -> &T {
			self.0.deref()
		}
	}

	impl<'a, T> DerefMut for MutexGuard<'a, T> {
		fn deref_mut(&mut self) -> &mut T {
			self.0.deref_mut()
		}
	}

	impl<'a, T: fmt::Display> fmt::Display for MutexGuard<'a, T> {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			self.0.fmt(f)
		}
	}

	#[derive(Debug, Default)]
	pub struct Condvar(loom::sync::Condvar);

	impl Condvar {
		pub fn new() -> Self {
			Self(loom::sync::Condvar::new())
		}

		pub fn notify_one(&self) {
			self.0.notify_one();
		}

		pub fn notify_all(&self) {
			self.0.notify_all()
		}

		pub fn wait<T>(&self, mutex_guard: &mut MutexGuard<'_, T>) {
			// loom API takes mutex by value. We do a hack to get the value from the reference and
			// copy back it at the end
			#[allow(invalid_value)]
			let mut owned_gard = unsafe { mem::zeroed() };
			swap(&mut mutex_guard.0, &mut owned_gard);
			owned_gard = self.0.wait(owned_gard).unwrap();
			mutex_guard.0 = owned_gard;
		}
	}

	#[derive(Debug, Default)]
	pub struct RwLock<T>(loom::sync::RwLock<T>);

	impl<T> RwLock<T> {
		pub fn new(val: T) -> Self {
			Self(loom::sync::RwLock::new(val))
		}

		pub fn read(&self) -> RwLockReadGuard<'_, T> {
			RwLockReadGuard(self.0.read().unwrap())
		}

		pub fn upgradable_read(&self) -> RwLockUpgradableReadGuard<'_, T> {
			RwLockUpgradableReadGuard(self.0.write().unwrap())
		}

		pub fn write(&self) -> RwLockWriteGuard<'_, T> {
			RwLockWriteGuard(self.0.write().unwrap())
		}
	}

	#[derive(Debug)]
	pub struct RwLockReadGuard<'a, T>(loom::sync::RwLockReadGuard<'a, T>);

	impl<'a, T> Deref for RwLockReadGuard<'a, T> {
		type Target = T;

		fn deref(&self) -> &T {
			self.0.deref()
		}
	}

	#[derive(Debug)]
	pub struct RwLockUpgradableReadGuard<'a, T>(loom::sync::RwLockWriteGuard<'a, T>);

	impl<'a, T> RwLockUpgradableReadGuard<'a, T> {
		pub fn upgrade(s: Self) -> RwLockWriteGuard<'a, T> {
			RwLockWriteGuard(s.0)
		}
	}

	impl<'a, T> Deref for RwLockUpgradableReadGuard<'a, T> {
		type Target = T;

		fn deref(&self) -> &T {
			self.0.deref()
		}
	}

	#[derive(Debug)]
	pub struct RwLockWriteGuard<'a, T>(loom::sync::RwLockWriteGuard<'a, T>);

	impl<'a, T> RwLockWriteGuard<'a, T> {
		pub fn downgrade_to_upgradable(s: Self) -> RwLockUpgradableReadGuard<'a, T> {
			RwLockUpgradableReadGuard(s.0)
		}
	}

	impl<'a, T> Deref for RwLockWriteGuard<'a, T> {
		type Target = T;

		fn deref(&self) -> &T {
			self.0.deref()
		}
	}

	impl<'a, T> DerefMut for RwLockWriteGuard<'a, T> {
		fn deref_mut(&mut self) -> &mut T {
			self.0.deref_mut()
		}
	}
}
