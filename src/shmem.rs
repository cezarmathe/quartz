// src/shmem.rs

use once_cell::sync::OnceCell;

use pgrx::prelude::*;
use pgrx::shmem::PgSharedMemoryInitialization;

use std::ffi::CString;

/// An object residing in PostgreSQL's shared memory.
pub struct SharedObject<T> {
    inner: OnceCell<*mut T>,
    name: &'static str,
}

impl<T> SharedObject<T> {
    /// Create a new shared object.
    pub const fn new(name: &'static str) -> Self {
        Self {
            inner: OnceCell::new(),
            name,
        }
    }

    /// Attach a value to this shared object.
    pub fn attach(&self, value: *mut T) {
        self.inner.set(value).expect(
            format!(
                "SharedObject<{}> {} has already been initialized",
                std::any::type_name::<T>(),
                self.name,
            )
            .as_str(),
        );
    }

    /// Get a reference to the value attached to this shared object.
    pub fn get(&self) -> &'static T {
        unsafe {
            self.inner
                .get()
                .expect(
                    format!(
                        "SharedObject<{}> {} has not been initialized",
                        std::any::type_name::<T>(), self.name,
                    ).as_str())
                .as_ref()
                .expect(
                    format!(
                        "SharedObject<{}> {} has been initialized with a null pointer. This is an internal error.",
                        std::any::type_name::<T>(), self.name,
                    ).as_str(),
                )
        }
    }
}

impl<T: Default> PgSharedMemoryInitialization for SharedObject<T> {
    fn pg_init(&'static self) {
        unsafe {
            pg_sys::RequestAddinShmemSpace(std::mem::size_of::<T>());
        }
    }

    fn shmem_init(&'static self) {
        unsafe {
            let shm_name = CString::new(self.name) // fixme
                .expect("CString::new() failed");

            let addin_shmem_init_lock: *mut pg_sys::LWLock =
                &mut (*pg_sys::MainLWLockArray.add(21)).lock;

            let mut found = false;
            pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
            let fv_shmem =
                pg_sys::ShmemInitStruct(shm_name.into_raw(), std::mem::size_of::<T>(), &mut found)
                    as *mut T;

            if found {
                error!("SharedObject<{}> {} already existed in shared memory", std::any::type_name::<T>(), self.name);
            }

            self.attach(fv_shmem);
            let object = T::default();
            std::ptr::copy(&object, fv_shmem, 1);

            pg_sys::LWLockRelease(addin_shmem_init_lock);
        }
    }
}

unsafe impl<T> Send for SharedObject<T> where T: Default {}
unsafe impl<T> Sync for SharedObject<T> where T: Default {}
