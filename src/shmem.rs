// // src/shmem.rs

// use once_cell::sync::OnceCell;
// use pgrx::pg_sys;
// use pgrx::shmem::PgSharedMemoryInitialization;

// pub struct SharedObject<T> {
//     inner: OnceCell<T>
// }

// impl<T> SharedObject<T> {
//     pub const fn new() -> Self {
//         Self { inner: OnceCell::new() }
//     }

//     pub fn take(self) -> T {
//         self.inner
//     }
// }

// impl<T> std::ops::Deref for SharedObject<T> {
//     type Target = T;

//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }

// impl<T> std::ops::DerefMut for SharedObject<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.inner
//     }
// }

// impl<T> PgSharedMemoryInitialization for SharedObject<T> {
//     fn pg_init(&'static self) {
//         unsafe {
//             pg_sys::RequestAddinShmemSpace(std::mem::size_of::<T>());
//         }
//     }

//     fn shmem_init(&'static self) {
//         unsafe {
//             let shm_name = std::ffi::CString::new("dsadasdas") // fixme
//                 .expect("CString::new() failed");

//             let addin_shmem_init_lock: *mut pg_sys::LWLock =
//                 &mut (*pg_sys::MainLWLockArray.add(21)).lock;

//             let mut found = false;
//             pg_sys::LWLockAcquire(addin_shmem_init_lock, pg_sys::LWLockMode_LW_EXCLUSIVE);
//             let fv_shmem =
//                 pg_sys::ShmemInitStruct(shm_name.into_raw(), std::mem::size_of::<T>(), &mut found)
//                     as *mut T;

//             ty.attach(fv_shmem);
//             let atomic = T::default();
//             std::ptr::copy(&atomic, fv_shmem, 1);

//             let value = self.inner .

//             pg_sys::LWLockRelease(addin_shmem_init_lock);
//         }
//     }
// }
