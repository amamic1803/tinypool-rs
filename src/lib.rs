//! ***tinypool*** is a simple thread pool implementation in Rust.
//!
//! A thread pool is a collection of threads that can be used to execute jobs concurrently.
//! The aim of this crate is to provide a simple, easy-to-use thread pool that can be used in any project.
//!
//! **Example (1):**
//! ```
//! use tinypool::ThreadPool;
//! use std::sync::{Arc, Mutex};
//!
//! let mut threadpool = ThreadPool::new(Some(4)).unwrap();
//! let counter = Arc::new(Mutex::new(0));
//!
//! for _ in 0..100 {
//!     let counter_thrd = Arc::clone(&counter);
//!     threadpool.add_to_queue(move || {
//!         let mut counter = counter_thrd.lock().unwrap();
//!         *counter += 1;
//!     });
//! }
//!
//! threadpool.join();
//! assert_eq!(*counter.lock().unwrap(), 100);
//! ```
//!
//! **Example (2):**
//! ```
//! use tinypool::ThreadPool;
//! use std::sync::mpsc;
//!
//! let mut threadpool = ThreadPool::new(Some(4)).unwrap();
//! let (tx, rx) = mpsc::channel();
//!
//! for i in 0..100 {
//!     let tx = tx.clone();
//!     threadpool.add_to_queue(move || tx.send(i).unwrap());
//! }
//! drop(tx);
//!
//! let mut sum = 0;
//! while let Ok(i) = rx.recv() {
//!    sum += i;
//! }
//!
//! assert_eq!(sum, 4950);
//! ```
//!
//! **Note:** The ```ThreadPool::join()``` method is automatically called when the thread pool is dropped.





pub mod threadpool;

#[doc(inline)]
pub use threadpool::*;
