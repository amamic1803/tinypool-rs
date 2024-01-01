//! A module containing the thread pool implementation.





// IMPORTS

use std::cmp::Ordering;
use std::io;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;





// ENUMS

/// The messages that can be sent to the worker threads.
enum Message {
    /// A job to execute in the worker thread.
    Job(Box<dyn FnOnce() + Send + 'static>),
    /// Terminate the worker thread.
    Terminate,
    /// The worker thread with the given ID is closing.
    Closing(usize),
    /// The queue is empty. There are no more jobs to execute.
    EmptyQueue,
    /// Continue executing jobs. (unpause)
    Continue,
}





// STRUCTS

/// A thread pool that manages threads and executes jobs in them.
pub struct ThreadPool {
    /// The workers in the thread pool.
    workers: Vec<Worker>,
    /// The transmitting end of the channel to communicate info messages from the worker threads.
    worker_info_up_tx: mpsc::Sender<Message>,
    /// The receiving end of the channel to communicate info messages from the worker threads.
    worker_info_up_rx: mpsc::Receiver<Message>,
    /// The transmitting end of the channel to communicate info messages to the worker threads.
    worker_info_down_tx: mpsc::Sender<Message>,
    /// The receiving end of the channel to communicate info messages to the worker threads.
    worker_info_down_rx: Arc<Mutex<mpsc::Receiver<Message>>>,
    /// The transmitting end of the channel to communicate jobs to the worker threads.
    queue_tx: mpsc::Sender<Message>,
    /// The receiving end of the channel to communicate jobs to the worker threads.
    queue_rx: Arc<Mutex<mpsc::Receiver<Message>>>,
    /// The number of jobs in the queue.
    queued_jobs: Arc<AtomicUsize>,
}
impl ThreadPool {
    /// Create a new thread pool with the given size.
    /// # Arguments
    /// * `size` - The number of threads to create in the thread pool. Use `None` to determine the number of threads automatically.
    /// # Returns
    /// A ```Result``` containing the ```ThreadPool``` if the creation was successful, or an ```io::Error``` if it was unsuccessful.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// // Create a thread pool with 4 threads
    /// let threadpool = ThreadPool::new(Some(4)).unwrap();
    /// // Create a thread pool with the number of threads determined automatically
    /// let threadpool2 = ThreadPool::new(None).unwrap();
    /// ```
    pub fn new(size: Option<usize>) -> Result<Self, io::Error> {
        let size = size.unwrap_or(thread::available_parallelism()?.get());

        let workers = Vec::with_capacity(size);
        let (worker_info_up_tx, worker_info_up_rx) = mpsc::channel::<Message>();
        let (worker_info_down_tx, worker_info_down_rx) = mpsc::channel::<Message>();
        let worker_info_down_rx = Arc::new(Mutex::new(worker_info_down_rx));
        let (queue_tx, queue_rx) = mpsc::channel::<Message>();
        let queue_rx = Arc::new(Mutex::new(queue_rx));
        let queued_jobs = Arc::new(AtomicUsize::new(0));

        let mut threadpool = Self {
            workers,
            worker_info_up_tx,
            worker_info_up_rx,
            worker_info_down_tx,
            worker_info_down_rx,
            queue_tx,
            queue_rx,
            queued_jobs,
        };

        threadpool.set_size(Some(size))?;

        Ok(threadpool)
    }

    /// Add a job to the queue. Jobs are executed in the order they are added.
    /// If there are no available threads, the job will be queued until a thread is available.
    /// # Arguments
    /// * `job` - The job (closure) to execute in a thread pool.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// let counter = Arc::new(Mutex::new(0));
    ///
    /// for _ in 0..100 {
    ///     let counter_thrd = Arc::clone(&counter);
    ///     threadpool.add_to_queue(move || {
    ///         let mut counter = counter_thrd.lock().unwrap();
    ///         *counter += 1;
    ///     });
    /// }
    ///
    /// threadpool.join();
    /// assert_eq!(*counter.lock().unwrap(), 100);
    /// ```
    pub fn add_to_queue<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.queued_jobs.fetch_add(1, AtomicOrdering::Release);
        self.queue_tx.send(Message::Job(Box::new(job))).unwrap();
    }

    /// Get the number of queued (and running) jobs.
    /// # Returns
    /// * ```usize``` - The number of queued jobs.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let mut threadpool = ThreadPool::new(Some(0)).unwrap();
    ///
    /// for i in 0..8 {
    ///     threadpool.add_to_queue(move || { println!("{i}"); });
    /// }
    /// assert_eq!(threadpool.queued_jobs(), 8);  // 8 jobs queued
    ///
    /// threadpool.set_size(Some(4)).unwrap();
    /// threadpool.join();
    /// assert_eq!(threadpool.queued_jobs(), 0);  // 0 jobs
    /// ```
    pub fn queued_jobs(&self) -> usize {
        self.queued_jobs.load(AtomicOrdering::Acquire)
    }

    /// Get the number of threads in the thread pool.
    /// # Returns
    /// * ```usize``` - The number of threads in the thread pool.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    ///
    /// let threadpool2 = ThreadPool::new(Some(0)).unwrap();
    /// assert_eq!(threadpool2.size(), 0);
    /// ```
    pub fn size(&self) -> usize {
        self.workers.len()
    }

    /// Set the number of threads in the thread pool.
    /// If increasing the number of threads, this method will not block the main thread.
    /// If reducing the number of threads, this method will put closing messages in the queue for the worker threads to receive.
    /// So, this method will block the main thread until all those closing messages are received.
    /// That means that all jobs before the closing messages need to be processed.
    /// Note that some jobs may still be executing in the remaining worker threads, so the ```queued_jobs()``` method may return a non-zero value.
    /// # Arguments
    /// * `size` - The number of threads to set the thread pool size to. Use `None` to determine the number of threads automatically.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or an ```io::Error``` if unsuccessful.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let mut threadpool = ThreadPool::new(Some(0)).unwrap();
    /// assert_eq!(threadpool.size(), 0);
    ///
    /// for i in 0..8 {
    ///    threadpool.add_to_queue(move || { println!("{i}"); });
    /// }
    /// assert_eq!(threadpool.queued_jobs(), 8);
    ///
    /// // increasing thread pool size doesn't block the main thread
    /// threadpool.set_size(Some(8)).unwrap();
    /// assert_eq!(threadpool.size(), 8);
    /// // jobs are now being executed
    ///
    /// // decreasing thread pool size blocks the main thread while waiting for all queued jobs to finish
    /// threadpool.set_size(Some(2)).unwrap();
    /// assert_eq!(threadpool.size(), 2);
    /// // threadpool.queued_jobs() may return a non-zero value here (the value is guaranteed to be <= threadpool.size())
    /// assert!(threadpool.queued_jobs() <= threadpool.size());
    /// ```
    pub fn set_size(&mut self, size: Option<usize>) -> Result<(), io::Error> {
        let new_size = size.unwrap_or(thread::available_parallelism()?.get());
        let current_size = self.size();

        match new_size.cmp(&current_size) {
            Ordering::Less => {
                let reduce = current_size - new_size;

                for _ in 0..reduce {
                    self.queue_tx.send(Message::Terminate).unwrap();
                }

                for _ in 0..reduce {
                    match self.worker_info_up_rx.recv().unwrap() {
                        Message::Closing(id) => {
                            self.workers.retain(|worker| worker.id != id);
                        },
                        _ => panic!("Received unexpected message from worker thread."),
                    }
                }

                self.workers.shrink_to_fit();
            },
            Ordering::Equal => {},
            Ordering::Greater => {
                let additional_size = new_size - current_size;
                self.workers.reserve_exact(additional_size);

                let mut id = 0;
                for _ in 0..additional_size {
                    while self.workers.iter().any(|worker| worker.id == id) {
                        id += 1;
                    }
                    self.workers.push(
                        Worker::new(
                            id,
                            self.worker_info_up_tx.clone(),
                            Arc::clone(&self.worker_info_down_rx),
                            Arc::clone(&self.queue_rx),
                            Arc::clone(&self.queued_jobs),
                        )?
                    );
                }
            },
        }

        Ok(())
    }

    /// Wait for all queued jobs to finish and close all threads.
    /// This method will block until all queued jobs have finished.
    /// It will then close all threads in the thread pool.
    /// If you want to wait for all queued jobs to finish, but want to keep the threads running, use ```ThreadPool::wait()``` instead.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    ///
    /// for i in 0..8 {
    ///    threadpool.add_to_queue(move || { println!("{i}"); });
    /// }
    ///
    /// threadpool.join();
    /// assert_eq!(threadpool.size(), 0);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    /// ```
    pub fn join(&mut self) {
        let num_of_workers = self.workers.len();
        for _ in 0..num_of_workers {
            self.queue_tx.send(Message::Terminate).unwrap();
        }
        for worker in self.workers.drain(..) {
            worker.thread.join().unwrap();
        }
        self.workers.shrink_to_fit();
        for _ in 0..num_of_workers {
            match self.worker_info_up_rx.recv().unwrap() {
                Message::Closing(_) => {},
                _ => panic!("Received unexpected message from worker thread."),
            }
        }
    }

    /// Wait for all queued jobs to finish.
    /// This method will block until all queued jobs have finished.
    /// If you want to wait for all queued jobs to finish and close all threads, use ```ThreadPool::join()``` instead.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    ///
    /// for i in 0..8 {
    ///   threadpool.add_to_queue(move || { println!("{i}") });
    /// }
    ///
    /// threadpool.wait();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    /// ```
    pub fn wait(&self) {
        // send empty queue messages to all workers

        let pool_size = self.size();
        for _ in 0..pool_size {
            self.queue_tx.send(Message::EmptyQueue).unwrap();
        }

        // every worker, when it receives an empty queue message, will echo that message in worker_info_up channel
        // and pause and wait for continue message in the info channel

        // we need to receive the empty queue messages from all workers in the worker_info_up channel
        // when we do, that means all workers are paused and waiting for continue message
        // that is all jobs have finished executing

        for _ in 0..pool_size {
            match self.worker_info_up_rx.recv().unwrap() {
                Message::EmptyQueue => {},
                _ => panic!("Received unexpected message from worker thread."),
            }
        }

        // send continue messages to all workers
        for _ in 0..pool_size {
            self.worker_info_down_tx.send(Message::Continue).unwrap();
        }
    }
}
impl Drop for ThreadPool {
    /// Drop the thread pool.
    /// This will wait for all queued jobs to finish and close all threads.
    /// Equivalent to ```ThreadPool::join()```.
    fn drop(&mut self) {
        self.join();
    }
}

/// A worker struct, used to spawn a worker thread.
struct Worker {
    /// The id of the worker.
    id: usize,
    /// The thread of the worker.
    thread: thread::JoinHandle<()>,
}
impl Worker {
    /// Create a new worker struct.
    /// # Arguments
    /// * `id` - The id of the worker.
    /// * `worker_info_up_tx` - The transmitting end of the channel to communicate info messages from the worker threads.
    /// * `worker_info_down_rx` - The receiving end of the channel to communicate info messages to the worker threads.
    /// * `queue_rx` - The receiving end of the channel to communicate jobs to the worker threads.
    /// * `queued_jobs` - The number of jobs in the queue.
    /// # Returns
    /// A ```Result``` containing the ```Worker``` if successful, or an ```io::Error``` if unsuccessful.
    fn new(id: usize, worker_info_up_tx: mpsc::Sender<Message>, worker_info_down_rx: Arc<Mutex<mpsc::Receiver<Message>>>, queue_rx: Arc<Mutex<mpsc::Receiver<Message>>>, queued_jobs: Arc<AtomicUsize>) -> Result<Self, io::Error> {
        let thread = thread::Builder::new()
            .spawn(move || {
                loop {
                    let msg = queue_rx.lock().unwrap().recv().unwrap();
                    match msg {
                        Message::Job(job) => {
                            job();
                            queued_jobs.fetch_sub(1, AtomicOrdering::Release);
                        },
                        Message::Terminate => {
                            worker_info_up_tx.send(Message::Closing(id)).unwrap();
                            break;
                        },
                        Message::EmptyQueue => {
                            worker_info_up_tx.send(Message::EmptyQueue).unwrap();
                            match worker_info_down_rx.lock().unwrap().recv().unwrap() {
                                Message::Continue => {},
                                _ => panic!("Received unexpected message from thread pool."),
                            }
                        },
                        _ => panic!("Received unexpected message from thread pool."),
                    }
                }
            })?;

        Ok(Self { id, thread })
    }
}





// TESTS

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);
        assert_eq!(pool.queued_jobs(), 0);
    }

    #[test]
    fn size() {
        let mut pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        pool.set_size(Some(8)).unwrap();
        assert_eq!(pool.size(), 8);

        pool.set_size(Some(2)).unwrap();
        assert_eq!(pool.size(), 2);

        pool.set_size(Some(0)).unwrap();
        assert_eq!(pool.size(), 0);

        pool.set_size(None).unwrap();
        assert_eq!(pool.size(), thread::available_parallelism().unwrap().get());
    }

    #[test]
    fn queue() {
        let pool = ThreadPool::new(Some(0)).unwrap();
        assert_eq!(pool.queued_jobs(), 0);

        for i in 0..8 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        assert_eq!(pool.queued_jobs(), 8);
    }

    #[test]
    fn join() {
        let mut pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.join();
        assert_eq!(pool.queued_jobs(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 0);  // all threads have been closed
    }

    #[test]
    fn wait() {
        let pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.wait();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(pool.queued_jobs(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 4);  // all threads are still running
    }

    #[test]
    fn complex() {
        let mut pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.set_size(Some(8)).unwrap();
        assert_eq!(pool.size(), 8);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.wait();
        assert!(pool.queued_jobs() <= pool.size());

        pool.set_size(Some(2)).unwrap();
        assert_eq!(pool.size(), 2);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.set_size(Some(0)).unwrap();
        assert_eq!(pool.size(), 0);

        for i in 0..1000 {
            pool.add_to_queue(move || { println!("{i}"); });
        }

        pool.set_size(None).unwrap();
        assert_eq!(pool.size(), thread::available_parallelism().unwrap().get());

        pool.join();
        assert_eq!(pool.queued_jobs(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 0);  // all threads have been closed
    }
}
