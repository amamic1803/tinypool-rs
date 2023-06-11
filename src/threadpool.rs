//! A module containing the thread pool implementation.





// STD LIBRARY IMPORTS

use std::{
    error::Error,
    fmt::Display,
    io,
    sync::{
        Arc,
        Condvar,
        mpsc,
        Mutex
    },
    thread,
};





// ENUMS

/// The messages that can be sent to the worker threads.
enum Message {
    /// A job to execute in the worker thread.
    Job(Box<dyn FnOnce() + Send + 'static>),
    /// Terminate the worker thread.
    Terminate,
    /// The worker thread with the given ID is closing.
    Closing(usize),
}


/// Errors that can occur when creating a thread pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadPoolError {
    /// Attempted to add a job to an empty thread pool.
    EmptyPool,
    /// The thread pool failed to spawn a thread.
    ThreadSpawn,
}

impl Display for ThreadPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThreadPoolError::EmptyPool => write!(f, "Attempted to add a job to an empty thread pool."),
            ThreadPoolError::ThreadSpawn => write!(f, "The thread pool failed to spawn a thread."),
        }
    }
}

impl Error for ThreadPoolError {}

impl From<io::Error> for ThreadPoolError {
    fn from(_: io::Error) -> Self {
        Self::ThreadSpawn
    }
}





// STRUCTS

/// A thread pool that holds a number of threads and executes jobs on them.
pub struct ThreadPool {
    /// The workers in the thread pool.
    workers: Vec<Worker>,
    /// The channel to send messages to the worker threads.
    downstream_channel: (mpsc::Sender<Message>, Arc<Mutex<mpsc::Receiver<Message>>>),
    /// The channel to receive messages from the worker threads.
    upstream_channel: (mpsc::Sender<Message>, mpsc::Receiver<Message>),
    /// The number of queued jobs.
    queued_jobs: Arc<(Mutex<usize>, Condvar)>,
}

impl ThreadPool {

    /// Create a new thread pool with the given size.
    /// # Arguments
    /// * `size` - The number of threads to create in the thread pool. Use `0` to determine the number of threads automatically.
    /// # Returns
    /// A ```Result``` containing the ```ThreadPool``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::ThreadSpawn``` will be returned if the thread pool failed to spawn a thread.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let threadpool = ThreadPool::new(4).unwrap();  // Create a thread pool with 4 threads
    /// let threadpool2 = ThreadPool::new(0).unwrap();  // Create a thread pool with the number of threads determined automatically
    /// ```
    pub fn new(size: usize) -> Result<Self, ThreadPoolError> {
        let workers = Vec::new();

        let downstream_channel = mpsc::channel::<Message>();
        let downstream_channel = (downstream_channel.0, Arc::new(Mutex::new(downstream_channel.1)));

        let upstream_channel = mpsc::channel::<Message>();

        let queued_jobs = Arc::new((Mutex::new(0), Condvar::new()));

        let mut threadpool = Self {
            workers,
            downstream_channel,
            upstream_channel,
            queued_jobs
        };

        threadpool.set_size(size)?;

        Ok(threadpool)
    }

    /// Add a job to the thread pool. Jobs are executed in the order they are added.
    /// # Arguments
    /// * `job` - The job (closure) to execute on a worker thread.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::EmptyPool``` will be returned if the thread pool is empty.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let mut  threadpool = ThreadPool::new(4).unwrap();
    /// let counter = Arc::new(Mutex::new(0));
    ///
    /// for _ in 0..100 {
    ///     let counter_thrd = Arc::clone(&counter);
    ///     threadpool.execute(move || {
    ///         let mut counter = counter_thrd.lock().unwrap();
    ///         *counter += 1;
    ///     }).unwrap();
    /// }
    ///
    /// threadpool.join();
    /// assert_eq!(*counter.lock().unwrap(), 100);
    /// ```
    pub fn execute<F>(&self, job: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.size() == 0 {
            Err(ThreadPoolError::EmptyPool)
        } else {
            *self.queued_jobs.0.lock().unwrap() += 1;
            self.downstream_channel.0.send(Message::Job(Box::new(job))).unwrap();
            Ok(())
        }
    }

    /// Get the number of queued (and running) jobs.
    /// # Returns
    /// The number of queued jobs.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(4).unwrap();
    ///
    /// for _ in 0..8 {
    ///     threadpool.execute(|| { thread::sleep(Duration::from_secs(1)); }).unwrap();
    /// }
    ///
    /// assert_eq!(threadpool.queued(), 8);  // 8 jobs (shouldn't have finished yet)
    /// threadpool.join();
    /// assert_eq!(threadpool.queued(), 0);  // 0 jobs (should have finished)
    /// ```
    pub fn queued(&self) -> usize {
        *self.queued_jobs.0.lock().unwrap()
    }

    /// Get the number of threads in the thread pool.
    /// # Returns
    /// The number of worker threads.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    ///
    /// let threadpool = ThreadPool::new(4).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    ///
    /// let threadpool2 = ThreadPool::new(0).unwrap();
    /// assert_ne!(threadpool2.size(), 0);
    /// ```
    pub fn size(&self) -> usize {
        self.workers.len()
    }

    /// Set the number of threads in the thread pool.
    /// If you want to close all threads, use ```ThreadPool::join()``` instead.
    /// If reducing the number of threads, this method will close threads only after more jobs aren't available.
    /// # Arguments
    /// * `size` - The number of threads to set the thread pool to. Use `0` to determine the number of threads automatically.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::ThreadSpawn``` will be returned if the thread pool failed to spawn a thread.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(4).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    ///
    /// for _ in 0..8 {
    ///    threadpool.execute(|| { thread::sleep(Duration::from_secs(1)); }).unwrap();
    /// }
    /// assert_eq!(threadpool.queued(), 8);
    ///
    /// threadpool.set_size(8).unwrap();
    /// assert_eq!(threadpool.size(), 8);
    /// assert_eq!(threadpool.queued(), 8);  // increasing thread pool size doesn't block main thread
    ///
    /// threadpool.set_size(2).unwrap();
    /// assert_eq!(threadpool.size(), 2);
    /// assert!(threadpool.queued() <= 2);  // decreasing thread pool size blocks main thread until all jobs are finished
    ///
    /// threadpool.join();
    /// ```
    pub fn set_size(&mut self, size: usize) -> Result<(), ThreadPoolError> {
        let size: usize =
            if size == 0 {
                thread::available_parallelism()?.get()
            } else {
                size
            };

        let current_size = self.size();

        if size > current_size {
            let mut id = 0;
            self.workers.reserve_exact(size - current_size);
            for _ in current_size..size {
                while self.workers.iter().any(|worker| worker.id == id) {
                    id += 1;
                }
                self.workers.push(
                    Worker::new(
                        id,
                        Arc::clone(&self.downstream_channel.1),
                        self.upstream_channel.0.clone(),
                        Arc::clone(&self.queued_jobs)
                    )?
                );
            }
        } else {
            for _ in size..current_size {
                self.downstream_channel.0.send(Message::Terminate).unwrap()
            }
            let mut reduce = current_size - size;
            while reduce != 0 {
                match self.upstream_channel.1.recv().unwrap() {
                    Message::Closing(id) => {
                        self.workers.retain(|worker| worker.id != id);
                        reduce -= 1;
                    },
                    _ => panic!("Received unexpected message from worker thread."),
                }
            }
            self.workers.shrink_to_fit();
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
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(4).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued(), 0);
    /// for _ in 0..8 {
    ///    threadpool.execute(|| { thread::sleep(Duration::from_secs(1)); }).unwrap();
    /// }
    /// threadpool.join();
    /// assert_eq!(threadpool.size(), 0);
    /// assert_eq!(threadpool.queued(), 0);
    /// ```
    pub fn join(&mut self) {
        for _ in &self.workers {
            self.downstream_channel.0.send(Message::Terminate).unwrap();
        }
        for worker in &mut self.workers.drain(..) {
            worker.thread.join().unwrap();
        }
        self.workers.shrink_to_fit();
    }

    /// Wait for all queued jobs to finish.
    /// This method will block until all queued jobs have finished.
    /// If you want to wait for all queued jobs to finish and close all threads, use ```ThreadPool::join()``` instead.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(4).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued(), 0);
    /// for _ in 0..8 {
    ///   threadpool.execute(|| { thread::sleep(Duration::from_secs(1)); }).unwrap();
    /// }
    /// threadpool.wait();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued(), 0);
    /// ```
    pub fn wait(&self) {
        let mut queued_jobs = self.queued_jobs.0.lock().unwrap();
        while *queued_jobs > 0 {
            queued_jobs = self.queued_jobs.1.wait(queued_jobs).unwrap();
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


/// A worker struct. This struct is used to spawn a worker thread.
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
    /// * `downstream_receiver` - The receiver end of the channel to receive messages from the thread pool.
    /// * `upstream_sender` - The sender end of the channel to send messages to the thread pool.
    /// * `queued_jobs` - The number of queued jobs.
    /// # Returns
    /// A ```Result``` containing the ```Worker``` if successful, or a ```ThreadPoolError``` if error occurs.
    fn new(id: usize, downstream_receiver: Arc<Mutex<mpsc::Receiver<Message>>>, upstream_sender:mpsc::Sender<Message>, queued_jobs: Arc<(Mutex<usize>, Condvar)>) -> Result<Self, ThreadPoolError> {
        let thread = thread::Builder::new()
            .spawn(move || loop {
                let message = downstream_receiver.lock().unwrap().recv().unwrap();
                match message {
                    Message::Job(job) => {
                        job();
                        *queued_jobs.0.lock().unwrap() -= 1;
                        queued_jobs.1.notify_all();
                    },
                    Message::Terminate => {
                        upstream_sender.send(Message::Closing(id)).unwrap();
                        break;
                    },
                    _ => panic!("Received unexpected message from thread pool."),
                }
            })?;

        Ok(Self { id, thread })
    }
}





// TESTS

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::*;

    #[test]
    fn new() {
        let pool = ThreadPool::new(4).unwrap();

        assert_eq!(pool.size(), 4);
        assert_eq!(pool.queued(), 0);

        pool.wait();
        assert_eq!(pool.queued(), 0);
    }

    #[test]
    fn size() {
        let mut pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.size(), 4);

        pool.set_size(8).unwrap();
        assert_eq!(pool.size(), 8);

        pool.set_size(2).unwrap();
        assert_eq!(pool.size(), 2);

        pool.set_size(0).unwrap();
        assert_eq!(pool.size(), thread::available_parallelism().unwrap().get());
    }

    #[test]
    fn queue() {
        let pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.queued(), 0);

        for _ in 0..8 {
            pool.execute(|| {
                thread::sleep(Duration::from_millis(100));
            }).unwrap();
        }
        assert_eq!(pool.queued(), 8);  // none of the jobs have finished yet (sleeping for 100ms)
    }

    #[test]
    fn size_and_queue() {
        let mut pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.size(), 4);
        assert_eq!(pool.queued(), 0);

        for _ in 0..100 {
            pool.execute(|| {
                thread::sleep(Duration::from_millis(100));
            }).unwrap();
        }
        assert_eq!(pool.queued(), 100);  // none of the jobs have finished yet (sleeping for 100ms)

        pool.set_size(8).unwrap();
        assert_eq!(pool.size(), 8);
        assert_ne!(pool.queued(), 0);  // increasing pool size does not affect queued jobs

        pool.set_size(2).unwrap();
        assert_eq!(pool.size(), 2);
        assert!(pool.queued() <= 2);  // decreasing pool size requires all queued jobs to have finished
    }

    #[test]
    fn join() {
        let mut pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.size(), 4);

        for _ in 0..40 {
            pool.execute(|| {
                thread::sleep(Duration::from_millis(100));
            }).unwrap();
        }

        pool.join();
        assert_eq!(pool.queued(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 0);  // all threads have been closed
    }

    #[test]
    fn wait() {
        let pool = ThreadPool::new(4).unwrap();
        assert_eq!(pool.size(), 4);

        for _ in 0..40 {
            pool.execute(|| {
                thread::sleep(Duration::from_millis(100));
            }).unwrap();
        }

        pool.wait();
        assert_eq!(pool.queued(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 4);  // all threads are still running
    }
}
