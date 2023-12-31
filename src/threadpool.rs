//! A module containing the thread pool implementation.





// STD LIBRARY IMPORTS

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Condvar, mpsc, Mutex};
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
}





// STRUCTS

/// A thread pool that manages threads and executes jobs in them.
pub struct ThreadPool {
    /// The workers in the thread pool.
    workers: Vec<Worker>,
    /// The channel to receive messages from the worker threads.
    worker_info_channel: (mpsc::Sender<Message>, mpsc::Receiver<Message>),
    /// The queue of jobs to execute.
    queue: Arc<(Mutex<VecDeque<Message>>, Condvar)>,
    /// The number of queued jobs.
    queued_jobs: Arc<(Mutex<usize>, Condvar)>,
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
        let worker_info_channel = mpsc::channel::<Message>();
        let queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let queued_jobs = Arc::new((Mutex::new(0), Condvar::new()));

        let mut threadpool = Self {
            workers,
            worker_info_channel,
            queue,
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
        *self.queued_jobs.0.lock().unwrap() += 1;
        self.queue.0.lock().unwrap().push_back(Message::Job(Box::new(job)));
        self.queue.1.notify_one();
    }

    /// Get the number of queued (incomplete) jobs.
    /// # Returns
    /// The number of queued jobs.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    ///
    /// for _ in 0..8 {
    ///     threadpool.add_to_queue(|| {
    ///         thread::sleep(Duration::from_secs(5));
    ///     });
    /// }
    ///
    /// assert_eq!(threadpool.queued_jobs(), 8);  // 8 jobs (shouldn't have finished yet)
    /// threadpool.join();
    /// assert_eq!(threadpool.queued_jobs(), 0);  // 0 jobs (should have finished)
    /// ```
    pub fn queued_jobs(&self) -> usize {
        *self.queued_jobs.0.lock().unwrap()
    }

    /// Get the number of threads in the thread pool.
    /// # Returns
    /// The number of worker threads.
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
    /// If reducing the number of threads, this method will close threads only after they finish executing their current job.
    /// # Arguments
    /// * `size` - The number of threads to set the thread pool size to. Use `None` to determine the number of threads automatically.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or an ```io::Error``` if unsuccessful.
    /// # Examples
    /// ```
    /// use tinypool::ThreadPool;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    ///
    /// for _ in 0..8 {
    ///    threadpool.add_to_queue(|| { thread::sleep(Duration::from_secs(5)); });
    /// }
    /// assert_eq!(threadpool.queued_jobs(), 8);
    ///
    /// // increasing thread pool size doesn't block the main thread
    /// threadpool.set_size(Some(8)).unwrap();
    /// assert_eq!(threadpool.size(), 8);
    ///
    /// // decreasing thread pool size blocks the main thread while waiting for currently queued jobs to finish (to close threads)
    /// threadpool.set_size(Some(2)).unwrap();
    /// assert_eq!(threadpool.size(), 2);
    /// ```
    pub fn set_size(&mut self, size: Option<usize>) -> Result<(), io::Error> {
        let size = size.unwrap_or(thread::available_parallelism()?.get());
        let current_size = self.size();

        match size.cmp(&current_size) {
            Ordering::Less => {
                let mut reduce = current_size - size;

                let mut queue_lock = self.queue.0.lock().unwrap();
                for _ in 0..reduce {
                    queue_lock.push_front(Message::Terminate);
                    self.queue.1.notify_one();
                }
                drop(queue_lock);

                while reduce != 0 {
                    match self.worker_info_channel.1.recv().unwrap() {
                        Message::Closing(id) => {
                            self.workers.retain(|worker| worker.id != id);
                            reduce -= 1;
                        },
                        _ => panic!("Received unexpected message from worker thread."),
                    }
                }

                self.workers.shrink_to_fit();
            },
            Ordering::Equal => {},
            Ordering::Greater => {
                let additional_size = size - current_size;
                self.workers.reserve_exact(additional_size);

                let mut id = 0;
                for _ in 0..additional_size {
                    while self.workers.iter().any(|worker| worker.id == id) {
                        id += 1;
                    }
                    self.workers.push(
                        Worker::new(
                            id,
                            self.worker_info_channel.0.clone(),
                            Arc::clone(&self.queue),
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
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    /// for _ in 0..8 {
    ///    threadpool.add_to_queue(|| { thread::sleep(Duration::from_secs(1)); });
    /// }
    /// threadpool.join();
    /// assert_eq!(threadpool.size(), 0);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    /// ```
    pub fn join(&mut self) {
        let mut queue_lock = self.queue.0.lock().unwrap();
        for _ in &self.workers {
            queue_lock.push_back(Message::Terminate);
        }
        self.queue.1.notify_all();
        drop(queue_lock);
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
    /// let mut threadpool = ThreadPool::new(Some(4)).unwrap();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
    ///
    /// for _ in 0..8 {
    ///   threadpool.add_to_queue(|| { thread::sleep(Duration::from_secs(1)); });
    /// }
    /// threadpool.wait();
    /// assert_eq!(threadpool.size(), 4);
    /// assert_eq!(threadpool.queued_jobs(), 0);
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
    /// * `worker_info_channel` - The channel to send messages to the thread pool.
    /// * `queue` - The queue of jobs to execute.
    /// * `queued_jobs` - The number of queued jobs.
    /// # Returns
    /// A ```Result``` containing the ```Worker``` if successful, or an ```io::Error``` if unsuccessful.
    #[allow(unused_assignments)]
    fn new(id: usize, worker_info_channel: mpsc::Sender<Message>, queue: Arc<(Mutex<VecDeque<Message>>, Condvar)>, queued_jobs: Arc<(Mutex<usize>, Condvar)>) -> Result<Self, io::Error> {
        let thread = thread::Builder::new()
            .spawn(move || {
                let mut queue_lock = Some(queue.0.lock().unwrap());
                loop {
                    match queue_lock.as_mut().unwrap().pop_front() {
                        Some(message) => {
                            queue_lock = None;  // unlock the queue
                            match message {
                                Message::Job(job) => {
                                    job();
                                    *queued_jobs.0.lock().unwrap() -= 1;
                                    queued_jobs.1.notify_all();
                                },
                                Message::Terminate => {
                                    worker_info_channel.send(Message::Closing(id)).unwrap();
                                    break;
                                },
                                _ => panic!("Received unexpected message from thread pool."),
                            }
                            queue_lock = Some(queue.0.lock().unwrap());  // lock the queue
                        },
                        None => {
                            queue_lock = Some(queue.1.wait(queue_lock.unwrap()).unwrap());
                        },
                    }
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
        let pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.queued_jobs(), 0);

        for _ in 0..8 {
            pool.add_to_queue(|| {
                thread::sleep(Duration::from_secs(5));
            });
        }
        assert_eq!(pool.queued_jobs(), 8);  // none of the jobs should have finished yet (sleeping for 5s)
    }

    #[test]
    fn size_and_queue() {
        let mut pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);
        assert_eq!(pool.queued_jobs(), 0);

        for _ in 0..10 {
            pool.add_to_queue(|| {
                thread::sleep(Duration::from_secs(5));
            });
        }
        assert_eq!(pool.queued_jobs(), 10);  // none of the jobs should have finished yet (sleeping for 5s)

        pool.set_size(Some(8)).unwrap();
        assert_eq!(pool.size(), 8);
        assert_eq!(pool.queued_jobs(), 10);  // increasing pool size does not affect queued jobs

        thread::sleep(Duration::from_secs(1));  // wait to be sure that threads really took the jobs

        pool.set_size(Some(2)).unwrap();
        assert_eq!(pool.size(), 2);
        assert_eq!(pool.queued_jobs(), 2);  // after resizing to 8 threads, 8 jobs should have finished, leaving 2 jobs
    }

    #[test]
    fn join() {
        let mut pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        for _ in 0..40 {
            pool.add_to_queue(|| {
                thread::sleep(Duration::from_millis(100));
            });
        }

        pool.join();
        assert_eq!(pool.queued_jobs(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 0);  // all threads have been closed
    }

    #[test]
    fn wait() {
        let pool = ThreadPool::new(Some(4)).unwrap();
        assert_eq!(pool.size(), 4);

        for _ in 0..40 {
            pool.add_to_queue(|| {
                thread::sleep(Duration::from_millis(100));
            });
        }

        pool.wait();
        assert_eq!(pool.queued_jobs(), 0);  // all jobs have finished
        assert_eq!(pool.size(), 4);  // all threads are still running
    }
}
