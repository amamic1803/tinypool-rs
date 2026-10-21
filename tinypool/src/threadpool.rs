//! A thread pool implementation.

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::any::Any;
use std::fmt::Debug;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::{io, panic};
use thiserror::Error;

/// A message between the thread pool and the worker threads.
enum Message {
    /// A job to execute in the worker thread.
    Job(Box<dyn FnOnce() + Send + 'static>),
    /// Terminate the worker thread.
    Terminate,
    /// Tell the worker thread to wait and not take any more jobs until told to continue.
    Wait,
    /// Tell the worker thread to continue taking jobs.
    Continue,
    /// The worker thread is waiting and not taking jobs.
    Waiting,
}

/// A thread pool implementation.
#[derive(Debug)]
pub struct ThreadPool {
    /// The workers in the thread pool.
    workers: Vec<Worker>,
    /// The sending end of the channel to communicate jobs to the worker threads.
    queue_tx: Sender<Message>,
    /// The receiving end of the channel to communicate jobs to the worker threads.
    queue_rx: Receiver<Message>,
    /// The sending end of the channel to communicate info messages to the worker threads.
    info_down_tx: Sender<Message>,
    /// The receiving end of the channel to communicate info messages from the worker threads.
    info_up_rx: Receiver<Message>,
}
impl ThreadPool {
    /// Creates a new [ThreadPool] instance with defaults from [Builder::new].
    /// # Returns
    /// * A [ThreadPool] instance with default configuration.
    /// # Errors
    /// * An [io::Error] if not enough threads could be created for the thread pool.
    pub fn new() -> Result<Self, io::Error> {
        Builder::default().build()
    }

    /// Returns a [Builder] instance to configure a [ThreadPool] before its workers are started.
    ///
    /// This is exactly the same as calling [Builder::new].
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Checks if the work queue is full.
    ///
    /// This means that the queue is at its maximum capacity
    /// and adding more jobs with [spawn](ThreadPool::spawn) or [execute](ThreadPool::execute) will
    /// block until a slot frees up.
    pub fn is_queue_full(&self) -> bool {
        self.queue_tx.is_full()
    }

    /// Returns the number of unfinished jobs in the work queue.
    ///
    /// This includes both queued and running jobs.
    /// # Returns
    /// * The number of unfinished jobs in the work queue.
    pub fn queued_jobs(&self) -> usize {
        let mut count = self.workers.iter().filter(|w| w.active()).count();
        count += self.queue_tx.len();
        count
    }

    /// Returns the capacity of the work queue.
    /// # Returns
    /// * The capacity of the work queue if it is bounded, or `None` if it is unbounded.
    pub fn capacity(&self) -> Option<usize> {
        self.queue_tx.capacity()
    }

    /// Returns the number of worker threads in the thread pool.
    /// # Returns
    /// * The number of worker threads in the thread pool.
    pub fn size(&self) -> usize {
        self.workers.len()
    }

    /// Queues a job and returns a handle to its return value.
    ///
    /// This will block if the queue is bounded and full, until a slot frees up.
    /// # Returns
    /// * A [JoinHandle] to the return value of the job.
    pub fn spawn<F, T>(&self, job: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.queue_tx
            .send(Message::Job(Box::new(move || {
                let result = panic::catch_unwind(AssertUnwindSafe(job));
                let _ = tx.send(result); // ignores errors if the receiver was dropped
            })))
            .unwrap();
        JoinHandle { rx }
    }

    /// Queues a job and returns a handle to its return value.
    ///
    /// This will return an error if the queue is bounded and full, instead of blocking.
    /// # Returns
    /// * `Ok(JoinHandle)` to the return value of the job if it was successfully queued.
    /// * `Err(TryError)` if the queue is bounded and full, and the job could not be queued.
    pub fn try_spawn<F, T>(&self, job: F) -> Result<JoinHandle<T>, TryError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = move || {
            let result = panic::catch_unwind(AssertUnwindSafe(job));
            let _ = tx.send(result); // ignores errors if the receiver was dropped
        };
        self.queue_tx
            .try_send(Message::Job(Box::new(job)))
            .map_err(|_| TryError)?;
        Ok(JoinHandle { rx })
    }

    /// Queues a job without creating a handle (fire-and-forget).
    ///
    /// Cheaper than [spawn](Self::spawn) because no result slot is allocated.
    /// This will block if the queue is bounded and full, until a slot frees up.
    /// A panic in the job is caught and discarded.
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = move || {
            let _ = panic::catch_unwind(AssertUnwindSafe(job));
        };
        self.queue_tx.send(Message::Job(Box::new(job))).unwrap();
    }

    /// Queues a job without creating a handle (fire-and-forget).
    ///
    /// Cheaper than [spawn](Self::spawn) because no result slot is allocated.
    /// This will return an error if the queue is bounded and full, instead of blocking.
    /// A panic in the job is caught and discarded.
    pub fn try_execute<F>(&self, job: F) -> Result<(), TryError>
    where
        F: FnOnce() + Send + 'static,
    {
        let job = move || {
            let _ = panic::catch_unwind(AssertUnwindSafe(job));
        };
        self.queue_tx
            .try_send(Message::Job(Box::new(job)))
            .map_err(|_| TryError)
    }

    /// Clears the work queue.
    ///
    /// This will remove all jobs from the queue,
    /// but will not affect any jobs that are currently running.
    pub fn clear_queue(&self) {
        while self.queue_rx.try_recv().is_ok() {}
    }

    /// Wait for all queued jobs to finish and close the thread pool.
    ///
    /// This method will block until all queued jobs have finished,
    /// and then close the thread pool. Dropping the thread pool does the same thing,
    /// so this usually doesn't need to be called explicitly. For waiting
    /// for all queued jobs to finish without closing the thread pool,
    /// use [wait](ThreadPool::wait) instead.
    pub fn join(self) {
        drop(self)
    }

    /// Wait for all queued jobs to finish.
    ///
    /// This method will block until all queued jobs have finished,
    /// but will not close the thread pool like [join](ThreadPool::join) does.
    pub fn wait(&self) {
        for _ in 0..self.size() {
            self.queue_tx.send(Message::Wait).unwrap();
        }
        for _ in 0..self.size() {
            match self.info_up_rx.recv().unwrap() {
                Message::Waiting => {}
                _ => panic!("received unexpected message from the worker thread"),
            }
        }
        for _ in 0..self.size() {
            self.info_down_tx.send(Message::Continue).unwrap();
        }
    }
}
impl Default for ThreadPool {
    fn default() -> ThreadPool {
        Self::new().unwrap()
    }
}
impl Drop for ThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.size() {
            self.queue_tx.send(Message::Terminate).unwrap();
        }
        for worker in self.workers.drain(..) {
            worker.thread.join().unwrap();
        }
    }
}

/// A builder for a [ThreadPool] instance.
///
/// Use this to configure the thread pool before starting the workers.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Builder {
    size: usize,
    capacity: Option<usize>,
}
impl Builder {
    /// Creates a new [Builder] instance with default values.
    ///
    /// Default values are:
    /// * one thread per available CPU ([available_parallelism](thread::available_parallelism))
    /// * unlimited queue capacity
    pub fn new() -> Builder {
        Builder {
            size: thread::available_parallelism().unwrap().get(),
            capacity: None,
        }
    }

    /// Sets the number of worker threads in the thread pool.
    /// # Arguments
    /// * `n` - The number of worker threads to create.
    /// # Returns
    /// * A modified [Builder] instance with the specified number of worker threads.
    /// # Panics
    /// * If `n` is zero since adding threads after the thread pool is created is not supported, and the thread pool would not be able to execute any jobs.
    pub fn size(mut self, n: usize) -> Builder {
        if n == 0 {
            panic!("thread pool size cannot be zero");
        }
        self.size = n;
        self
    }

    /// Sets the maximum number of jobs that can be queued at once.
    /// # Arguments
    /// * `capacity` - The maximum number of jobs that can be queued at once.
    /// # Returns
    /// * A modified [Builder] instance with the specified queue capacity.
    /// # Panics
    /// * If `capacity` is zero since a queue capacity of zero would not allow any jobs to be queued, and the thread pool would not be able to execute any jobs.
    pub fn capacity(mut self, capacity: usize) -> Builder {
        if capacity == 0 {
            panic!("queue capacity cannot be zero");
        }
        self.capacity = Some(capacity);
        self
    }

    /// Creates a new [ThreadPool] instance with the specified configuration, starting the worker threads.
    /// # Returns
    /// * A [ThreadPool] instance with the specified configuration.
    /// # Errors
    /// * An [io::Error] if not enough threads could be created for the thread pool.
    pub fn build(self) -> Result<ThreadPool, io::Error> {
        let (queue_tx, queue_rx) = match self.capacity {
            Some(capacity) => bounded(capacity),
            None => unbounded(),
        };
        let (info_up_tx, info_up_rx) = unbounded();
        let (info_down_tx, info_down_rx) = unbounded();

        let mut workers = Vec::with_capacity(self.size);
        for id in 0..self.size {
            workers.push(Worker::new(
                id,
                queue_rx.clone(),
                info_down_rx.clone(),
                info_up_tx.clone(),
            )?);
        }

        Ok(ThreadPool {
            workers,
            queue_tx,
            queue_rx,
            info_down_tx,
            info_up_rx,
        })
    }
}
impl Default for Builder {
    fn default() -> Builder {
        Builder::new()
    }
}

/// A reason why a job did not produce a value.
#[derive(Debug, Error)]
pub enum JoinError {
    /// The job panicked, holds the panic payload.
    #[error("the job panicked")]
    Panicked(Box<dyn Any + Send + 'static>),
    /// The job was dropped before it ran.
    #[error("the job was dropped before it ran")]
    Canceled,
}

/// Transform join channel messages into results.
fn join_to_result<T>(msg: Result<thread::Result<T>, oneshot::RecvError>) -> Result<T, JoinError> {
    match msg {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(payload)) => Err(JoinError::Panicked(payload)),
        Err(oneshot::RecvError) => Err(JoinError::Canceled),
    }
}

/// A handle to the return value of a job.
///
/// This handle is returned by [ThreadPool::spawn] and [ThreadPool::try_spawn].
/// To receive the value, wait for it with `.join()` in normal code, or `.await` in async code.
pub struct JoinHandle<T> {
    rx: oneshot::Receiver<thread::Result<T>>,
}
impl<T> JoinHandle<T> {
    /// Wait for the job to finish and return its value.
    ///
    /// This will block the current thread until the job is finished.
    /// # Returns
    /// * `Ok(value)` if the job finished successfully and returned a value.
    /// * `Err(JoinError::Panicked(payload))` if the job panicked, with the panic payload.
    /// * `Err(JoinError::Canceled)` if the job was dropped before it ran.
    pub fn join(self) -> Result<T, JoinError> {
        join_to_result(self.rx.recv())
    }
}
impl<T> IntoFuture for JoinHandle<T> {
    type Output = Result<T, JoinError>;
    type IntoFuture = JoinFuture<T>;

    fn into_future(self) -> JoinFuture<T> {
        JoinFuture {
            rx: self.rx.into_future(),
        }
    }
}

/// The future returned by [JoinHandle::into_future].
#[derive(Debug)]
pub struct JoinFuture<T> {
    rx: oneshot::AsyncReceiver<thread::Result<T>>,
}
impl<T> Future for JoinFuture<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(join_to_result)
    }
}

/// An error returned by [ThreadPool::try_spawn] or [ThreadPool::try_execute].
///
/// This error is returned when a job cannot be added to the thread pool's queue,
/// typically because the queue is full.
#[derive(Error, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[error("adding job to the thread pool queue failed because the queue is full")]
pub struct TryError;

/// A worker thread in the thread pool.
#[derive(Debug)]
struct Worker {
    /// The id of the worker.
    #[allow(dead_code)]
    id: usize,
    /// The thread of the worker.
    thread: thread::JoinHandle<()>,
    /// Whether the worker is currently executing a job.
    active: Arc<AtomicBool>,
}
impl Worker {
    /// Creates a new [Worker] instance.
    /// # Arguments
    /// * `id` - The id of the worker.
    /// * `queue_rx` - The receiving end of the channel to communicate jobs to the worker threads.
    /// * `info_down_rx` - The receiving end of the channel to communicate info messages to the worker threads.
    /// * `info_up_tx` - The transmitting end of the channel to communicate info messages from the worker threads.
    /// # Returns
    /// * A new [Worker] instance or an [io::Error] if the thread could not be created.
    fn new(
        id: usize,
        queue_rx: Receiver<Message>,
        info_down_rx: Receiver<Message>,
        info_up_tx: Sender<Message>,
    ) -> Result<Self, io::Error> {
        let active = Arc::new(AtomicBool::new(false));
        let active_clone = Arc::clone(&active);
        let thread = thread::Builder::new()
            .name(format!("worker-{id}"))
            .spawn(move || {
                loop {
                    match queue_rx.recv().unwrap() {
                        Message::Job(job) => {
                            active.store(true, Ordering::Relaxed);
                            job();
                            active.store(false, Ordering::Relaxed);
                        }
                        Message::Terminate => break,
                        Message::Wait => {
                            info_up_tx.send(Message::Waiting).unwrap();
                            match info_down_rx.recv().unwrap() {
                                Message::Continue => {}
                                _ => panic!(
                                    "received unexpected message from the thread pool manager"
                                ),
                            }
                        }
                        _ => panic!("received unexpected message from the thread pool manager"),
                    }
                }
            })?;

        Ok(Self {
            id,
            thread,
            active: active_clone,
        })
    }

    /// Returns whether the worker is currently executing a job.
    fn active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
