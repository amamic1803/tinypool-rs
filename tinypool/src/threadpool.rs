//! A thread pool implementation.

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::{io, mem, panic};
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
///
/// The thread pool is neither [Send] nor [Sync],
/// so it can only be used and dropped on the thread that created it.
///
/// # Deadlocks
/// A job should never block while waiting for another job from the same thread pool,
/// for example by calling [JoinHandle::join] on its handle.
/// If all workers end up blocked like that, no worker is left to run the jobs
/// they are waiting for, and the thread pool deadlocks.
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
    /// Makes the thread pool neither [Send] nor [Sync] since raw pointers are neither.
    _not_send_sync: PhantomData<*const ()>,
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
    ///
    /// The value is only an approximate snapshot while jobs are running:
    /// a worker marks itself as busy right after taking a job from the queue
    /// and as idle right after the job's result has been sent,
    /// so for a moment a job can be counted in neither place, or still be counted after
    /// its [JoinHandle] has returned. The value is exact after [wait](ThreadPool::wait) returns,
    /// until new jobs are queued.
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
            size: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
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
    ///
    /// Memory for all `capacity` slots of the queue is allocated up front when the thread pool
    /// is built, not as jobs are queued. Each slot takes a few dozen bytes,
    /// so a capacity in the millions already allocates tens of megabytes.
    /// For a queue without a limit, don't set a capacity at all.
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
            _not_send_sync: PhantomData,
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
    ///
    /// If the payload is a string, which is the case for panics created with [panic!],
    /// its message is included when the error is displayed.
    Panicked(Box<dyn Any + Send + 'static>),
    /// The job was dropped before it ran.
    Canceled,
}
impl Display for JoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            // `&**payload` is needed so the payload itself is inspected, not the box
            Self::Panicked(payload) => match payload_message(&**payload) {
                Some(message) => write!(f, "the job panicked: {message}"),
                None => f.write_str("the job panicked"),
            },
            Self::Canceled => f.write_str("the job was dropped before it ran"),
        }
    }
}

/// Returns the message of a panic payload if it is a string.
fn payload_message(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
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
#[derive(Debug)]
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
                    let msg = if let Ok(msg) = queue_rx.recv() {
                        msg
                    } else {
                        break;
                    };
                    match msg {
                        Message::Job(job) => {
                            active.store(true, Ordering::Relaxed);
                            // Jobs catch their own panics, but dropping a job's result or
                            // panic payload happens outside of that and can panic too.
                            // Catching it here makes sure that a job can never kill the worker.
                            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(job)) {
                                // Dropping this payload could panic again, so it is leaked instead.
                                mem::forget(payload);
                            }
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
    use crossbeam_channel::RecvTimeoutError;
    use std::collections::HashSet;
    use std::error::Error as _;
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::pin::pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, Mutex};
    use std::task::{Wake, Waker};
    use std::time::{Duration, Instant};

    /// Upper bound for a single test, exceeding it almost certainly means a deadlock.
    const TIMEOUT: Duration = Duration::from_secs(30);
    /// How long to wait before concluding that an operation is blocked.
    const BLOCK_CHECK: Duration = Duration::from_millis(100);

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /// Runs `f` on a separate thread and fails the test if it doesn't finish within [TIMEOUT].
    ///
    /// Panics inside `f` are propagated, so assertions work as usual.
    fn with_timeout<F>(f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (done_tx, done_rx) = bounded(1);
        let handle = thread::spawn(move || {
            f();
            let _ = done_tx.send(());
        });
        match done_rx.recv_timeout(TIMEOUT) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                if let Err(payload) = handle.join() {
                    panic::resume_unwind(payload);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                panic!("test did not finish within {TIMEOUT:?}, probably deadlocked")
            }
        }
    }

    /// Polls `cond` until it returns `true` or [TIMEOUT] passes.
    fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < TIMEOUT {
            if cond() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        cond()
    }

    /// Creates a gate. Jobs block on `rx.recv()` until the sender is dropped.
    fn gate() -> (Sender<()>, Receiver<()>) {
        bounded(0)
    }

    /// Occupies every worker of the pool with a job that blocks until the gate opens.
    ///
    /// Returns only after every worker has started its blocking job,
    /// so afterwards the queue is empty and all workers are active.
    fn occupy_workers(pool: &ThreadPool, gate: &Receiver<()>) {
        let (started_tx, started_rx) = unbounded();
        for _ in 0..pool.size() {
            let gate = gate.clone();
            let started_tx = started_tx.clone();
            pool.execute(move || {
                started_tx.send(()).unwrap();
                let _ = gate.recv();
            });
        }
        for _ in 0..pool.size() {
            started_rx
                .recv_timeout(TIMEOUT)
                .expect("worker did not pick up the blocking job");
        }
    }

    /// Asserts that `enqueue` blocks while the bounded queue is full and completes once a slot frees up.
    fn assert_blocks_while_full<F>(enqueue: F)
    where
        F: FnOnce(&ThreadPool) + Send + 'static,
    {
        with_timeout(move || {
            let pool = Builder::new().size(1).capacity(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            pool.try_execute(|| {}).unwrap();
            assert!(pool.is_queue_full());

            let (opened, opener) = open_gate_later(gate_tx);
            enqueue(&pool);
            assert!(
                opened.load(Ordering::SeqCst),
                "operation should block while the queue is full"
            );
            opener.join().unwrap();
            pool.wait();
            assert_eq!(pool.queued_jobs(), 0);
        });
    }

    /// Opens the gate from another thread after [BLOCK_CHECK].
    ///
    /// The returned flag is set right before the gate opens, so an operation
    /// that sees it set after returning must have been blocked until the gate opened.
    fn open_gate_later(gate: Sender<()>) -> (Arc<AtomicBool>, thread::JoinHandle<()>) {
        let opened = Arc::new(AtomicBool::new(false));
        let opened_clone = Arc::clone(&opened);
        let opener = thread::spawn(move || {
            thread::sleep(BLOCK_CHECK);
            opened_clone.store(true, Ordering::SeqCst);
            drop(gate);
        });
        (opened, opener)
    }

    /// Fails to compile if `$ty` implements `$trait`.
    ///
    /// If `$ty` implements `$trait`, both impls of `AmbiguousIfImpl` apply to it,
    /// so the type parameter of `some_item` can't be inferred.
    macro_rules! assert_not_impl {
        ($ty:ty: $trait:path) => {{
            trait AmbiguousIfImpl<A> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            struct Invalid;
            impl<T: ?Sized + $trait> AmbiguousIfImpl<Invalid> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        }};
    }

    /// Extracts the message from a [JoinError::Panicked] with a string payload.
    fn panic_message(err: JoinError) -> String {
        match err {
            JoinError::Panicked(payload) => payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .expect("panic payload is not a string"),
            JoinError::Canceled => panic!("expected a panicked job, but it was canceled"),
        }
    }

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Increments the shared counter when dropped.
    struct DropCounter(Arc<AtomicUsize>);
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Increments the shared counter and panics when dropped.
    struct PanicOnDrop(Arc<AtomicUsize>);
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("panic on drop");
        }
    }

    /// Increments the shared counter and panics with a [PanicOnDrop] payload when dropped.
    struct PanicWithPanicOnDropPayload(Arc<AtomicUsize>);
    impl Drop for PanicWithPanicOnDropPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic::panic_any(PanicOnDrop(Arc::clone(&self.0)));
        }
    }

    /// Runs `queue` while the only worker is blocked, so every handle it drops is gone
    /// before its job runs, then asserts that the very same worker is still alive and healthy.
    fn assert_worker_survives<F>(queue: F)
    where
        F: FnOnce(&ThreadPool) + Send + 'static,
    {
        with_timeout(move || {
            let pool = Builder::new().size(1).build().unwrap();
            let worker = pool.workers[0].thread.thread().id();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);

            queue(&pool);
            let (alive_tx, alive_rx) = bounded(1);
            pool.execute(move || alive_tx.send(thread::current().id()).unwrap());
            drop(gate_tx);

            // the last job only runs if the worker survived the jobs queued before it
            let id = loop {
                if let Ok(id) = alive_rx.recv_timeout(Duration::from_millis(1)) {
                    break id;
                }
                if pool.workers[0].thread.is_finished() {
                    // dropping the pool would join the dead worker and panic,
                    // which aborts the whole test process while this panic unwinds
                    mem::forget(pool);
                    panic!("the worker thread died");
                }
            };
            assert_eq!(id, worker);

            pool.wait();
            assert_eq!(pool.queued_jobs(), 0);
            assert!(!pool.workers[0].active());
            drop(pool);
        });
    }

    /// A waker that unparks the thread that created it.
    struct ThreadWaker(thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    /// A waker that counts how many times it was woken.
    #[derive(Default)]
    struct CountingWaker(AtomicUsize);
    impl CountingWaker {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }
    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A minimal executor that drives a single future to completion on the current thread.
    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }

    fn available_parallelism() -> usize {
        thread::available_parallelism().unwrap().get()
    }

    // ---------------------------------------------------------------------
    // Builder
    // ---------------------------------------------------------------------

    #[test]
    fn builder_new_uses_defaults() {
        let builder = Builder::new();
        assert_eq!(builder.size, available_parallelism());
        assert_eq!(builder.capacity, None);
    }

    #[test]
    fn builder_default_and_thread_pool_builder_match_new() {
        assert_eq!(Builder::default(), Builder::new());
        assert_eq!(ThreadPool::builder(), Builder::new());
    }

    #[test]
    fn builder_sets_size_and_capacity() {
        let builder = Builder::new().size(3).capacity(7);
        assert_eq!(builder.size, 3);
        assert_eq!(builder.capacity, Some(7));
    }

    #[test]
    fn builder_last_setting_wins() {
        let builder = Builder::new().size(2).size(5).capacity(1).capacity(9);
        assert_eq!(builder.size, 5);
        assert_eq!(builder.capacity, Some(9));
    }

    #[test]
    fn builder_setter_order_does_not_matter() {
        assert_eq!(
            Builder::new().size(2).capacity(4),
            Builder::new().capacity(4).size(2)
        );
    }

    #[test]
    #[should_panic(expected = "thread pool size cannot be zero")]
    fn builder_zero_size_panics() {
        let _ = Builder::new().size(0);
    }

    #[test]
    #[should_panic(expected = "queue capacity cannot be zero")]
    fn builder_zero_capacity_panics() {
        let _ = Builder::new().capacity(0);
    }

    #[test]
    fn builder_accepts_minimal_values() {
        let pool = Builder::new().size(1).capacity(1).build().unwrap();
        assert_eq!(pool.size(), 1);
        assert_eq!(pool.capacity(), Some(1));
        assert_eq!(pool.spawn(|| 1).join().unwrap(), 1);
    }

    #[test]
    fn builder_clone_eq_and_hash_are_consistent() {
        let a = Builder::new().size(4).capacity(16);
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_ne!(a, Builder::new().size(4));
        assert_ne!(a, Builder::new().size(5).capacity(16));

        let set: HashSet<Builder> = [a.clone(), b, Builder::new().size(4)].into_iter().collect();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&a));
    }

    #[test]
    fn builder_debug_shows_configuration() {
        let debug = format!("{:?}", Builder::new().size(2).capacity(3));
        assert!(debug.contains("Builder"), "{debug}");
        assert!(debug.contains("size: 2"), "{debug}");
        assert!(debug.contains("capacity: Some(3)"), "{debug}");
    }

    #[test]
    fn builder_can_build_multiple_pools_via_clone() {
        let builder = Builder::new().size(2).capacity(8);
        let first = builder.clone().build().unwrap();
        let second = builder.build().unwrap();
        assert_eq!(first.size(), second.size());
        assert_eq!(first.capacity(), second.capacity());
        assert_eq!(
            first.spawn(|| 1).join().unwrap() + second.spawn(|| 2).join().unwrap(),
            3
        );
    }

    // ---------------------------------------------------------------------
    // construction and introspection
    // ---------------------------------------------------------------------

    #[test]
    fn new_pool_uses_default_configuration() {
        let pool = ThreadPool::new().unwrap();
        assert_eq!(pool.size(), available_parallelism());
        assert_eq!(pool.capacity(), None);
        assert!(!pool.is_queue_full());
        assert_eq!(pool.queued_jobs(), 0);
    }

    #[test]
    fn default_pool_matches_new() {
        let pool = ThreadPool::default();
        assert_eq!(pool.size(), available_parallelism());
        assert_eq!(pool.capacity(), None);
    }

    #[test]
    fn build_respects_size() {
        for size in [1, 2, 3, 8, 64] {
            let pool = Builder::new().size(size).build().unwrap();
            assert_eq!(pool.size(), size);
            assert_eq!(pool.workers.len(), size);
        }
    }

    #[test]
    fn build_respects_capacity() {
        for capacity in [1, 2, 100, 10_000] {
            let pool = Builder::new().size(2).capacity(capacity).build().unwrap();
            assert_eq!(pool.capacity(), Some(capacity));
        }
        let pool = Builder::new().size(2).build().unwrap();
        assert_eq!(pool.capacity(), None);
    }

    #[test]
    fn workers_have_sequential_ids_and_matching_thread_names() {
        let pool = Builder::new().size(5).build().unwrap();
        for (i, worker) in pool.workers.iter().enumerate() {
            assert_eq!(worker.id, i);
            assert_eq!(
                worker.thread.thread().name(),
                Some(format!("worker-{i}").as_str())
            );
            assert!(!worker.active());
        }
    }

    #[test]
    fn every_worker_runs_jobs_under_its_own_name() {
        with_timeout(|| {
            let size = 4;
            let pool = Builder::new().size(size).build().unwrap();
            let barrier = Arc::new(Barrier::new(size));
            let handles: Vec<_> = (0..size)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    pool.spawn(move || {
                        // the barrier forces each job onto a different worker
                        barrier.wait();
                        thread::current().name().unwrap().to_owned()
                    })
                })
                .collect();
            let names: HashSet<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let expected: HashSet<String> = (0..size).map(|i| format!("worker-{i}")).collect();
            assert_eq!(names, expected);
        });
    }

    #[test]
    fn jobs_run_in_parallel_on_all_workers() {
        with_timeout(|| {
            let size = 16;
            let pool = Builder::new().size(size).build().unwrap();
            // would deadlock unless all jobs run at the same time
            let barrier = Arc::new(Barrier::new(size + 1));
            for _ in 0..size {
                let barrier = Arc::clone(&barrier);
                pool.execute(move || {
                    barrier.wait();
                });
            }
            barrier.wait();
            pool.wait();
        });
    }

    #[test]
    fn jobs_never_run_on_the_calling_thread() {
        let pool = Builder::new().size(2).build().unwrap();
        let caller = thread::current().id();
        for _ in 0..50 {
            assert_ne!(
                pool.spawn(|| thread::current().id()).join().unwrap(),
                caller
            );
        }
    }

    #[test]
    fn thread_pool_debug_is_implemented() {
        let pool = Builder::new().size(2).build().unwrap();
        let debug = format!("{pool:?}");
        assert!(debug.contains("ThreadPool"), "{debug}");
        assert!(debug.contains("Worker"), "{debug}");
    }

    #[test]
    fn thread_pool_is_neither_send_nor_sync() {
        assert_not_impl!(ThreadPool: Send);
        assert_not_impl!(ThreadPool: Sync);
    }

    #[test]
    fn other_public_types_are_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<Builder>();
        assert_sync::<Builder>();
        assert_send::<JoinHandle<i32>>();
        assert_send::<JoinHandle<String>>();
        assert_send::<JoinFuture<i32>>();
        assert_send::<JoinError>();
        assert_send::<TryError>();
        assert_sync::<TryError>();
    }

    // ---------------------------------------------------------------------
    // spawn
    // ---------------------------------------------------------------------

    #[test]
    fn spawn_returns_job_value() {
        let pool = Builder::new().size(2).build().unwrap();
        assert_eq!(pool.spawn(|| 2 + 2).join().unwrap(), 4);
    }

    #[test]
    fn spawn_supports_various_return_types() {
        let pool = Builder::new().size(2).build().unwrap();
        pool.spawn(|| ()).join().unwrap();
        assert_eq!(
            pool.spawn(|| String::from("hello")).join().unwrap(),
            "hello"
        );
        assert_eq!(
            pool.spawn(|| vec![1u8, 2, 3]).join().unwrap(),
            vec![1, 2, 3]
        );
        assert_eq!(
            pool.spawn(|| Some(Box::new(5))).join().unwrap(),
            Some(Box::new(5))
        );
        assert_eq!(pool.spawn(|| Ok::<_, String>(1)).join().unwrap(), Ok(1));
        assert_eq!(pool.spawn(|| [7u64; 1024]).join().unwrap(), [7u64; 1024]);
        let (a, b) = pool.spawn(|| (1, 'x')).join().unwrap();
        assert_eq!((a, b), (1, 'x'));
    }

    #[test]
    fn spawn_moves_ownership_in_and_out() {
        let pool = Builder::new().size(2).build().unwrap();
        let data: Vec<u32> = (0..100).collect();
        let reversed = pool
            .spawn(move || data.into_iter().rev().collect::<Vec<_>>())
            .join()
            .unwrap();
        assert_eq!(reversed, (0..100).rev().collect::<Vec<_>>());
    }

    #[test]
    fn spawn_many_jobs_joined_out_of_order() {
        let pool = Builder::new().size(4).build().unwrap();
        let handles: Vec<_> = (0..1000u64).map(|i| pool.spawn(move || i * i)).collect();
        for (i, handle) in handles.into_iter().enumerate().rev() {
            assert_eq!(handle.join().unwrap(), (i * i) as u64);
        }
    }

    #[test]
    fn spawn_handles_can_be_joined_on_other_threads() {
        let pool = Builder::new().size(4).build().unwrap();
        let handles: Vec<_> = (0..32).map(|i| pool.spawn(move || i * 2)).collect();
        thread::scope(|s| {
            for (i, handle) in handles.into_iter().enumerate() {
                s.spawn(move || assert_eq!(handle.join().unwrap(), i * 2));
            }
        });
    }

    #[test]
    fn spawn_handle_can_be_joined_after_pool_is_dropped() {
        let pool = Builder::new().size(2).build().unwrap();
        let handles: Vec<_> = (0..10).map(|i| pool.spawn(move || i + 1)).collect();
        drop(pool);
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results, (1..=10).collect::<Vec<_>>());
    }

    #[test]
    fn spawn_blocks_while_queue_is_full() {
        assert_blocks_while_full(|pool| {
            assert_eq!(pool.spawn(|| 7).join().unwrap(), 7);
        });
    }

    #[test]
    fn spawn_through_tiny_bounded_queue_completes_all_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(4).capacity(1).build().unwrap();
            let handles: Vec<_> = (0..2000).map(|i| pool.spawn(move || i)).collect();
            let sum: i32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(sum, (0..2000).sum());
        });
    }

    #[test]
    fn dropping_handle_does_not_cancel_job() {
        let pool = Builder::new().size(2).build().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let ran_clone = Arc::clone(&ran);
        drop(pool.spawn(move || ran_clone.store(true, Ordering::SeqCst)));
        pool.wait();
        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn result_of_dropped_handle_is_dropped_by_worker() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);

            let drops = Arc::new(AtomicUsize::new(0));
            let drops_clone = Arc::clone(&drops);
            drop(pool.spawn(move || DropCounter(drops_clone)));
            assert_eq!(drops.load(Ordering::SeqCst), 0);

            drop(gate_tx);
            pool.wait();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn joined_result_is_dropped_exactly_once() {
        let pool = Builder::new().size(2).build().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let drops_clone = Arc::clone(&drops);
        let value = pool.spawn(move || DropCounter(drops_clone)).join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(value);
        pool.wait();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // ---------------------------------------------------------------------
    // panics
    // ---------------------------------------------------------------------

    #[test]
    fn spawn_panic_with_str_payload() {
        let pool = Builder::new().size(2).build().unwrap();
        let err = pool.spawn(|| -> i32 { panic!("boom") }).join().unwrap_err();
        assert_eq!(panic_message(err), "boom");
    }

    #[test]
    fn spawn_panic_with_formatted_payload() {
        let pool = Builder::new().size(2).build().unwrap();
        let n = 42;
        let err = pool
            .spawn(move || -> i32 { panic!("boom {n}") })
            .join()
            .unwrap_err();
        assert_eq!(panic_message(err), "boom 42");
    }

    #[test]
    fn spawn_panic_with_custom_payload() {
        #[derive(Debug, PartialEq)]
        struct CustomPayload(u32);

        let pool = Builder::new().size(2).build().unwrap();
        let err = pool
            .spawn(|| panic::panic_any(CustomPayload(7)))
            .join()
            .unwrap_err();
        match err {
            JoinError::Panicked(payload) => {
                assert_eq!(
                    payload.downcast_ref::<CustomPayload>(),
                    Some(&CustomPayload(7))
                );
            }
            JoinError::Canceled => panic!("expected a panicked job"),
        }
    }

    #[test]
    fn panic_drops_captured_values() {
        let pool = Builder::new().size(1).build().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = DropCounter(Arc::clone(&drops));
        let result = pool
            .spawn(move || {
                let _keep = &counter;
                panic!("boom");
            })
            .join();
        assert!(matches!(result, Err(JoinError::Panicked(_))));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panics_do_not_kill_workers() {
        let pool = Builder::new().size(1).build().unwrap();
        let worker_id = pool.spawn(|| thread::current().id()).join().unwrap();
        for _ in 0..20 {
            assert!(pool.spawn(|| panic!("boom")).join().is_err());
            pool.execute(|| panic!("boom"));
        }
        // the very same thread is still alive and serving jobs
        assert_eq!(
            pool.spawn(|| thread::current().id()).join().unwrap(),
            worker_id
        );
    }

    #[test]
    fn mixed_panicking_and_successful_jobs() {
        let pool = Builder::new().size(4).build().unwrap();
        let handles: Vec<_> = (0..300)
            .map(|i| {
                pool.spawn(move || {
                    if i % 3 == 0 {
                        panic!("job {i} failed");
                    }
                    i
                })
            })
            .collect();
        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(value) => {
                    assert_ne!(i % 3, 0);
                    assert_eq!(value, i);
                }
                Err(err) => {
                    assert_eq!(i % 3, 0);
                    assert_eq!(panic_message(err), format!("job {i} failed"));
                }
            }
        }
    }

    #[test]
    fn execute_panic_is_swallowed_and_pool_keeps_working() {
        let pool = Builder::new().size(2).build().unwrap();
        for _ in 0..10 {
            pool.execute(|| panic!("boom"));
        }
        pool.wait();
        assert_eq!(pool.queued_jobs(), 0);
        assert_eq!(pool.spawn(|| 5).join().unwrap(), 5);
    }

    #[test]
    fn panicking_jobs_do_not_leave_workers_marked_active() {
        let pool = Builder::new().size(3).build().unwrap();
        for _ in 0..30 {
            pool.execute(|| panic!("boom"));
            let _ = pool.spawn(|| panic!("boom"));
        }
        pool.wait();
        assert_eq!(pool.queued_jobs(), 0);
        assert!(pool.workers.iter().all(|w| !w.active()));
    }

    #[test]
    fn worker_survives_panic_while_dropping_unclaimed_result() {
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&drops);
        assert_worker_survives(move |pool| {
            for _ in 0..3 {
                let counter = Arc::clone(&counter);
                // the handle is dropped, so the worker has to drop the result itself
                drop(pool.spawn(move || PanicOnDrop(counter)));
            }
        });
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn worker_survives_panic_while_dropping_panic_payload() {
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&drops);
        assert_worker_survives(move |pool| {
            // execute discards the payload of the panic
            let execute_counter = Arc::clone(&counter);
            pool.execute(move || panic::panic_any(PanicOnDrop(execute_counter)));
            // the handle is dropped, so the worker has to drop the payload itself
            let spawn_counter = Arc::clone(&counter);
            drop(pool.spawn(move || panic::panic_any(PanicOnDrop(spawn_counter))));
        });
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn worker_leaks_payload_of_panic_while_dropping_panic_payload() {
        let drops = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&drops);
        assert_worker_survives(move |pool| {
            pool.execute(move || panic::panic_any(PanicWithPanicOnDropPayload(counter)));
        });
        // the inner payload would panic too if it was dropped, so it must be leaked instead
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // ---------------------------------------------------------------------
    // try_spawn
    // ---------------------------------------------------------------------

    #[test]
    fn try_spawn_succeeds_on_unbounded_queue() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let handles: Vec<_> = (0..1000)
                .map(|i| pool.try_spawn(move || i).unwrap())
                .collect();
            drop(gate_tx);
            let sum: i32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(sum, (0..1000).sum());
        });
    }

    #[test]
    fn try_spawn_succeeds_while_there_is_space() {
        let pool = Builder::new().size(2).capacity(4).build().unwrap();
        assert_eq!(pool.try_spawn(|| 10).unwrap().join().unwrap(), 10);
    }

    #[test]
    fn try_spawn_fails_when_queue_is_full_and_drops_job() {
        with_timeout(|| {
            let pool = Builder::new().size(1).capacity(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let queued = pool.try_spawn(|| 1).unwrap();
            assert!(pool.is_queue_full());

            let ran = Arc::new(AtomicBool::new(false));
            let ran_clone = Arc::clone(&ran);
            let result = pool.try_spawn(move || ran_clone.store(true, Ordering::SeqCst));
            assert!(matches!(result, Err(TryError)));
            // the rejected job (and everything it captured) is dropped immediately
            assert_eq!(Arc::strong_count(&ran), 1);

            drop(gate_tx);
            assert_eq!(queued.join().unwrap(), 1);
            pool.wait();
            assert!(!ran.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn try_spawn_works_again_after_space_frees_up() {
        with_timeout(|| {
            let pool = Builder::new().size(1).capacity(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let first = pool.try_spawn(|| 1).unwrap();
            assert!(pool.try_spawn(|| 2).is_err());
            drop(gate_tx);
            pool.wait();
            assert_eq!(first.join().unwrap(), 1);
            assert_eq!(pool.try_spawn(|| 3).unwrap().join().unwrap(), 3);
        });
    }

    #[test]
    fn try_spawn_propagates_panics() {
        let pool = Builder::new().size(1).capacity(2).build().unwrap();
        let err = pool
            .try_spawn(|| -> u8 { panic!("boom") })
            .unwrap()
            .join()
            .unwrap_err();
        assert_eq!(panic_message(err), "boom");
    }

    // ---------------------------------------------------------------------
    // execute / try_execute
    // ---------------------------------------------------------------------

    #[test]
    fn execute_runs_job() {
        let pool = Builder::new().size(2).build().unwrap();
        let (tx, rx) = bounded(1);
        pool.execute(move || tx.send(99).unwrap());
        assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 99);
    }

    #[test]
    fn execute_runs_every_job_exactly_once() {
        let pool = Builder::new().size(4).build().unwrap();
        let hits: Arc<Vec<AtomicUsize>> =
            Arc::new((0..1000).map(|_| AtomicUsize::new(0)).collect());
        for i in 0..1000 {
            let hits = Arc::clone(&hits);
            pool.execute(move || {
                hits[i].fetch_add(1, Ordering::SeqCst);
            });
        }
        pool.wait();
        assert!(hits.iter().all(|h| h.load(Ordering::SeqCst) == 1));
    }

    #[test]
    fn execute_drops_job_after_running_it() {
        let pool = Builder::new().size(2).build().unwrap();
        let marker = Arc::new(());
        for _ in 0..50 {
            let marker = Arc::clone(&marker);
            pool.execute(move || {
                let _keep = &marker;
            });
        }
        pool.wait();
        assert_eq!(Arc::strong_count(&marker), 1);
    }

    #[test]
    fn execute_blocks_while_queue_is_full() {
        assert_blocks_while_full(|pool| pool.execute(|| {}));
    }

    #[test]
    fn single_worker_executes_jobs_in_fifo_order() {
        let pool = Builder::new().size(1).build().unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        for i in 0..200 {
            let order = Arc::clone(&order);
            if i % 2 == 0 {
                pool.execute(move || order.lock().unwrap().push(i));
            } else {
                let _ = pool.spawn(move || order.lock().unwrap().push(i));
            }
        }
        pool.wait();
        assert_eq!(*order.lock().unwrap(), (0..200).collect::<Vec<_>>());
    }

    #[test]
    fn try_execute_succeeds_on_unbounded_queue() {
        let pool = Builder::new().size(2).build().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..500 {
            let counter = Arc::clone(&counter);
            pool.try_execute(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }
        pool.wait();
        assert_eq!(counter.load(Ordering::SeqCst), 500);
    }

    #[test]
    fn try_execute_fills_queue_to_exact_capacity() {
        with_timeout(|| {
            let capacity = 3;
            let pool = Builder::new().size(1).capacity(capacity).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);

            let counter = Arc::new(AtomicUsize::new(0));
            for i in 0..capacity {
                assert!(!pool.is_queue_full(), "queue full after {i} jobs");
                let counter = Arc::clone(&counter);
                pool.try_execute(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
            }
            assert!(pool.is_queue_full());
            assert_eq!(pool.queued_jobs(), pool.size() + capacity);

            let rejected = Arc::clone(&counter);
            assert_eq!(
                pool.try_execute(move || {
                    rejected.fetch_add(100, Ordering::SeqCst);
                }),
                Err(TryError)
            );

            drop(gate_tx);
            pool.wait();
            assert_eq!(counter.load(Ordering::SeqCst), capacity);
            assert_eq!(Arc::strong_count(&counter), 1);
        });
    }

    #[test]
    fn try_execute_racing_workers_accounts_for_every_job() {
        with_timeout(|| {
            let pool = Builder::new().size(2).capacity(4).build().unwrap();
            let executed = Arc::new(AtomicUsize::new(0));
            let mut accepted = 0;
            for _ in 0..10_000 {
                let executed = Arc::clone(&executed);
                let job = move || {
                    executed.fetch_add(1, Ordering::SeqCst);
                };
                if pool.try_execute(job).is_ok() {
                    accepted += 1;
                }
            }
            pool.wait();
            // the queue starts empty, so at least the first `capacity` jobs fit
            assert!(accepted >= 4);
            assert_eq!(executed.load(Ordering::SeqCst), accepted);
            assert_eq!(Arc::strong_count(&executed), 1);
        });
    }

    // ---------------------------------------------------------------------
    // queue state: is_queue_full / queued_jobs / capacity
    // ---------------------------------------------------------------------

    #[test]
    fn unbounded_queue_is_never_full() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            for _ in 0..10_000 {
                pool.execute(|| {});
            }
            assert!(!pool.is_queue_full());
            drop(gate_tx);
            pool.wait();
        });
    }

    #[test]
    fn bounded_queue_full_state_transitions() {
        with_timeout(|| {
            let pool = Builder::new().size(2).capacity(2).build().unwrap();
            assert!(!pool.is_queue_full());

            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            assert!(!pool.is_queue_full());
            pool.execute(|| {});
            assert!(!pool.is_queue_full());
            pool.execute(|| {});
            assert!(pool.is_queue_full());

            drop(gate_tx);
            pool.wait();
            assert!(!pool.is_queue_full());
        });
    }

    #[test]
    fn queued_jobs_is_zero_for_idle_pool() {
        let pool = Builder::new().size(4).build().unwrap();
        assert_eq!(pool.queued_jobs(), 0);
        pool.wait();
        assert_eq!(pool.queued_jobs(), 0);
    }

    #[test]
    fn queued_jobs_counts_running_and_waiting_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            assert_eq!(pool.queued_jobs(), 2);

            for i in 1..=5 {
                pool.execute(|| {});
                assert_eq!(pool.queued_jobs(), 2 + i);
            }
            let _handle = pool.spawn(|| 1);
            assert_eq!(pool.queued_jobs(), 8);

            pool.clear_queue();
            assert_eq!(pool.queued_jobs(), 2);

            drop(gate_tx);
            pool.wait();
            assert_eq!(pool.queued_jobs(), 0);
        });
    }

    #[test]
    fn queued_jobs_returns_to_zero_after_wait() {
        let pool = Builder::new().size(4).build().unwrap();
        for _ in 0..100 {
            pool.execute(|| thread::sleep(Duration::from_micros(100)));
        }
        pool.wait();
        assert_eq!(pool.queued_jobs(), 0);
    }

    // ---------------------------------------------------------------------
    // clear_queue
    // ---------------------------------------------------------------------

    #[test]
    fn clear_queue_removes_pending_but_not_running_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let (gate_tx, gate_rx) = gate();

            let finished_running = Arc::new(AtomicUsize::new(0));
            let (started_tx, started_rx) = unbounded();
            for _ in 0..pool.size() {
                let gate_rx = gate_rx.clone();
                let started_tx = started_tx.clone();
                let finished_running = Arc::clone(&finished_running);
                pool.execute(move || {
                    started_tx.send(()).unwrap();
                    let _ = gate_rx.recv();
                    finished_running.fetch_add(1, Ordering::SeqCst);
                });
            }
            for _ in 0..pool.size() {
                started_rx.recv_timeout(TIMEOUT).unwrap();
            }

            let pending_ran = Arc::new(AtomicUsize::new(0));
            for _ in 0..50 {
                let pending_ran = Arc::clone(&pending_ran);
                pool.execute(move || {
                    pending_ran.fetch_add(1, Ordering::SeqCst);
                });
            }
            pool.clear_queue();
            assert_eq!(pool.queued_jobs(), pool.size());

            drop(gate_tx);
            pool.wait();
            assert_eq!(finished_running.load(Ordering::SeqCst), pool.size());
            assert_eq!(pending_ran.load(Ordering::SeqCst), 0);
            assert_eq!(Arc::strong_count(&pending_ran), 1);
        });
    }

    #[test]
    fn clear_queue_cancels_handles() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let handles: Vec<_> = (0..5).map(|i| pool.spawn(move || i)).collect();
            pool.clear_queue();
            for handle in handles {
                assert!(matches!(handle.join(), Err(JoinError::Canceled)));
            }
            drop(gate_tx);
        });
    }

    #[test]
    fn clear_queue_drops_captured_values_immediately() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let drops = Arc::new(AtomicUsize::new(0));
            for _ in 0..10 {
                let counter = DropCounter(Arc::clone(&drops));
                pool.execute(move || {
                    let _keep = &counter;
                });
            }
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            pool.clear_queue();
            assert_eq!(drops.load(Ordering::SeqCst), 10);
            drop(gate_tx);
        });
    }

    #[test]
    fn clear_queue_on_empty_queue_is_noop() {
        let pool = Builder::new().size(2).build().unwrap();
        pool.clear_queue();
        pool.clear_queue();
        assert_eq!(pool.queued_jobs(), 0);
        assert_eq!(pool.spawn(|| 1).join().unwrap(), 1);
    }

    #[test]
    fn clear_queue_frees_space_in_bounded_queue() {
        with_timeout(|| {
            let pool = Builder::new().size(1).capacity(3).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            while pool.try_execute(|| {}).is_ok() {}
            assert!(pool.is_queue_full());

            pool.clear_queue();
            assert!(!pool.is_queue_full());
            let handle = pool.try_spawn(|| "after clear").unwrap();

            drop(gate_tx);
            assert_eq!(handle.join().unwrap(), "after clear");
        });
    }

    #[test]
    fn pool_is_fully_usable_after_clear_queue() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            for _ in 0..10 {
                pool.execute(|| {});
            }
            pool.clear_queue();
            drop(gate_tx);
            pool.wait();

            let handles: Vec<_> = (0..100).map(|i| pool.spawn(move || i)).collect();
            let sum: i32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
            assert_eq!(sum, (0..100).sum());
        });
    }

    // ---------------------------------------------------------------------
    // wait
    // ---------------------------------------------------------------------

    #[test]
    fn wait_on_idle_pool_returns() {
        with_timeout(|| {
            let pool = Builder::new().size(4).build().unwrap();
            pool.wait();
            pool.wait();
        });
    }

    #[test]
    fn wait_waits_for_all_queued_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(4).build().unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..200 {
                let counter = Arc::clone(&counter);
                pool.execute(move || {
                    thread::sleep(Duration::from_micros(200));
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            pool.wait();
            assert_eq!(counter.load(Ordering::SeqCst), 200);
        });
    }

    #[test]
    fn wait_blocks_until_running_jobs_finish() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);

            let (opened, opener) = open_gate_later(gate_tx);
            pool.wait();
            assert!(
                opened.load(Ordering::SeqCst),
                "wait returned while jobs were running"
            );
            assert_eq!(pool.queued_jobs(), 0);
            opener.join().unwrap();
        });
    }

    #[test]
    fn wait_can_be_called_repeatedly_between_batches() {
        with_timeout(|| {
            let pool = Builder::new().size(3).build().unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            for round in 1..=50 {
                for _ in 0..20 {
                    let counter = Arc::clone(&counter);
                    pool.execute(move || {
                        counter.fetch_add(1, Ordering::SeqCst);
                    });
                }
                pool.wait();
                assert_eq!(counter.load(Ordering::SeqCst), round * 20);
                assert_eq!(pool.queued_jobs(), 0);
            }
        });
    }

    #[test]
    fn wait_with_capacity_smaller_than_size() {
        with_timeout(|| {
            let pool = Builder::new().size(4).capacity(1).build().unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..50 {
                let counter = Arc::clone(&counter);
                pool.execute(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            pool.wait();
            assert_eq!(counter.load(Ordering::SeqCst), 50);
        });
    }

    #[test]
    fn wait_with_full_bounded_queue() {
        assert_blocks_while_full(|pool| pool.wait());
    }

    #[test]
    fn wait_after_panicking_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            for _ in 0..10 {
                pool.execute(|| panic!("boom"));
            }
            pool.wait();
            assert_eq!(pool.queued_jobs(), 0);
        });
    }

    // ---------------------------------------------------------------------
    // join / drop
    // ---------------------------------------------------------------------

    #[test]
    fn join_waits_for_all_queued_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..100 {
                let counter = Arc::clone(&counter);
                pool.execute(move || {
                    thread::sleep(Duration::from_micros(200));
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            pool.join();
            assert_eq!(counter.load(Ordering::SeqCst), 100);
            assert_eq!(Arc::strong_count(&counter), 1);
        });
    }

    #[test]
    fn drop_waits_for_all_queued_jobs() {
        with_timeout(|| {
            let pool = Builder::new().size(3).capacity(2).build().unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..100 {
                let counter = Arc::clone(&counter);
                pool.execute(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            drop(pool);
            assert_eq!(counter.load(Ordering::SeqCst), 100);
        });
    }

    #[test]
    fn drop_runs_jobs_queued_behind_blocked_workers() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let counter = Arc::new(AtomicUsize::new(0));
            for _ in 0..10 {
                let counter = Arc::clone(&counter);
                pool.execute(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
            let (opened, opener) = open_gate_later(gate_tx);
            drop(pool);
            assert!(opened.load(Ordering::SeqCst));
            assert_eq!(counter.load(Ordering::SeqCst), 10);
            opener.join().unwrap();
        });
    }

    #[test]
    fn drop_with_capacity_smaller_than_size() {
        with_timeout(|| {
            let pool = Builder::new().size(8).capacity(1).build().unwrap();
            pool.execute(|| {});
            drop(pool);
        });
    }

    #[test]
    fn drop_idle_pools_repeatedly() {
        with_timeout(|| {
            for size in 1..=8 {
                let pool = Builder::new().size(size).build().unwrap();
                drop(pool);
            }
            for _ in 0..20 {
                ThreadPool::new().unwrap().join();
            }
        });
    }

    #[test]
    fn pools_on_different_threads_are_independent() {
        with_timeout(|| {
            let totals: Vec<usize> = thread::scope(|s| {
                let threads: Vec<_> = (1..=4)
                    .map(|t| {
                        s.spawn(move || {
                            let pool = Builder::new().size(t).build().unwrap();
                            let handles: Vec<_> =
                                (0..100).map(|i| pool.spawn(move || i * t)).collect();
                            handles.into_iter().map(|h| h.join().unwrap()).sum()
                        })
                    })
                    .collect();
                threads.into_iter().map(|t| t.join().unwrap()).collect()
            });
            let expected: Vec<usize> = (1..=4).map(|t| (0..100).sum::<usize>() * t).collect();
            assert_eq!(totals, expected);
        });
    }

    // ---------------------------------------------------------------------
    // concurrency
    // ---------------------------------------------------------------------

    #[test]
    fn jobs_can_share_state_through_mutex() {
        let pool = Builder::new().size(8).build().unwrap();
        let shared = Arc::new(Mutex::new(Vec::new()));
        for i in 0..1000 {
            let shared = Arc::clone(&shared);
            pool.execute(move || shared.lock().unwrap().push(i));
        }
        pool.wait();
        let mut values = shared.lock().unwrap().clone();
        values.sort_unstable();
        assert_eq!(values, (0..1000).collect::<Vec<_>>());
    }

    #[test]
    fn work_is_distributed_across_workers() {
        with_timeout(|| {
            let size = 4;
            let pool = Builder::new().size(size).build().unwrap();
            let barrier = Arc::new(Barrier::new(size));
            let handles: Vec<_> = (0..size * 10)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    pool.spawn(move || {
                        barrier.wait();
                        thread::current().id()
                    })
                })
                .collect();
            let ids: HashSet<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            assert_eq!(ids.len(), size);
        });
    }

    // ---------------------------------------------------------------------
    // async (IntoFuture / JoinFuture)
    // ---------------------------------------------------------------------

    #[test]
    fn await_returns_job_value() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let result = block_on(async { pool.spawn(|| 21 * 2).await });
            assert_eq!(result.unwrap(), 42);
        });
    }

    #[test]
    fn await_multiple_handles() {
        with_timeout(|| {
            let pool = Builder::new().size(4).build().unwrap();
            let sum = block_on(async {
                let handles: Vec<_> = (0..100).map(|i| pool.spawn(move || i)).collect();
                let mut sum = 0;
                for handle in handles {
                    sum += handle.await.unwrap();
                }
                sum
            });
            assert_eq!(sum, (0..100).sum());
        });
    }

    #[test]
    fn await_panicked_job() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let err =
                block_on(pool.spawn(|| -> i32 { panic!("async boom") }).into_future()).unwrap_err();
            assert_eq!(panic_message(err), "async boom");
        });
    }

    #[test]
    fn await_canceled_job() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);
            let handle = pool.spawn(|| 1);
            pool.clear_queue();
            assert!(matches!(
                block_on(handle.into_future()),
                Err(JoinError::Canceled)
            ));
            drop(gate_tx);
        });
    }

    #[test]
    fn join_future_is_pending_until_job_finishes_and_wakes_task() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let (gate_tx, gate_rx) = gate();
            occupy_workers(&pool, &gate_rx);

            let mut future = pin!(pool.spawn(|| 42).into_future());
            let wakes = Arc::new(CountingWaker::default());
            let waker = Waker::from(Arc::clone(&wakes));
            let mut cx = Context::from_waker(&waker);

            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert!(future.as_mut().poll(&mut cx).is_pending());
            assert_eq!(wakes.count(), 0);

            drop(gate_tx);
            assert!(wait_until(|| wakes.count() > 0), "waker was never woken");
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(42))));
        });
    }

    #[test]
    fn join_future_is_ready_immediately_for_finished_job() {
        let pool = Builder::new().size(1).build().unwrap();
        let handle = pool.spawn(|| "done");
        pool.wait();
        let mut future = pin!(handle.into_future());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Ok("done"))
        ));
    }

    #[test]
    fn join_future_can_be_awaited_on_another_thread() {
        with_timeout(|| {
            let pool = Builder::new().size(2).build().unwrap();
            let future = pool.spawn(|| vec![1, 2, 3]).into_future();
            let result = thread::spawn(move || block_on(future)).join().unwrap();
            assert_eq!(result.unwrap(), vec![1, 2, 3]);
        });
    }

    #[test]
    fn join_future_debug_is_implemented() {
        let pool = Builder::new().size(1).build().unwrap();
        let future = pool.spawn(|| 1).into_future();
        assert!(format!("{future:?}").contains("JoinFuture"));
    }

    // ---------------------------------------------------------------------
    // errors
    // ---------------------------------------------------------------------

    #[test]
    fn join_error_display() {
        assert_eq!(
            JoinError::Panicked(Box::new("x")).to_string(),
            "the job panicked: x"
        );
        assert_eq!(
            JoinError::Canceled.to_string(),
            "the job was dropped before it ran"
        );
    }

    #[test]
    fn join_error_display_includes_string_payload() {
        assert_eq!(
            JoinError::Panicked(Box::new(String::from("owned message"))).to_string(),
            "the job panicked: owned message"
        );
        assert_eq!(
            JoinError::Panicked(Box::new("")).to_string(),
            "the job panicked: "
        );
    }

    #[test]
    fn join_error_display_omits_non_string_payload() {
        assert_eq!(
            JoinError::Panicked(Box::new(42)).to_string(),
            "the job panicked"
        );
        assert_eq!(
            JoinError::Panicked(Box::new(())).to_string(),
            "the job panicked"
        );
        // a boxed box must not be mistaken for a string payload
        let nested: Box<dyn Any + Send> = Box::new("inner");
        assert_eq!(
            JoinError::Panicked(Box::new(nested)).to_string(),
            "the job panicked"
        );
    }

    #[test]
    fn join_error_display_shows_message_of_real_panics() {
        let pool = Builder::new().size(2).build().unwrap();
        let err = pool
            .spawn(|| -> i32 { panic!("static message") })
            .join()
            .unwrap_err();
        assert_eq!(err.to_string(), "the job panicked: static message");

        let n = 7;
        let err = pool
            .spawn(move || -> i32 { panic!("formatted message {n}") })
            .join()
            .unwrap_err();
        assert_eq!(err.to_string(), "the job panicked: formatted message 7");

        let err = pool.spawn(|| panic::panic_any(1u8)).join().unwrap_err();
        assert_eq!(err.to_string(), "the job panicked");
    }

    #[test]
    fn join_error_display_from_async_join() {
        with_timeout(|| {
            let pool = Builder::new().size(1).build().unwrap();
            let err = block_on(
                pool.spawn(|| -> i32 { panic!("async message") })
                    .into_future(),
            )
            .unwrap_err();
            assert_eq!(err.to_string(), "the job panicked: async message");
        });
    }

    #[test]
    fn join_error_debug_and_source() {
        let panicked = JoinError::Panicked(Box::new("x"));
        assert!(format!("{panicked:?}").contains("Panicked"));
        assert!(panicked.source().is_none());
        assert_eq!(format!("{:?}", JoinError::Canceled), "Canceled");
        assert!(JoinError::Canceled.source().is_none());
    }

    #[test]
    fn join_error_can_be_boxed_as_std_error() {
        let err: Box<dyn std::error::Error + Send> = Box::new(JoinError::Canceled);
        assert_eq!(err.to_string(), "the job was dropped before it ran");
    }

    #[test]
    fn try_error_display_and_traits() {
        let err = TryError;
        assert_eq!(
            err.to_string(),
            "adding job to the thread pool queue failed because the queue is full"
        );
        assert_eq!(format!("{err:?}"), "TryError");
        assert!(err.source().is_none());

        let copy = err;
        assert_eq!(copy, err);
        assert_eq!(hash_of(&copy), hash_of(&err));

        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(err);
        assert_eq!(boxed.to_string(), err.to_string());
    }

    #[test]
    fn join_to_result_maps_every_case() {
        assert!(matches!(join_to_result::<i32>(Ok(Ok(1))), Ok(1)));
        assert!(matches!(
            join_to_result::<i32>(Ok(Err(Box::new("p")))),
            Err(JoinError::Panicked(_))
        ));
        assert!(matches!(
            join_to_result::<i32>(Err(oneshot::RecvError)),
            Err(JoinError::Canceled)
        ));
    }
}
