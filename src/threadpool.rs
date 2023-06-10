//! A thread pool that holds a number of threads and executes jobs on them.

//! tinypool-rs
//! A simple thread pool implementation in Rust




// STD LIBRARY IMPORTS

use std::{
    error::Error,
    fmt::Display,
    io,
    sync::{Arc, Condvar, mpsc, Mutex},
    thread,
};





// ENUMS

/// The messages that can be sent to the worker threads.
enum Message {
    /// A new job to execute.
    NewJob(Box<dyn FnOnce() + Send + 'static>),
    /// Terminate the worker thread.
    Terminate,
}


/// Errors that can occur when creating a thread pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadPoolError {
    /// Attempted to call a method on a stopped thread pool.
    PoolStopped,
    /// The thread pool failed to spawn a thread.
    ThreadSpawn,
}

impl Display for ThreadPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThreadPoolError::PoolStopped => write!(f, "Attempted to call a method on a stopped thread pool."),
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


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadPoolState {
    Running,
    Stopped,
}





// STRUCTS

/// A thread pool that holds a number of threads and executes jobs on them.
pub struct ThreadPool {
    /// The current state of the thread pool.
    state: ThreadPoolState,
    /// The workers in the thread pool.
    workers: Vec<Option<Worker>>,
    /// The sender for the worker threads.
    sender: mpsc::Sender<Message>,
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
    pub fn new(size: usize) -> Result<Self, ThreadPoolError> {
        let size: usize =
            if size == 0 {
                thread::available_parallelism()?.get()
            } else {
                size
            };

        let state = ThreadPoolState::Running;

        let (sender, receiver) = mpsc::channel::<Message>();
        let receiver = Arc::new(Mutex::new(receiver));

        let queued_jobs = Arc::new((Mutex::new(0), Condvar::new()));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Some(Worker::new(id, Arc::clone(&receiver), Arc::clone(&queued_jobs))?));
        }

        Ok(Self { state, workers, sender, queued_jobs })
    }

    /// Execute a closure on a worker thread.
    /// # Arguments
    /// * `f` - The closure to execute on a worker thread.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::PoolStopped``` will be returned if the thread pool is stopped.
    pub fn execute<F>(&self, f: F) -> Result<(), ThreadPoolError>
    where
        F: FnOnce() + Send + 'static,
    {
        match self.state {
            ThreadPoolState::Running => {
                *self.queued_jobs.0.lock().unwrap() += 1;
                self.sender.send(Message::NewJob(Box::new(f))).expect("Failed to send job message to worker thread. Shouldn't happen.");
                Ok(())
            },
            ThreadPoolState::Stopped => Err(ThreadPoolError::ThreadSpawn),
        }
    }

    /// Wait for all worker threads to finish and shut down the thread pool.
    /// This method will block until all queued jobs have finished.
    /// It will then shut down the thread pool. And change the state to ```ThreadPoolState::Stopped```.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::PoolStopped``` will be returned if the thread pool is stopped.
    pub fn join(&mut self) -> Result<(), ThreadPoolError> {
        match self.state {
            ThreadPoolState::Running => {
                for _ in &self.workers {
                    self.sender.send(Message::Terminate).expect("Failed to send terminate message to worker thread.")
                }
                for worker in &mut self.workers {
                    if let Some(worker) = worker.take() {
                        worker.thread.unwrap().join().expect("Failed to join worker thread.");
                    }
                }
                self.workers.clear();
                self.workers.shrink_to_fit();
                self.state = ThreadPoolState::Stopped;
                Ok(())
            },
            ThreadPoolState::Stopped => Err(ThreadPoolError::PoolStopped),
        }
    }

    /// Wait for all queued jobs to finish.
    /// This method will block until all queued jobs have finished.
    /// # Returns
    /// A ```Result``` containing ```()``` if successful, or a ```ThreadPoolError``` if unsuccessful.
    /// # Errors
    /// A ```ThreadPoolError::PoolStopped``` will be returned if the thread pool is stopped.
    pub fn wait(&self) -> Result<(), ThreadPoolError>{
        match self.state {
            ThreadPoolState::Running => {
                let mut queued_jobs = self.queued_jobs.0.lock().unwrap();
                while *queued_jobs > 0 {
                    queued_jobs = self.queued_jobs.1.wait(queued_jobs).unwrap();
                }
                Ok(())
            },
            ThreadPoolState::Stopped => Err(ThreadPoolError::PoolStopped),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.join().unwrap_or(());  // Ignore error
    }
}


struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {

    /// Create a new worker struct.
    /// # Arguments
    /// * `id` - The id of the worker.
    /// * `receiver` - The receiver end of the channel that the worker will listen to.
    /// # Returns
    /// A ```Result``` containing the ```Worker``` if successful, or a ```ThreadPoolError``` if error occurs.
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, queued_jobs: Arc<(Mutex<usize>, Condvar)>) -> Result<Self, ThreadPoolError> {
        let thread = Some(
            thread::Builder::new()
                .spawn(move || {
                    while let Message::NewJob(job) = receiver.lock().unwrap().recv().unwrap() {
                        job();
                        *queued_jobs.0.lock().unwrap() -= 1;
                        queued_jobs.1.notify_all();
                    };
                })?
        );

        Ok(Self { id, thread })
    }
}





mod tests {
    use super::*;

    #[test]
    fn test_new() {
        assert!(true);
    }
}
