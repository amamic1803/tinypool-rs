# tinypool

[<img alt="GitHub Repository Static Badge" src="https://img.shields.io/badge/GitHub-tinypool-blue?logo=github">](https://github.com/amamic1803/tinypool-rs)
[<img alt="Crates.io Version" src="https://img.shields.io/crates/v/tinypool?logo=rust">](https://crates.io/crates/tinypool)
[<img alt="docs.rs" src="https://img.shields.io/docsrs/tinypool?logo=docs.rs&label=docs.rs">](https://docs.rs/tinypool)
[<img alt="GitHub Actions Workflow Status" src="https://img.shields.io/github/actions/workflow/status/amamic1803/tinypool-rs/test.yml">](https://github.com/amamic1803/tinypool-rs/actions/workflows/test.yml)
[<img alt="GitHub License" src="https://img.shields.io/github/license/amamic1803/tinypool-rs">](https://github.com/amamic1803/tinypool-rs/blob/main/LICENSE)

***tinypool*** is a simple thread pool implementation in Rust.

A thread pool is a collection of threads that can be used to execute jobs concurrently.
The aim of this crate is to provide a simple, easy-to-use thread pool that can be used in any project.

### Example (1)
```rust
use tinypool::ThreadPool;
use std::sync::{Arc, Mutex};

let mut threadpool = ThreadPool::new().unwrap();
let counter = Arc::new(Mutex::new(0));

for _ in 0..100 {
    let counter_thrd = Arc::clone(&counter);
    threadpool.execute(move || {
        let mut counter = counter_thrd.lock().unwrap();
        *counter += 1;
    });
}

threadpool.join();
assert_eq!(*counter.lock().unwrap(), 100);
```

### Example (2)
```rust
use tinypool::ThreadPool;
use std::sync::mpsc;

let mut threadpool = ThreadPool::new().unwrap();
let (tx, rx) = mpsc::channel();

for i in 0..100 {
    let tx = tx.clone();
    threadpool.execute(move || tx.send(i).unwrap());
}
drop(tx);

let mut sum = 0;
while let Ok(i) = rx.recv() {
   sum += i;
}

assert_eq!(sum, 4950);
```

### Example (3)
```rust
use tinypool::ThreadPool;

let mut threadpool = ThreadPool::new().unwrap();

let mut join_handles = Vec::new();
for i in 0..100 {
    join_handles.push(threadpool.spawn(move || i));
}

let mut sum = join_handles.into_iter().map(|handle| handle.join().unwrap()).sum::<i32>();
assert_eq!(sum, 4950);
```

## OS support
Supports targets with threading support.

## License
This project is licensed under the [MIT License](https://github.com/amamic1803/tinypool-rs/blob/main/LICENSE).

## Contributing
Contributions are welcome!

Please open an issue or a pull request in the [GitHub repository](https://github.com/amamic1803/tinypool-rs).
