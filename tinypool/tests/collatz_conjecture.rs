use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tinypool::ThreadPool;

/// Returns the number of steps and the maximum number reached
/// in the Collatz conjecture for a given starting number.
/// # Arguments
/// * `n` - The starting number for the Collatz conjecture.
/// # Returns
/// * A tuple containing the number of steps and the maximum number reached.
fn collatz_conjecture(mut n: u64) -> (u64, u64) {
    let mut count = 0;
    let mut max = n;
    while n != 1 {
        if n.is_multiple_of(2) {
            n /= 2;
        } else {
            n = 3 * n + 1;
        };
        count += 1;
        if n > max {
            max = n;
        }
    }
    (count, max)
}

fn collatz_channel(n: u64, max_index: u64, max_steps: u64, max_number: u64) {
    let pool = ThreadPool::default();
    let (tx, rx) = mpsc::channel();

    for i in 1..(n + 1) {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        });
    }
    drop(tx);

    let mut results = Vec::with_capacity(n as usize);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    let max_steps_result = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps_result, (max_index, max_steps, max_number));
}

fn collatz_mutex(n: u64, max_index: u64, max_steps: u64, max_number: u64) {
    let pool = ThreadPool::default();
    let results: Arc<Mutex<Vec<(u64, u64, u64)>>> =
        Arc::new(Mutex::new(Vec::with_capacity(n as usize)));

    let per_thread = n as usize / pool.size();

    for i in pool.size() * per_thread..(n as usize + 1) {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u64);
            results.lock().unwrap().push((i as u64, result.0, result.1));
        });
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u64, u64, u64)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..(i + 1) * per_thread {
                let result = collatz_conjecture(n as u64);
                temp_results.push((n as u64, result.0, result.1));
            }
            results.lock().unwrap().extend(temp_results);
        });
    }
    pool.wait();

    let mut results = results.lock().unwrap();
    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    let max_steps_result = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps_result, (max_index, max_steps, max_number));
}

fn collatz_join(n: u64, max_index: u64, max_steps: u64, max_number: u64) {
    let pool = ThreadPool::default();

    let mut results = Vec::with_capacity(n as usize);
    for i in 1..(n + 1) {
        results.push(pool.spawn(move || {
            let result = collatz_conjecture(i);
            (i, result.0, result.1)
        }));
    }
    let mut results = results
        .into_iter()
        .map(|r| r.join().unwrap())
        .collect::<Vec<_>>();

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), n as usize);

    let max_steps_result = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps_result, (max_index, max_steps, max_number));
}

#[test]
fn collatz_1000_channel() {
    collatz_channel(1000, 871, 178, 190_996);
}
#[test]
fn collatz_10_000_channel() {
    collatz_channel(10_000, 6171, 261, 975_400);
}
#[test]
fn collatz_100_000_channel() {
    collatz_channel(100_000, 77031, 350, 21_933_016);
}
#[test]
fn collatz_1_000_000_channel() {
    collatz_channel(1_000_000, 837799, 524, 2_974_984_576);
}
#[test]
fn collatz_10_000_000_channel() {
    collatz_channel(10_000_000, 8400511, 685, 159_424_614_880);
}

#[test]
fn collatz_1000_mutex() {
    collatz_mutex(1000, 871, 178, 190_996);
}
#[test]
fn collatz_10_000_mutex() {
    collatz_mutex(10_000, 6171, 261, 975_400);
}
#[test]
fn collatz_100_000_mutex() {
    collatz_mutex(100_000, 77031, 350, 21_933_016);
}
#[test]
fn collatz_1_000_000_mutex() {
    collatz_mutex(1_000_000, 837799, 524, 2_974_984_576);
}
#[test]
fn collatz_10_000_000_mutex() {
    collatz_mutex(10_000_000, 8400511, 685, 159_424_614_880);
}

#[test]
fn collatz_1000_join() {
    collatz_join(1000, 871, 178, 190_996);
}
#[test]
fn collatz_10_000_join() {
    collatz_join(10_000, 6171, 261, 975_400);
}
#[test]
fn collatz_100_000_join() {
    collatz_join(100_000, 77031, 350, 21_933_016);
}
#[test]
fn collatz_1_000_000_join() {
    collatz_join(1_000_000, 837799, 524, 2_974_984_576);
}
#[test]
fn collatz_10_000_000_join() {
    collatz_join(10_000_000, 8400511, 685, 159_424_614_880);
}
