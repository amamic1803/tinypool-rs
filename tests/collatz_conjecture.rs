use tinypool::ThreadPool;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};





#[test]
fn collatz_1000_channel() {
    let pool = ThreadPool::new(0).unwrap();
    let (tx, rx) = mpsc::channel();

    for i in 1..1001 {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        }).unwrap();
    }
    drop(tx);

    let mut results = Vec::with_capacity(1000);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 1000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 1000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (871, 178, 190_996));
}

#[test]
fn collatz_10_000_channel() {
    let pool = ThreadPool::new(0).unwrap();
    let (tx, rx) = mpsc::channel();

    for i in 1..10001 {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        }).unwrap();
    }
    drop(tx);

    let mut results = Vec::with_capacity(10000);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 10000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 10000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (6171, 261, 975_400));
}

#[test]
fn collatz_100_000_channel() {
    let pool = ThreadPool::new(0).unwrap();
    let (tx, rx) = mpsc::channel();

    for i in 1..100001 {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        }).unwrap();
    }
    drop(tx);

    let mut results = Vec::with_capacity(100000);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 100000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 100000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (77031, 350, 21_933_016));
}

#[test]
fn collatz_1_000_000_channel() {
    let pool = ThreadPool::new(0).unwrap();
    let (tx, rx) = mpsc::channel();

    for i in 1..1000001 {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        }).unwrap();
    }
    drop(tx);

    let mut results = Vec::with_capacity(1000000);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 1_000_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 1_000_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (837799, 524, 2_974_984_576));
}

#[test]
fn collatz_10_000_000_channel() {
    let pool = ThreadPool::new(0).unwrap();
    let (tx, rx) = mpsc::channel();

    for i in 1..10000001 {
        let tx = tx.clone();
        pool.execute(move || {
            let result = collatz_conjecture(i);
            tx.send((i, result.0, result.1)).unwrap();
        }).unwrap();
    }
    drop(tx);

    let mut results = Vec::with_capacity(10000000);
    while let Ok(result) = rx.recv() {
        results.push(result);
    }

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 10_000_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 10_000_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (8400511, 685, 159_424_614_880));
}

#[test]
fn collatz_1000_mutex() {
    let pool = ThreadPool::new(0).unwrap();
    let results: Arc<Mutex<Vec<(u128, u128, u128)>>> = Arc::new(Mutex::new(Vec::with_capacity(1000)));

    let per_thread = 1000 / pool.size();

    for i in pool.size() * per_thread..1001 {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u128);
            results.lock().unwrap().push((i as u128, result.0, result.1));
        }).unwrap();
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u128, u128, u128)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..(i + 1) * per_thread {
                let result = collatz_conjecture(n as u128);
                temp_results.push((n as u128, result.0, result.1));
            }
            results.lock().unwrap().extend(temp_results);
        }).unwrap();
    }
    pool.wait();

    let mut results = results.lock().unwrap();
    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 1000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 1000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (871, 178, 190_996));
}

#[test]
fn collatz_10_000_mutex() {
    let pool = ThreadPool::new(0).unwrap();
    let results: Arc<Mutex<Vec<(u128, u128, u128)>>> = Arc::new(Mutex::new(Vec::with_capacity(10_000)));

    let per_thread = 10_000 / pool.size();

    for i in pool.size() * per_thread..10_001 {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u128);
            results.lock().unwrap().push((i as u128, result.0, result.1));
        }).unwrap();
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u128, u128, u128)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..(i + 1) * per_thread {
                let result = collatz_conjecture(n as u128);
                temp_results.push((n as u128, result.0, result.1));
            }
            results.lock().unwrap().extend(temp_results);
        }).unwrap();
    }
    pool.wait();

    let mut results = results.lock().unwrap();
    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 10_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 10_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (6171, 261, 975_400));
}

#[test]
fn collatz_100_000_mutex() {
    let pool = ThreadPool::new(0).unwrap();
    let results: Arc<Mutex<Vec<(u128, u128, u128)>>> = Arc::new(Mutex::new(Vec::with_capacity(100_000)));

    let per_thread = 100_000 / pool.size();

    for i in pool.size() * per_thread..100_001 {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u128);
            results.lock().unwrap().push((i as u128, result.0, result.1));
        }).unwrap();
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u128, u128, u128)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..(i + 1) * per_thread {
                let result = collatz_conjecture(n as u128);
                temp_results.push((n as u128, result.0, result.1));
            }
            results.lock().unwrap().extend(temp_results);
        }).unwrap();
    }
    pool.wait();

    let mut results = results.lock().unwrap();
    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 100_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 100_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (77031, 350, 21_933_016));
}

#[test]
fn collatz_1_000_000_mutex() {
    let pool = ThreadPool::new(0).unwrap();
    let results: Arc<Mutex<Vec<(u128, u128, u128)>>> = Arc::new(Mutex::new(Vec::with_capacity(1000000)));

    let per_thread = 1_000_000 / pool.size();

    for i in pool.size() * per_thread..1_000_001 {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u128);
            results.lock().unwrap().push((i as u128, result.0, result.1));
        }).unwrap();
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u128, u128, u128)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..(i + 1) * per_thread {
                let result = collatz_conjecture(n as u128);
                temp_results.push((n as u128, result.0, result.1));
            }
            results.lock().unwrap().extend(temp_results);
        }).unwrap();
    }
    pool.wait();

    let mut results = results.lock().unwrap();
    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 1_000_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 1_000_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (837799, 524, 2_974_984_576));
}

#[test]
fn collatz_10_000_000_mutex() {
    let pool = ThreadPool::new(0).unwrap();
    let results: Arc<Mutex<Vec<(u128, u128, u128)>>> = Arc::new(Mutex::new(Vec::with_capacity(10000000)));

    let per_thread = 10_000_000 / pool.size();

    for i in pool.size() * per_thread..10_000_001 {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let result = collatz_conjecture(i as u128);
            results.lock().unwrap().push((i as u128, result.0, result.1));
        }).unwrap();
    }

    for i in 0..pool.size() {
        let results = Arc::clone(&results);
        pool.execute(move || {
            let mut temp_results: Vec<(u128, u128, u128)> = Vec::with_capacity(per_thread);
            let start = if i != 0 { i * per_thread } else { 1 };
            for n in start..((i + 1) * per_thread) {
                let res = collatz_conjecture(n as u128);
                temp_results.push((n as u128, res.0, res.1));
            }
            results.lock().unwrap().extend(temp_results);
        }).unwrap();
    }

    pool.wait();

    let mut results = results.lock().unwrap();

    results.sort_unstable_by_key(|&(i, _, _)| i);
    assert_eq!(results.len(), 10_000_000);

    results.dedup_by_key(|&mut (i, _, _)| i);
    assert_eq!(results.len(), 10_000_000);

    let max_steps = *results.iter().max_by_key(|&&(_, steps, _)| steps).unwrap();
    assert_eq!(max_steps, (8400511, 685, 159_424_614_880));
}





fn collatz_conjecture(mut n: u128) -> (u128, u128) {
    let mut count = 0;
    let mut max = n;
    while n != 1 {
        n = collatz_move(n);
        count += 1;
        if n > max {
            max = n;
        }
    }
    (count, max)
}

fn collatz_move(n: u128) -> u128 {
    if n % 2 == 0 {
        n / 2
    } else {
        3 * n + 1
    }
}
