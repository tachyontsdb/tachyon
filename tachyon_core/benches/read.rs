use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, iter::zip, rc::Rc};
use tachyon_core::storage::{file::*, page_cache::PageCache};

const NUM_ITEMS: u64 = 100000;

fn bench_read_sequential_timestamps(
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
) -> u64 {
    let file_paths = vec!["../tmp/bench_sequential_read.ty".into()];
    let cursor = Cursor::new(file_paths, start, end, page_cache, ScanHint::None).unwrap();

    let mut res = 0;
    for (timestamp, value) in cursor {
        res += timestamp + value;
    }
    res
}

fn bench_read_voltage_dataset(page_cache: Rc<RefCell<PageCache>>) -> u128 {
    let file_paths = vec!["../tmp/bench_voltage_read.ty".into()];
    let cursor = Cursor::new(file_paths, 0, u64::MAX, page_cache, ScanHint::None).unwrap();

    let mut res: u128 = 0;
    for (timestamp, value) in cursor {
        res += (timestamp + value) as u128;
    }
    res
}

fn read_from_csv(path: &str) -> (Vec<u64>, Vec<u64>) {
    println!("Reading from: {}", path);
    let mut rdr = Reader::from_path(path).unwrap();

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for result in rdr.records() {
        let record = result.unwrap();
        timestamps.push(record[0].parse::<u64>().unwrap());
        values.push(record[1].parse::<u64>().unwrap());
    }
    println!("Done reading from: {}\n", path);

    (timestamps, values)
}

fn sequential_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new();
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, i + (i % 100));
    }
    model.write("../tmp/bench_sequential_read.ty".into());
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));
    c.bench_function(&format!("tachyon: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sequential_timestamps(0, NUM_ITEMS, page_cache.clone()))
    });
    std::fs::remove_file("../tmp/bench_sequential_read.ty").unwrap();
}

fn voltage_benchmark(c: &mut Criterion) {
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));

    // set up voltage benchmark
    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv");
    let mut model = TimeDataFile::new();
    for (ts, v) in zip(&timestamps, &values) {
        model.write_data_to_file_in_mem(*ts, *v);
    }
    model.write("../tmp/bench_voltage_read.ty".into());

    c.bench_function(
        &format!(
            "tachyon: read voltage dataset ({} entries)",
            timestamps.len()
        ),
        |b| b.iter(|| bench_read_voltage_dataset(page_cache.clone())),
    );
    std::fs::remove_file("../tmp/bench_voltage_read.ty").unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = sequential_benchmark, voltage_benchmark
);
criterion_main!(benches);
