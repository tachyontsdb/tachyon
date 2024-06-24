use criterion::{criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, rc::Rc};
use tachyon::storage::{file::*, page_cache::PageCache};

const NUM_ITEMS: u64 = 100000;

fn bench_read_sequential_timestamps(
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
) -> u64 {
    let file_paths = Rc::new(["./tmp/bench_sequential_read.ty".into()]);
    let cursor = Cursor::new(file_paths, start, end, page_cache, ScanHint::None).unwrap();

    let mut res = 0;
    for (timestamp, value) in cursor {
        res += timestamp + value;
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new();
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, i + (i % 100));
    }
    model.write("./tmp/bench_sequential_read.ty".into());
    let page_cache = Rc::new(RefCell::new(PageCache::new(512)));
    c.bench_function(&format!("tachyon: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sequential_timestamps(0, NUM_ITEMS, page_cache.clone()))
    });
    std::fs::remove_file("./tmp/bench_sequential_read.ty").unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark
);
criterion_main!(benches);
