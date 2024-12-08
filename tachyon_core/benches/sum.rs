use criterion::{criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, path::PathBuf, rc::Rc};
use tachyon_core::{tachyon_benchmarks::*, StreamId, ValueType, Version};

const NUM_ITEMS: u64 = 10000000;

fn bench_sum_sequential_timestamps(
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
    file_paths: Vec<PathBuf>,
) -> u64 {
    let mut cursor = Cursor::new(file_paths, start, end, page_cache, ScanHint::None).unwrap();

    let mut res = 0;
    loop {
        let vector = cursor.fetch();
        res += vector.value.get_uinteger64();
        if cursor.next().is_none() {
            break;
        }
    }
    res
}

fn bench_sum_sequential_timestamps_with_hint(
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
    file_paths: Vec<PathBuf>,
) -> u64 {
    let mut cursor = Cursor::new(file_paths, start, end, page_cache, ScanHint::Sum).unwrap();

    let mut res = 0;

    loop {
        let vector = cursor.fetch();
        res += vector.value.get_uinteger64();
        if cursor.next().is_none() {
            break;
        }
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
    for i in 0..NUM_ITEMS / 3 {
        model.write_data_to_file_in_mem(i, (i + (i % 100)).into());
    }
    model.write("../tmp/bench_sequential_sum.ty".into());

    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
    for i in NUM_ITEMS / 3..2 * NUM_ITEMS / 3 {
        model.write_data_to_file_in_mem(i, (100000 - i + (i % 10)).into());
    }
    model.write("../tmp/bench_sequential_sum_2.ty".into());

    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
    for i in 2 * NUM_ITEMS / 3..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, (9000 - i + (i % 10)).into());
    }
    model.write("../tmp/bench_sequential_sum_3.ty".into());

    let page_cache = Rc::new(RefCell::new(PageCache::new(512)));
    let file_paths = vec![
        "../tmp/bench_sequential_sum.ty".into(),
        "../tmp/bench_sequential_sum_2.ty".into(),
        "../tmp/bench_sequential_sum_3.ty".into(),
    ];

    c.bench_function(&format!("tachyon: sum sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| {
            bench_sum_sequential_timestamps(0, NUM_ITEMS, page_cache.clone(), file_paths.clone())
        })
    });
    c.bench_function(
        &format!("tachyon: sum sequential with hint 0-{}", NUM_ITEMS),
        |b| {
            b.iter(|| {
                bench_sum_sequential_timestamps_with_hint(
                    0,
                    NUM_ITEMS,
                    page_cache.clone(),
                    file_paths.clone(),
                )
            })
        },
    );

    std::fs::remove_file("../tmp/bench_sequential_sum.ty").unwrap();
    std::fs::remove_file("../tmp/bench_sequential_sum_2.ty").unwrap();
    std::fs::remove_file("../tmp/bench_sequential_sum_3.ty").unwrap();
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
