use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use tachyon::storage::file::*;

const NUM_ITEMS: u64 = 100000;

fn bench_write_sequential_timestamps(start: u64, end: u64) {
    let mut model = TimeDataFile::new();
    for i in start..=end {
        model.write_data_to_file_in_mem(i, i + (i % 100));
    }
    model.write("./tmp/bench_sequential_write.ty".into());
    std::fs::remove_file("./tmp/bench_sequential_write.ty").unwrap();
}

fn bench_write_memory_dataset(timestamps: &[u64], values: &[u64]) {
    let mut model = TimeDataFile::new();
    for i in 0..timestamps.len() {
        model.write_data_to_file_in_mem(timestamps[i], values[i]);
    }
    model.write("./tmp/bench_write_memory_dataset.ty".into());
    std::fs::remove_file("./tmp/bench_write_memory_dataset.ty").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark
    c.bench_function(&format!("tachyon: write sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_write_sequential_timestamps(0, NUM_ITEMS))
    });

    let mut rdr = Reader::from_path("./data/memory_dataset.csv").unwrap();

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (i, result) in rdr.records().enumerate() {
        if i > 0 {
            let record = result.unwrap();
            timestamps.push(record[0].parse::<u64>().unwrap());
            values.push(record[1].parse::<u64>().unwrap());
        }
    }
    c.bench_function(
        &format!("tachyon: write memory dataset 0-{}", NUM_ITEMS),
        |b| b.iter(|| bench_write_memory_dataset(&timestamps, &values)),
    );
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
