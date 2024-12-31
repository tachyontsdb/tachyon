use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{hint::black_box, iter::zip, path::Path};
use tachyon_core::{tachyon_benchmarks::*, StreamId, ValueType, Version};

const NUM_ITEMS: u64 = 100000;

fn bench_write_sequential_timestamps(start: u64, end: u64) {
    let mut model = black_box(TimeDataFile::new(
        black_box(Version(0)),
        black_box(StreamId(0)),
        black_box(ValueType::UInteger64),
    ));
    for i in start..=end {
        model.write_data_to_file_in_mem(black_box(i), black_box((i + (i % 100)).into()));
    }
    model.write(black_box("../tmp/bench_sequential_write.ty".into()));
    std::fs::remove_file("../tmp/bench_sequential_write.ty").unwrap();
}

fn bench_write_dataset(timestamps: &[u64], values: &[u64], file: impl AsRef<Path>) {
    let mut model = black_box(TimeDataFile::new(
        black_box(Version(0)),
        black_box(StreamId(0)),
        black_box(ValueType::UInteger64),
    ));
    for (ts, v) in zip(timestamps, values) {
        model.write_data_to_file_in_mem(black_box(*ts), black_box((*v).into()));
    }
    model.write(black_box(file.as_ref().to_path_buf()));
    std::fs::remove_file(file.as_ref()).unwrap();
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

fn write_sequential(c: &mut Criterion) {
    // setup tachyon benchmark
    c.bench_function(&format!("tachyon: write sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_write_sequential_timestamps(0, NUM_ITEMS))
    });
}

fn write_memory_dataset(c: &mut Criterion) {
    let (timestamps, values) = read_from_csv("../data/memory_dataset.csv");
    c.bench_function(
        &format!(
            "tachyon: write memory dataset ({} entries)",
            timestamps.len()
        ),
        |b| {
            b.iter(|| {
                bench_write_dataset(&timestamps, &values, "../tmp/bench_write_memory_dataset.ty")
            })
        },
    );
}

fn write_voltage_dataset(c: &mut Criterion) {
    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv");
    c.bench_function(
        &format!(
            "tachyon: write voltage dataset ({} entries)",
            timestamps.len()
        ),
        |b| {
            b.iter(|| {
                bench_write_dataset(
                    &timestamps,
                    &values,
                    "../tmp/bench_write_voltage_dataset.ty",
                )
            })
        },
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
    targets = write_sequential, write_memory_dataset, write_voltage_dataset
);
criterion_main!(benches);
