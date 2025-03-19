use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{cell::RefCell, hint::black_box, iter::zip, rc::Rc};
use tachyon_core::{tachyon_benchmarks::*, StreamId, Value, ValueType, Version};

const NUM_ITEMS: u64 = 100000;

// Generic function for reading values from cursor and processing them differently based on ValueType
fn bench_read_sequential_timestamps(
    file_path: &str,
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
    value_type: ValueType,
) -> u64 {
    let file_paths = vec![file_path.into()];
    let cursor = black_box(
        Cursor::new(
            black_box(file_paths),
            black_box(start),
            black_box(end),
            black_box(page_cache),
            black_box(ScanHint::None),
        )
        .unwrap(),
    );

    let mut res = 0;
    for vector in cursor {
        let value: u64 = match value_type {
            ValueType::UInteger64 => black_box(vector.value.get_uinteger64()),
            ValueType::Float64 => black_box(vector.value.get_float64() as u64),
            _ => panic!("Unsupported value type"),
        };
        res += vector.timestamp + value;
    }
    res
}

fn bench_read_voltage_dataset(
    file_path: &str,
    page_cache: Rc<RefCell<PageCache>>,
    value_type: ValueType,
) -> u128 {
    let file_paths = vec![file_path.into()];
    let cursor = black_box(
        Cursor::new(
            black_box(file_paths),
            black_box(0),
            black_box(u64::MAX),
            black_box(page_cache),
            black_box(ScanHint::None),
        )
        .unwrap(),
    );

    let mut res = 0u128;
    for vector in cursor {
        let value: u64 = match value_type {
            ValueType::UInteger64 => black_box(vector.value.get_uinteger64()),
            ValueType::Float64 => black_box(vector.value.get_float64() as u64),
            _ => panic!("Unsupported value type"),
        };
        res += (vector.timestamp + value) as u128;
    }
    res
}

fn read_from_csv(path: &str, as_float: bool) -> (Vec<u64>, Vec<Value>) {
    println!("Reading from: {}", path);
    let mut rdr = Reader::from_path(path).unwrap();

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for result in rdr.records() {
        let record = result.unwrap();
        timestamps.push(record[0].parse::<u64>().unwrap());

        // Read the value as u64 or f64 based on the as_float flag
        let value = if as_float {
            let float_val = record[1].parse::<u64>().unwrap() as f64;
            Value::from(float_val)
        } else {
            let int_val = record[1].parse::<u64>().unwrap();
            Value::from(int_val)
        };
        values.push(value);
    }
    println!("Done reading from: {}\n", path);

    (timestamps, values)
}

// Benchmark for sequential timestamps with u64 values
fn sequential_benchmark_u64(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, (i + (i % 100)).into());
    }
    model.write("../tmp/bench_sequential_read_u64.ty".into());
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));
    c.bench_function(
        &format!("tachyon: read sequential uint64 0-{}", NUM_ITEMS),
        |b| {
            b.iter(|| {
                bench_read_sequential_timestamps(
                    "../tmp/bench_sequential_read_u64.ty",
                    0,
                    NUM_ITEMS,
                    page_cache.clone(),
                    ValueType::UInteger64,
                )
            })
        },
    );
    // std::fs::remove_file("../tmp/bench_sequential_read_u64.ty").unwrap();
}

// Benchmark for sequential timestamps with f64 values
fn sequential_benchmark_f64(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::Float64);
    for i in 0..NUM_ITEMS {
        let value = (i + (i % 100)) as f64;
        model.write_data_to_file_in_mem(i, value.into());
    }
    model.write("../tmp/bench_sequential_read_f64.ty".into());
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));
    c.bench_function(
        &format!("tachyon: read sequential float64 0-{}", NUM_ITEMS),
        |b| {
            b.iter(|| {
                bench_read_sequential_timestamps(
                    "../tmp/bench_sequential_read_f64.ty",
                    0,
                    NUM_ITEMS,
                    page_cache.clone(),
                    ValueType::Float64,
                )
            })
        },
    );
    // std::fs::remove_file("../tmp/bench_sequential_read_f64.ty").unwrap();
}

// Benchmark for voltage dataset with u64 values
fn voltage_benchmark_u64(c: &mut Criterion) {
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));

    // set up voltage benchmark
    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv", false);
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
    for (ts, v) in zip(&timestamps, &values) {
        model.write_data_to_file_in_mem(*ts, *v);
    }
    model.write("../tmp/bench_voltage_read_u64.ty".into());

    c.bench_function(
        &format!(
            "tachyon: read voltage dataset uint64 ({} entries)",
            timestamps.len()
        ),
        |b| {
            b.iter(|| {
                bench_read_voltage_dataset(
                    "../tmp/bench_voltage_read_u64.ty",
                    page_cache.clone(),
                    ValueType::UInteger64,
                )
            })
        },
    );
    // std::fs::remove_file("../tmp/bench_voltage_read_u64.ty").unwrap();
}

// Benchmark for voltage dataset with f64 values
fn voltage_benchmark_f64(c: &mut Criterion) {
    let page_cache = Rc::new(RefCell::new(PageCache::new(256)));

    // set up voltage benchmark
    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv", true);
    let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::Float64);
    for (ts, v) in zip(&timestamps, &values) {
        model.write_data_to_file_in_mem(*ts, *v);
    }
    model.write("../tmp/bench_voltage_read_f64.ty".into());

    c.bench_function(
        &format!(
            "tachyon: read voltage dataset float64 ({} entries)",
            timestamps.len()
        ),
        |b| {
            b.iter(|| {
                bench_read_voltage_dataset(
                    "../tmp/bench_voltage_read_f64.ty",
                    page_cache.clone(),
                    ValueType::Float64,
                )
            })
        },
    );
    // std::fs::remove_file("../tmp/bench_voltage_read_f64.ty").unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = sequential_benchmark_u64, sequential_benchmark_f64, voltage_benchmark_u64, voltage_benchmark_f64
);
criterion_main!(benches);
