use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use std::{fs, hint::black_box, path::PathBuf, str::FromStr};
use tachyon_core::{Connection, ValueType};

const STREAM: &str = r#"inserter_stream{service = "web"}"#;

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

fn bench_insert(conn: &mut Connection, timestamps: &[u64], values: &[u64]) {
    let mut inserter = black_box(conn.prepare_insert(STREAM).unwrap());

    for i in 0..timestamps.len() {
        inserter
            .insert_uinteger64(timestamps[i], values[i])
            .unwrap();
    }

    inserter.flush().unwrap();
}

fn insert_benchmark(c: &mut Criterion) {
    let root_dir = PathBuf::from_str("../tmp").unwrap();

    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv");

    c.bench_function("tachyon: insert benchmark", |b| {
        b.iter(|| {
            let mut conn = Connection::new(root_dir.clone()).unwrap();
            conn.create_stream(STREAM, ValueType::UInteger64).unwrap();
            bench_insert(&mut conn, &timestamps, &values);
            fs::remove_dir_all(root_dir.clone()).unwrap();
        })
    });
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default()
        .sample_size(20)
        .with_profiler(PProfProfiler::new(10000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = insert_benches;
    config = get_config();
    targets = insert_benchmark,
);
criterion_main!(insert_benches);
