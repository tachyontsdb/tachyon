use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use tachyon_core::{Connection, ReturnType, ValueType};

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

fn bench_query(query: &str, start: Option<u64>, end: Option<u64>, conn: &mut Connection) {
    let mut stmt = conn.prepare_query(query, start, end);

    match stmt.return_type() {
        ReturnType::Scalar => {
            stmt.next_scalar().unwrap();
        }
        ReturnType::Vector => loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
        },
    }
}

fn vector_selector_benchmark(c: &mut Criterion) {
    let root_dir = PathBuf::from_str("../tmp").unwrap();
    fs::create_dir_all(&root_dir).unwrap();

    const STREAM: &str = r#"http_requests_total{service = "web"}"#;

    let queries = vec![
        STREAM,
        r#"sum(http_requests_total{service = "web"})"#,
        r#"count(http_requests_total{service = "web"})"#,
        r#"avg(http_requests_total{service = "web"})"#,
        r#"min(http_requests_total{service = "web"})"#,
        r#"max(http_requests_total{service = "web"})"#,
        r#"bottomk(1000, http_requests_total{service = "web"})"#,
        r#"topk(1000, http_requests_total{service = "web"})"#,
    ];

    let mut conn = Connection::new(root_dir.clone());

    if !conn.check_stream_exists(STREAM) {
        conn.create_stream(STREAM, ValueType::UInteger64);
    }

    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv");

    let mut inserter = conn.prepare_insert(STREAM);

    for i in 0..timestamps.len() {
        inserter.insert_uinteger64(timestamps[i], values[i]);
    }

    inserter.flush();

    for query in queries {
        c.bench_function(&format!("tachyon: query benchmark for: {}", query), |b| {
            b.iter(|| bench_query(query, Some(0), Some(1300000000), &mut conn))
        });
    }

    fs::remove_dir_all(root_dir).unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(10000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = vector_selector_benchmark,
);
criterion_main!(benches);
