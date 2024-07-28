use std::{fs, path::PathBuf, str::FromStr};

use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use tachyon::api::{Connection, TachyonResultType};

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
    let mut stmt = conn.prepare(query, start, end);

    match stmt.return_type() {
        TachyonResultType::Scalar => {
            stmt.next_scalar().unwrap();
        }
        TachyonResultType::Vector => loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
        },
        TachyonResultType::Done => panic!("Invalid result type!"),
    }
}

fn vector_selector_benchmark(c: &mut Criterion) {
    let root_dir = PathBuf::from_str("./tmp/db").unwrap();
    fs::create_dir_all(&root_dir).unwrap();

    let queries = vec![
        r#"http_requests_total{service = "web"}"#,
        r#"sum(http_requests_total{service = "web"})"#,
        r#"count(http_requests_total{service = "web"})"#,
        r#"avg(http_requests_total{service = "web"})"#,
        r#"min(http_requests_total{service = "web"})"#,
        r#"max(http_requests_total{service = "web"})"#,
        r#"bottomk(1000, http_requests_total{service = "web"})"#,
        r#"topk(1000, http_requests_total{service = "web"})"#,
    ];

    let mut conn = Connection::new(root_dir.clone());

    let (timestamps, values) = read_from_csv("./data/voltage_dataset.csv");

    let mut batch_writer = conn.batch_insert(r#"http_requests_total{service = "web"}"#);

    for i in 0..timestamps.len() {
        batch_writer.insert(timestamps[i], values[i]);
    }

    conn.writer.flush_all();

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
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = vector_selector_benchmark,
);
criterion_main!(benches);
