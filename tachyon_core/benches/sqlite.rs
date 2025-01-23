use std::iter::zip;

use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use rusqlite::{Connection, Statement};

const NUM_ITEMS: u64 = 100000;

// TODO: Add black_box

#[derive(Debug)]
struct Item {
    timestamp: u64,
    value: u64,
}

fn read_from_csv(path: &str) -> (Vec<u64>, Vec<u64>) {
    println!("Reading from: {}", path);
    let mut rdr = Reader::from_path(path).unwrap();

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (i, result) in rdr.records().enumerate() {
        if i > 0 {
            let record = result.unwrap();
            timestamps.push(record[0].parse::<u64>().unwrap());
            values.push(record[1].parse::<u64>().unwrap());
        }
    }
    println!("Done reading from: {}\n", path);

    (timestamps, values)
}

fn bench_voltage_dataset(query: &str, conn: &mut Connection) {
    let mut stmt = conn.prepare(query).unwrap();
    let mut item_iter = stmt
        .query_map([], |row| {
            Ok(Item {
                timestamp: row.get(0).unwrap_or(0),
                value: row.get(1).unwrap_or(0),
            })
        })
        .unwrap();

    loop {
        let res = item_iter.next();
        if res.is_none() {
            break;
        }
    }
}

fn voltage_benchmark(c: &mut Criterion) {
    let queries = vec![
        r#"select timestamp, value from Item;"#,
        r#"select sum(value) from Item;"#,
        r#"select avg(value) from item;"#,
    ];

    let mut conn = Connection::open("../tmp/bench_voltage.sqlite").unwrap();

    conn.execute(
        "
        CREATE TABLE if not exists Item (
            timestamp INTEGER,
            value INTEGER
        )
        ",
        (),
    )
    .unwrap();

    let (timestamps, values) = read_from_csv("../data/voltage_dataset.csv");

    let transaction = conn.transaction().unwrap();
    let mut insert_stmt = transaction
        .prepare("INSERT INTO Item (timestamp, value) VALUES (?, ?)")
        .unwrap();

    for (t, v) in zip(timestamps, values) {
        insert_stmt.execute([&t, &v]).unwrap();
    }
    drop(insert_stmt);
    transaction.commit().unwrap();

    for query in queries {
        c.bench_function(&format!("sqlite: query benchmark for: {}", query), |b| {
            b.iter(|| bench_voltage_dataset(query, &mut conn))
        });
    }

    std::fs::remove_file("../tmp/bench_voltage.sqlite").unwrap();
}

fn bench_read_sqlite(stmt: &mut Statement) -> u64 {
    let item_iter = stmt
        .query_map([], |row| {
            Ok(Item {
                timestamp: row.get(0).unwrap(),
                value: row.get(1).unwrap(),
            })
        })
        .unwrap();

    let mut res = 0;
    for item in item_iter {
        let item = item.unwrap();
        res += item.timestamp + item.value;
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark

    // setup SQLite benchmark
    let mut conn = Connection::open(format!("../tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();
    conn.execute(
        "
        CREATE TABLE if not exists Item (
            timestamp INTEGER,
            value INTEGER
        )
        ",
        (),
    )
    .unwrap();

    let transaction = conn.transaction().unwrap();
    let mut insert_stmt = transaction
        .prepare("INSERT INTO Item (timestamp, value) VALUES (?, ?)")
        .unwrap();

    for i in 0..NUM_ITEMS {
        insert_stmt.execute([&i, &(i + (i % 100))]).unwrap();
    }
    drop(insert_stmt);
    transaction.commit().unwrap();

    let mut stmt = conn.prepare("SELECT * FROM Item").unwrap();
    c.bench_function(&format!("SQLite: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sqlite(&mut stmt))
    });
    std::fs::remove_file(format!("../tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();
}

fn get_config() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(Some(options))))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark,voltage_benchmark
);
criterion_main!(benches);
