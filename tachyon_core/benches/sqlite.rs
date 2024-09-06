use criterion::{criterion_group, criterion_main, Criterion};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use rusqlite::{Connection, Statement};

const NUM_ITEMS: u64 = 100000;

#[derive(Debug)]
struct Item {
    timestamp: u64,
    value: u64,
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
    let mut conn = Connection::open(format!("./tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();
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
        let item = Item {
            timestamp: i,
            value: i + (i % 100),
        };
        insert_stmt.execute([&item.timestamp, &item.value]).unwrap();
    }
    drop(insert_stmt);
    transaction.commit().unwrap();

    let mut stmt = conn.prepare("SELECT * FROM Item").unwrap();
    c.bench_function(&format!("SQLite: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sqlite(&mut stmt))
    });
    std::fs::remove_file(format!("./tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();
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
