use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use pprof::criterion::{Output, PProfProfiler};
use rusqlite::{Connection, Statement};
use std::{cell::RefCell, rc::Rc};
use tachyon::storage::{file::*, page_cache::PageCache};

const NUM_ITEMS: u64 = 100_000_000;

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

fn bench_read_sequential_timestamps(
    start: u64,
    end: u64,
    page_cache: Rc<RefCell<PageCache>>,
) -> u64 {
    let file_paths = Rc::new(["./tmp/bench_sequential_read.ty".into()]);
    let cursor = Cursor::new(file_paths, start, end, page_cache).unwrap();

    let mut res = 0;
    for (timestamp, value) in cursor {
        res += timestamp + value;
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("long-read");
    group.sampling_mode(SamplingMode::Flat).sample_size(10);

    // setup tachyon benchmark
    let mut model = TimeDataFile::new();
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, i + (i + 100));
    }
    model.write("./tmp/bench_sequential_read.ty".into());
    let page_cache = Rc::new(RefCell::new(PageCache::new(1000)));
    group.bench_function(&format!("tachyon: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sequential_timestamps(0, NUM_ITEMS, page_cache.clone()))
    });

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
    group.bench_function(&format!("SQLite: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sqlite(&mut stmt))
    });

    std::fs::remove_file("./tmp/bench_sequential_read.ty").unwrap();
    std::fs::remove_file(format!("./tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();

    group.finish()
}

fn get_config() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark,
);
criterion_main!(benches);
