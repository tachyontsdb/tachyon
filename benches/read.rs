use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rusqlite::Connection;
use std::{fs::File, path::Path, sync::Arc};
use tachyon::storage::{file::*, page_cache::PageCache};

const NUM_ITEMS: u64 = 100000;

#[derive(Debug)]
struct Item {
    timestamp: u64,
    value: u64,
}

fn bench_read_sqlite(conn: &Connection) -> u64 {
    let mut stmt = conn.prepare("SELECT * FROM Item").unwrap();
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
        res += item.timestamp * item.value;
    }
    res
}

fn bench_read_sequential_timestamps(start: u64, end: u64, page_cache: &mut PageCache) -> u64 {
    let file_paths = Arc::new(["./tmp/bench_sequential_read.ty".into()]);
    let cursor = Cursor::new(file_paths, start, end, page_cache).unwrap();

    let mut res = 0;
    for (timestamp, value) in cursor {
        res += timestamp + value;
    }
    res
}

fn criterion_benchmark(c: &mut Criterion) {
    // setup tachyon benchmark
    let mut model = TimeDataFile::new();
    for i in 0..NUM_ITEMS {
        model.write_data_to_file_in_mem(i, i + (i + 100));
    }
    model.write("./tmp/bench_sequential_read.ty".into());
    let mut page_cache = PageCache::new(1000);
    c.bench_function(&format!("tachyon: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sequential_timestamps(0, NUM_ITEMS, &mut page_cache))
    });

    // setup SQLite benchmark
    let conn = Connection::open(format!("./tmp/bench_sql_{}.sqlite", NUM_ITEMS)).unwrap();

    if !Path::new(&format!("./tmp/bench_sql_{}.exists", NUM_ITEMS)).exists() {
        conn.execute(
            "
            CREATE TABLE Item (
                timestamp INTEGER,
                value INTEGER
            )
            ",
            (),
        )
        .unwrap();
        for i in 0..NUM_ITEMS {
            let item = Item {
                timestamp: i,
                value: i + (i % 100),
            };
            conn.execute(
                "
            INSERT INTO Item (timestamp, value) VALUES (?1, ?2);
            ",
                (&item.timestamp, &item.value),
            )
            .unwrap();
        }
        File::create(format!("./tmp/bench_sql_{}.exists", NUM_ITEMS)).unwrap();
    }

    c.bench_function(&format!("SQLite: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sqlite(&conn))
    });

    // conn.execute(
    //     "
    //         DROP TABLE Item;
    //     ",
    //     (),
    // )
    // .unwrap();

    std::fs::remove_file("./tmp/bench_sequential_read.ty").unwrap();
}

fn get_config() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(50, Output::Flamegraph(None)))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark
);
criterion_main!(benches);
