use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use rusqlite::Connection;
use std::{cell::RefCell, fs::File, path::Path, sync::Arc};
use tachyon::storage::{file::*, page_cache::PageCache};

const NUM_ITEMS: u64 = 100000;
const FREQUENCY: i32 = 5000;

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

fn bench_read_sequential_timestamps(
    start: u64,
    end: u64,
    page_cache: Arc<RefCell<PageCache>>,
) -> u64 {
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

    let page_cache = Arc::new(RefCell::new(PageCache::new(1000)));
    c.bench_function(&format!("tachyon: read sequential 0-{}", NUM_ITEMS), |b| {
        b.iter(|| bench_read_sequential_timestamps(0, NUM_ITEMS, page_cache.clone()))
    });

    // setup SQLite benchmark
    let conn = Connection::open("./tmp/bench_sql.sqlite").unwrap();

    if !Path::new("./tmp/bench_sql.exists").exists() {
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
        File::create("./tmp/bench_sql.exists").unwrap();
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

// pub struct FlamegraphProfiler<'a> {
//     frequency: c_int,
//     active_profiler: Option<ProfilerGuard<'a>>,
// }

// impl<'a> FlamegraphProfiler<'a> {
//     #[allow(dead_code)]
//     pub fn new(frequency: c_int) -> Self {
//         FlamegraphProfiler {
//             frequency,
//             active_profiler: None,
//         }
//     }
// }

// impl<'a> Profiler for FlamegraphProfiler<'a> {
//     fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
//         self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
//     }

//     fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
//         std::fs::create_dir_all(benchmark_dir).unwrap();
//         // let flamegraph_path = benchmark_dir.join("flamegraph.svg");
//         let flamegraph_file = File::create("flamegraph.svg")
//             .expect("File system error while creating flamegraph.svg");
//         if let Some(profiler) = self.active_profiler.take() {
//             profiler
//                 .report()
//                 .build()
//                 .unwrap()
//                 .flamegraph(flamegraph_file)
//                 .expect("Error writing flamegraph");
//         }
//     }
// }

fn get_config() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(FREQUENCY, Output::Flamegraph(None)))
}

criterion_group!(
    name = benches;
    config = get_config();
    targets = criterion_benchmark
);
criterion_main!(benches);
