// use std::sync::Arc;

// use criterion::{criterion_group, criterion_main, Criterion};
// use rusqlite::{params, Connection, Result};
// use tachyon::storage::file::*;

// const NUM_ITEMS: u64 = 100000;

// #[derive(Debug)]
// pub struct Item {
//     timestamp: u64,
//     value: u64,
// }

// fn bench_write_sqlite(conn: &Connection) -> u64 {
//     // setup SQLite benchmark
//     let mut res = 0;
//     for i in 0..NUM_ITEMS {
//         let item = Item {
//             timestamp: i,
//             value: i + (i % 100),
//         };
//         conn.execute(
//             "
//                     INSERT INTO Item (timestamp, value) VALUES (?1, ?2);
//                 ",
//             (&item.timestamp, &item.value),
//         )
//         .unwrap();

//         res += item.timestamp + item.value;
//     }
//     res
// }

// fn bench_write_sequential_timestamps() -> u64 {
//     let mut model = TimeDataFile::new();
//     let mut res = 0;
//     for i in 0..NUM_ITEMS {
//         model.write_data_to_file_in_mem(i.into(), i + (i % 100));
//         res += i + (i + (i % 100));
//     }
//     model.write("./tmp/bench_sequential_read.ty".into());
//     std::fs::remove_file("./tmp/bench_sequential_read.ty").unwrap();
//     res
// }

// fn criterion_benchmark(c: &mut Criterion) {
//     c.bench_function(&format!("tachyon: write sequential 0-{}", NUM_ITEMS), |b| {
//         b.iter(|| bench_write_sequential_timestamps())
//     });

//     let conn = Connection::open("./tmp/bench_sql.sqlite").unwrap();
//     conn.execute(
//         "
//                 CREATE TABLE Item (
//                     timestamp INTEGER,
//                     value INTEGER
//                 )
//             ",
//         (),
//     )
//     .unwrap();

//     c.bench_function(&format!("SQLite: write sequential 0-{}", NUM_ITEMS), |b| {
//         b.iter(|| bench_write_sqlite(&conn))
//     });

//     // cleanup
//     conn.execute(
//         "
//             DROP TABLE Item;
//         ",
//         (),
//     )
//     .unwrap();
// }

// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);

fn main() {}
