use csv::Reader;
use std::{cell::RefCell, iter::zip, path::PathBuf, rc::Rc, str::FromStr};
use tachyon::{
    api::{Connection, TachyonResultType},
    storage::{
        file::{Cursor, ScanHint, TimeDataFile},
        page_cache::PageCache,
    },
};

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

fn create_tachyon(root_dir: &PathBuf) {
    let mut conn = Connection::new(root_dir.clone());
    let mut batch_writer = conn.batch_insert(r#"voltage"#);

    let (timestamps, values) = read_from_csv("./data/voltage_dataset.csv");
    for i in 0..timestamps.len() {
        batch_writer.insert(timestamps[i], values[i]);
    }
    conn.writer.flush_all();
}

fn tachyon_query(root_dir: &PathBuf) -> u128 {
    let query = r#"voltage"#;
    let mut conn = Connection::new(root_dir.clone());

    let mut stmt = conn.prepare(query, Some(0), Some(1300000000));

    let mut result = 0u128;
    match stmt.return_type() {
        TachyonResultType::Scalar => {
            result += stmt.next_scalar().unwrap() as u128;
        }
        TachyonResultType::Scalars => loop {
            let res = stmt.next_scalar();
            match res {
                None => break,
                Some => {
                    result += stmt.next_scalar().unwrap() as u128;
                }
            }
        },
        TachyonResultType::Vector => {
            let (timestamp, value) = stmt.next_vector().unwrap();
            result += (timestamp as u128) + (value as u128);
        }
        TachyonResultType::Vectors => loop {
            let res = stmt.next_vector();
            match res {
                None => break,
                Some((timestamp, value)) => {
                    result += (timestamp as u128) + (value as u128);
                }
            }
        },
        TachyonResultType::Done => panic!("Invalid result type!"),
    }
    result
}

#[derive(Debug)]
struct Item {
    timestamp: u64,
    value: u64,
}

fn sqlite_query() -> u128 {
    let conn = rusqlite::Connection::open("./tmp/bench_voltage.sqlite").unwrap();
    let mut stmt = conn.prepare("SELECT * FROM Item").unwrap();

    let item_iter = stmt
        .query_map([], |row| {
            Ok(Item {
                timestamp: row.get(0).unwrap(),
                value: row.get(1).unwrap(),
            })
        })
        .unwrap();

    let mut result = 0u128;
    for item in item_iter {
        let item_v = item.unwrap();
        result += (item_v.timestamp as u128) + (item_v.value as u128);
    }
    result
}

pub fn main() {
    let root_dir = PathBuf::from_str("./tmp/db").unwrap();
    // std::fs::create_dir_all(&root_dir).unwrap();

    // create_tachyon(&root_dir);
    let result = tachyon_query(&root_dir);
    println!("Result: {}", result);

    // let result = sqlite_query();
    // println!("Result: {}", result);
}
