use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use rusqlite::{params, Connection as SQLiteConnection};
use std::{
    fmt::Debug,
    fs,
    hint::black_box,
    io,
    path::{Path, PathBuf},
    u64,
};
use tachyon_core::{Connection as TachyonConnection, Timestamp, ValueType, Vector};

//----------------------------------------------------------------------
// Generic Database Source Trait
//----------------------------------------------------------------------

/// Trait defining operations for any database backend
pub trait DatabaseSource: Sized {
    /// Error type for database operations
    type Error: Debug;

    /// Initialize a new database connection or instance
    fn new(path: &Path) -> Result<Self, Self::Error>;

    /// Set up the database (create tables, streams, etc.)
    fn setup(&mut self) -> Result<(), Self::Error>;

    /// Insert data points (timestamp, value)
    fn insert(&mut self, timestamp: Vec<u64>, value: Vec<u64>) -> Result<(), Self::Error>;

    /// Read data from the database
    fn read(&mut self, start: Timestamp, end: Timestamp) -> Result<u128, Self::Error>;

    /// Clean up resources (e.g., close connections, remove files)
    fn cleanup(path: &Path) -> Result<(), Self::Error>;

    /// Name of the database for display in benchmarks
    fn name() -> &'static str;
}

//----------------------------------------------------------------------
// Tachyon Database Adapter
//----------------------------------------------------------------------

pub struct TachyonDB {
    conn: TachyonConnection,
    stream_name: String,
}

impl DatabaseSource for TachyonDB {
    type Error = tachyon_core::error::TachyonErr;

    fn new(path: &Path) -> Result<Self, Self::Error> {
        let conn = TachyonConnection::new(path.to_path_buf())?;
        Ok(TachyonDB {
            conn,
            stream_name: r#"bench_stream"#.to_string(),
        })
    }

    fn setup(&mut self) -> Result<(), Self::Error> {
        self.conn
            .create_stream(&self.stream_name, ValueType::UInteger64)?;
        Ok(())
    }

    fn insert(&mut self, timestamp: Vec<u64>, value: Vec<u64>) -> Result<(), Self::Error> {
        let mut inserter = self.conn.prepare_insert(&self.stream_name);
        for (ts, val) in timestamp.iter().zip(value.iter()) {
            inserter.insert_uinteger64(*ts, *val)?;
        }
        inserter.flush()?;
        Ok(())
    }

    fn read(&mut self, start: Timestamp, end: Timestamp) -> Result<u128, Self::Error> {
        let mut reader = self
            .conn
            .prepare_query(&self.stream_name, Some(start), Some(end))?;

        let mut sum: u128 = 0;
        while let Some(Vector { timestamp, value }) = reader.next_vector() {
            sum += (timestamp + value.get_uinteger64()) as u128;
        }

        Ok(sum)
    }

    fn cleanup(path: &Path) -> Result<(), Self::Error> {
        if path.exists() {
            fs::remove_dir_all(path)
                .map_err(|e| tachyon_core::error::TachyonErr::MiscErr { inner: Box::new(e) })?;
        }
        Ok(())
    }

    fn name() -> &'static str {
        "Tachyon"
    }
}

//----------------------------------------------------------------------
// SQLite Database Adapter
//----------------------------------------------------------------------

pub struct SQLiteDB {
    conn: SQLiteConnection,
}

impl DatabaseSource for SQLiteDB {
    type Error = rusqlite::Error;

    fn new(path: &Path) -> Result<Self, Self::Error> {
        let db_path = path.join("benchmark.sqlite");
        let conn = SQLiteConnection::open(db_path)?;
        Ok(SQLiteDB { conn })
    }

    fn setup(&mut self) -> Result<(), Self::Error> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS timeseries (
                timestamp INTEGER,
                value INTEGER
            )",
            [],
        )?;
        Ok(())
    }

    fn insert(&mut self, timestamp: Vec<u64>, value: Vec<u64>) -> Result<(), Self::Error> {
        let transaction = self.conn.transaction().unwrap();
        let mut insert_stmt = transaction
            .prepare("INSERT INTO timeseries (timestamp, value) VALUES (?, ?)")
            .unwrap();

        for (t, v) in timestamp.iter().zip(value.iter()) {
            insert_stmt.execute([&t, &v]).unwrap();
        }
        drop(insert_stmt);
        transaction.commit().unwrap();
        Ok(())
    }

    fn read(&mut self, start: Timestamp, end: Timestamp) -> Result<u128, Self::Error> {
        let mut stmt = self
            .conn
            .prepare("SELECT timestamp, value FROM timeseries WHERE timestamp BETWEEN ? AND ?")?;
        let mut rows = stmt.query([start, end])?;

        let mut sum: u128 = 0;
        while let Some(row) = rows.next()? {
            let timestamp: u64 = row.get(0)?;
            let value: u64 = row.get(1)?;
            sum += (timestamp + value) as u128;
        }

        Ok(sum)
    }

    fn cleanup(path: &Path) -> Result<(), Self::Error> {
        let db_path = path.join("benchmark.sqlite");
        if db_path.exists() {
            fs::remove_file(&db_path).map_err(|e| {
                rusqlite::Error::InvalidPath(format!("Failed to remove file: {}", e).into())
            })?;
        }
        Ok(())
    }

    fn name() -> &'static str {
        "SQLite"
    }
}

//----------------------------------------------------------------------
// Framework Utilities
//----------------------------------------------------------------------

/// Function to read timestamp-value pairs from a CSV file
pub fn read_from_csv(path: &str) -> (Vec<u64>, Vec<u64>) {
    println!("Reading from: {}", path);
    let mut rdr = Reader::from_path(path).unwrap();

    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for result in rdr.records() {
        let record = result.unwrap();
        timestamps.push(record[0].parse::<u64>().unwrap());
        values.push(record[1].parse::<u64>().unwrap());
    }
    println!(
        "Done reading from: {}, read {} records\n",
        path,
        timestamps.len()
    );

    (timestamps, values)
}

/// Benchmark inserting data into a database
pub fn bench_insert<D: DatabaseSource>(
    db_path: &Path,
    timestamps: &[u64],
    values: &[u64],
) -> Result<(), D::Error> {
    let mut db = black_box(D::new(db_path)?);
    db.setup()?;

    black_box(db.insert(timestamps.to_vec(), values.to_vec())?);

    Ok(())
}

/// Benchmark reading data from a database
pub fn bench_read<D: DatabaseSource>(
    db_path: &Path,
    start: u64,
    end: u64,
) -> Result<u128, D::Error> {
    let mut db = black_box(D::new(db_path)?);
    let result = black_box(db.read(start, end)?);
    Ok(result)
}

/// Runner for insert benchmarks
pub fn run_insert_benchmark<D: DatabaseSource>(c: &mut Criterion, db_path: &Path, csv_path: &str) {
    fs::create_dir_all(db_path).unwrap();
    let (timestamps, values) = read_from_csv(csv_path);

    c.bench_function(&format!("{}: insert benchmark", D::name()), |b| {
        b.iter(|| {
            bench_insert::<D>(db_path, &timestamps, &values).unwrap();
            D::cleanup(db_path).unwrap();
        })
    });
}

/// Runner for read benchmarks
pub fn run_read_benchmark<D: DatabaseSource>(c: &mut Criterion, db_path: &Path, csv_path: &str) {
    fs::create_dir_all(db_path).unwrap();
    let (timestamps, values) = read_from_csv(csv_path);

    // First, we need to insert the data
    let mut db = D::new(db_path).unwrap();
    db.setup().unwrap();
    black_box(db.insert(timestamps.to_vec(), values.to_vec()).unwrap());

    println!("Starting read benchmark");
    // Now benchmark reading
    c.bench_function(
        &format!(
            "{}: read benchmark ({} entries)",
            D::name(),
            timestamps.len()
        ),
        |b| {
            b.iter(|| bench_read::<D>(db_path, timestamps[0], *timestamps.last().unwrap()).unwrap())
        },
    );

    // Output recursive directory size
    println!("{} DB size: {} bytes", D::name(), get_dir_size(db_path));

    // Clean up after benchmark
    D::cleanup(db_path).unwrap();
}

/// Calculate the total size of a directory recursively
fn get_dir_size(path: &Path) -> u64 {
    let mut total_size = 0;

    if path.is_file() {
        return fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }

    match fs::read_dir(path) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    total_size += fs::metadata(&entry_path).map(|m| m.len()).unwrap_or(0);
                } else if entry_path.is_dir() {
                    total_size += get_dir_size(&entry_path);
                }
            }
        }
        Err(_) => {}
    }

    total_size
}

/// Helper function to get criterion configuration with profiling
pub fn get_criterion_config() -> Criterion {
    // let mut options = Options::default();
    // options.flame_chart = true;
    // Criterion::default()
    //     .sample_size(20)
    //     .with_profiler(PProfProfiler::new(10000, Output::Flamegraph(Some(options))))
    Criterion::default()
}

//----------------------------------------------------------------------
// Benchmark Main Functions
//----------------------------------------------------------------------

fn tachyon_insert_benchmark(c: &mut Criterion) {
    let db_path = PathBuf::from("../tmp/tachyon_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_insert_benchmark::<TachyonDB>(c, &db_path, csv_path);
}

fn sqlite_insert_benchmark(c: &mut Criterion) {
    let db_path = PathBuf::from("../tmp/sqlite_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_insert_benchmark::<SQLiteDB>(c, &db_path, csv_path);
}

fn tachyon_read_benchmark(c: &mut Criterion) {
    let db_path = PathBuf::from("../tmp/tachyon_read_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_read_benchmark::<TachyonDB>(c, &db_path, csv_path);
}

fn sqlite_read_benchmark(c: &mut Criterion) {
    let db_path = PathBuf::from("../tmp/sqlite_read_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_read_benchmark::<SQLiteDB>(c, &db_path, csv_path);
}

criterion_group!(
    name = insert_benches;
    config = get_criterion_config();
    targets = tachyon_insert_benchmark, sqlite_insert_benchmark
);

criterion_group!(
    name = read_benches;
    config = get_criterion_config();
    targets = tachyon_read_benchmark, sqlite_read_benchmark
);

criterion_main!(read_benches);
