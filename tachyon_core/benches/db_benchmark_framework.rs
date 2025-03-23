use criterion::{criterion_group, criterion_main, Criterion};
use csv::Reader;
use postgres::{Client as PgClient, NoTls};
use pprof::{
    criterion::{Output, PProfProfiler},
    flamegraph::Options,
};
use rusqlite::{params, Connection as SQLiteConnection};
use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
};
use tachyon_core::{
    error::TachyonErr, Connection as TachyonConnection, Timestamp, Value, ValueType, Vector,
};

#[cfg(feature = "tachyon_memory_profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

//----------------------------------------------------------------------
// Generic Database Source Trait
//----------------------------------------------------------------------

/// Trait defining operations for any database backend
pub trait DatabaseSource: Sized {
    /// Error type for database operations
    type Error: Debug;

    /// Initialize a new database connection or instance
    fn new(path: impl AsRef<Path>, value_type: ValueType) -> Result<Self, Self::Error>;

    /// Set up the database (create tables, streams, etc.)
    fn setup(&mut self) -> Result<(), Self::Error>;

    /// Insert data points (timestamp, value)
    fn insert(&mut self, timestamps: &[Timestamp], values: &[Value]) -> Result<(), Self::Error>;

    /// Read data from the database
    fn read(&mut self, start: Timestamp, end: Timestamp) -> Result<u128, Self::Error>;

    /// Clean up resources (e.g., close connections, remove files)
    fn cleanup(path: impl AsRef<Path>) -> Result<(), Self::Error>;

    /// Name of the database for display in benchmarks
    fn name() -> &'static str;
}

//----------------------------------------------------------------------
// Tachyon Database Adapter
//----------------------------------------------------------------------

const DEFAULT_STREAM_NAME: &str = r#"bench_stream"#;

pub struct TachyonDB {
    conn: TachyonConnection,
    value_type: ValueType,
    stream_name: String,
}

impl DatabaseSource for TachyonDB {
    type Error = TachyonErr;

    fn new(path: impl AsRef<Path>, value_type: ValueType) -> Result<Self, Self::Error> {
        let conn = TachyonConnection::new(path.as_ref())?;
        Ok(TachyonDB {
            conn,
            value_type,
            stream_name: DEFAULT_STREAM_NAME.to_string(),
        })
    }

    fn setup(&mut self) -> Result<(), Self::Error> {
        self.conn
            .create_stream(&self.stream_name, self.value_type)?;
        Ok(())
    }

    fn insert(&mut self, timestamps: &[Timestamp], values: &[Value]) -> Result<(), Self::Error> {
        let mut inserter = self.conn.prepare_insert(&self.stream_name);
        for (ts, val) in timestamps.iter().zip(values.iter()) {
            match self.value_type {
                ValueType::Integer64 => inserter.insert_integer64(*ts, val.get_integer64())?,
                ValueType::UInteger64 => inserter.insert_uinteger64(*ts, val.get_uinteger64())?,
                ValueType::Float64 => inserter.insert_float64(*ts, val.get_float64())?,
            }
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

    fn cleanup(path: impl AsRef<Path>) -> Result<(), Self::Error> {
        if path.as_ref().exists() {
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

const DEFAULT_SQLITE_FILE: &str = "benchmark.sqlite";

pub struct SQLiteDB {
    conn: SQLiteConnection,
    value_type: ValueType,
}

impl DatabaseSource for SQLiteDB {
    type Error = rusqlite::Error;

    fn new(path: impl AsRef<Path>, value_type: ValueType) -> Result<Self, Self::Error> {
        let db_path = path.as_ref().join(DEFAULT_SQLITE_FILE);
        let conn = SQLiteConnection::open(db_path)?;
        Ok(SQLiteDB { conn, value_type })
    }

    fn setup(&mut self) -> Result<(), Self::Error> {
        self.conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS timeseries (
                timestamp INTEGER,
                value {}
            )",
                match self.value_type {
                    ValueType::Integer64 | ValueType::UInteger64 => "INTEGER",
                    ValueType::Float64 => "REAL",
                }
            ),
            [],
        )?;
        Ok(())
    }

    fn insert(&mut self, timestamps: &[Timestamp], values: &[Value]) -> Result<(), Self::Error> {
        let transaction = self.conn.transaction().unwrap();
        let mut insert_stmt = transaction
            .prepare_cached("INSERT INTO timeseries (timestamp, value) VALUES (?, ?)")
            .unwrap();

        for (t, v) in timestamps.iter().zip(values.iter()) {
            match self.value_type {
                ValueType::Integer64 => {
                    insert_stmt.execute(params![t, v.get_integer64(),]).unwrap();
                }
                ValueType::UInteger64 => {
                    insert_stmt
                        .execute(params![t, v.get_uinteger64(),])
                        .unwrap();
                }
                ValueType::Float64 => {
                    insert_stmt.execute(params![t, v.get_float64(),]).unwrap();
                }
            }
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

    fn cleanup(path: impl AsRef<Path>) -> Result<(), Self::Error> {
        let db_path = path.as_ref().join(DEFAULT_SQLITE_FILE);
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
// TimescaleDB Database Adapter
//----------------------------------------------------------------------

const DEFAULT_CONN_STRING: &str = "host=localhost user=postgres password=password dbname=postgres";
const DEFAULT_TABLE_NAME: &str = "benchmark_timeseries";

pub struct TimescaleDB {
    conn: PgClient,
    value_type: ValueType,
    table_name: String,
}

impl DatabaseSource for TimescaleDB {
    type Error = postgres::Error;

    fn new(_path: impl AsRef<Path>, value_type: ValueType) -> Result<Self, Self::Error> {
        // Path is ignored for TimescaleDB since it uses a network connection
        let conn = PgClient::connect(DEFAULT_CONN_STRING, NoTls)?;
        Ok(TimescaleDB {
            conn,
            value_type,
            table_name: DEFAULT_TABLE_NAME.to_string(),
        })
    }

    fn setup(&mut self) -> Result<(), Self::Error> {
        // Drop table if it exists
        self.conn
            .execute(&format!("DROP TABLE IF EXISTS {}", self.table_name), &[])?;

        // Create the table with appropriate type
        let value_type = match self.value_type {
            ValueType::Integer64 => "BIGINT",
            ValueType::UInteger64 => "BIGINT", // PostgreSQL doesn't have unsigned types
            ValueType::Float64 => "DOUBLE PRECISION",
        };

        let create_table_query = format!(
            "CREATE TABLE {} (
                timestamp BIGINT,
                value {}
            )",
            self.table_name, value_type
        );

        self.conn.execute(&create_table_query, &[])?;

        // Create hypertable
        let create_hypertable_query = format!(
            "SELECT create_hypertable('{}', by_range('timestamp'))",
            self.table_name
        );
        self.conn.execute(&create_hypertable_query, &[])?;

        Ok(())
    }

    fn insert(&mut self, timestamps: &[Timestamp], values: &[Value]) -> Result<(), Self::Error> {
        let mut transaction = self.conn.transaction()?;

        for (ts, v) in timestamps.iter().zip(values.iter()) {
            match self.value_type {
                ValueType::Integer64 => {
                    transaction.execute(
                        &format!("INSERT INTO {} VALUES ($1, $2)", self.table_name),
                        &[&(*ts as i64), &v.get_integer64()],
                    )?;
                }
                ValueType::UInteger64 => {
                    transaction.execute(
                        &format!("INSERT INTO {} VALUES ($1, $2)", self.table_name),
                        &[&(*ts as i64), &(v.get_uinteger64() as i64)],
                    )?;
                }
                ValueType::Float64 => {
                    transaction.execute(
                        &format!("INSERT INTO {} VALUES ($1, $2)", self.table_name),
                        &[&(*ts as i64), &v.get_float64()],
                    )?;
                }
            }
        }

        transaction.commit()?;
        Ok(())
    }

    fn read(&mut self, start: Timestamp, end: Timestamp) -> Result<u128, Self::Error> {
        let query = format!(
            "SELECT timestamp, value FROM {} WHERE timestamp BETWEEN $1 AND $2",
            self.table_name
        );

        let mut sum: u128 = 0;
        for row in self.conn.query(&query, &[&(start as i64), &(end as i64)])? {
            let timestamp: i64 = row.get(0);

            // Handle different value types
            match self.value_type {
                ValueType::Integer64 | ValueType::UInteger64 => {
                    let value: i64 = row.get(1);
                    sum += (timestamp as u128) + (value as u128);
                }
                ValueType::Float64 => {
                    let value: f64 = row.get(1);
                    sum += (timestamp as u128) + (value as u128);
                }
            }
        }

        Ok(sum)
    }

    fn cleanup(_path: impl AsRef<Path>) -> Result<(), Self::Error> {
        // Connect and drop the table
        let mut client = PgClient::connect(DEFAULT_CONN_STRING, NoTls)?;
        client.execute(&format!("DROP TABLE IF EXISTS {}", DEFAULT_TABLE_NAME), &[])?;
        Ok(())
    }

    fn name() -> &'static str {
        "TimescaleDB"
    }
}

//----------------------------------------------------------------------
// Framework Utilities
//----------------------------------------------------------------------

/// Function to read timestamp-value pairs from a CSV file
pub fn read_from_csv(
    path: impl AsRef<Path>,
    value_type: ValueType,
) -> (Vec<Timestamp>, Vec<Value>) {
    println!("Reading from: {:?}", path.as_ref());

    let mut timestamps = Vec::new();
    let mut values = Vec::new();

    let mut rdr = Reader::from_path(path.as_ref()).unwrap();
    for result in rdr.records() {
        let record = result.unwrap();
        timestamps.push(record[0].parse::<Timestamp>().unwrap());
        match value_type {
            ValueType::Integer64 => values.push(record[1].parse::<i64>().unwrap().into()),
            ValueType::UInteger64 => values.push(record[1].parse::<u64>().unwrap().into()),
            ValueType::Float64 => values.push(record[1].parse::<f64>().unwrap().into()),
        }
    }

    println!(
        "Done reading from: {:?}, read {} records\n",
        path.as_ref(),
        timestamps.len()
    );

    (timestamps, values)
}

/// Benchmark inserting data into a database
pub fn bench_insert<D: DatabaseSource>(
    db_path: impl AsRef<Path>,
    value_type: ValueType,
    timestamps: &[Timestamp],
    values: &[Value],
) -> Result<(), D::Error> {
    let mut db = D::new(db_path.as_ref(), value_type)?;
    db.setup()?;
    db.insert(timestamps, values)?;
    D::cleanup(db_path.as_ref()).unwrap();
    Ok(())
}

/// Benchmark reading data from a database
pub fn bench_read<D: DatabaseSource>(
    db_path: impl AsRef<Path>,
    value_type: ValueType,
    start: Timestamp,
    end: Timestamp,
) -> Result<u128, D::Error> {
    let mut db = D::new(db_path, value_type)?;
    let result = db.read(start, end)?;
    Ok(result)
}

/// Runner for insert benchmarks
pub fn run_insert_benchmark<D: DatabaseSource>(
    c: &mut Criterion,
    value_type: ValueType,
    db_path: impl AsRef<Path>,
    csv_path: impl AsRef<Path>,
) {
    fs::create_dir_all(db_path.as_ref()).unwrap();
    let (timestamps, values) = read_from_csv(csv_path, value_type);

    c.bench_function(&format!("{}: insert benchmark", D::name()), |b| {
        b.iter(|| {
            bench_insert::<D>(db_path.as_ref(), value_type, &timestamps, &values).unwrap();
        })
    });
}

/// Runner for read benchmarks
pub fn run_read_benchmark<D: DatabaseSource>(
    c: &mut Criterion,
    value_type: ValueType,
    db_path: impl AsRef<Path>,
    csv_path: impl AsRef<Path>,
) {
    fs::create_dir_all(db_path.as_ref()).unwrap();
    let (timestamps, values) = read_from_csv(csv_path, value_type);

    let mut db = D::new(db_path.as_ref(), value_type).unwrap();
    db.setup().unwrap();
    db.insert(&timestamps, &values).unwrap();

    c.bench_function(
        &format!(
            "{}: read benchmark ({} entries)",
            D::name(),
            timestamps.len()
        ),
        |b| {
            b.iter(|| {
                bench_read::<D>(
                    db_path.as_ref(),
                    value_type,
                    *timestamps.first().unwrap(),
                    *timestamps.last().unwrap(),
                )
                .unwrap()
            })
        },
    );

    println!(
        "{} DB size: {} bytes",
        D::name(),
        get_dir_size(db_path.as_ref())
    );

    D::cleanup(db_path).unwrap();
}

/// Calculate the total size of a directory recursively
fn get_dir_size(path: impl AsRef<Path>) -> u64 {
    let mut total_size = 0;

    if path.as_ref().is_file() {
        return fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let entry_path = entry.path();
            total_size += get_dir_size(&entry_path);
        }
    }

    total_size
}

//----------------------------------------------------------------------
// Benchmark Options
//----------------------------------------------------------------------

/// Helper function to get criterion configuration with profiling
pub fn get_criterion_config<const SAMPLE_SIZE: usize>() -> Criterion {
    let mut options = Options::default();
    options.flame_chart = true;
    Criterion::default()
        .sample_size(SAMPLE_SIZE)
        .with_profiler(PProfProfiler::new(10000, Output::Flamegraph(Some(options))))
}

//----------------------------------------------------------------------
// Benchmark Main Functions
//----------------------------------------------------------------------

fn tachyon_insert_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/tachyon_insert_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_insert_benchmark::<TachyonDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

fn sqlite_insert_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/sqlite_insert_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_insert_benchmark::<SQLiteDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

fn tachyon_read_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/tachyon_read_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_read_benchmark::<TachyonDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

fn sqlite_read_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/sqlite_read_bench");
    let csv_path = "../data/voltage_dataset.csv";
    run_read_benchmark::<SQLiteDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

fn timescaledb_insert_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/timescaledb_insert_bench"); // Path is ignored but kept for consistency
    let csv_path = "../data/voltage_dataset.csv";
    run_insert_benchmark::<TimescaleDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

fn timescaledb_read_benchmark(c: &mut Criterion) {
    #[cfg(feature = "tachyon_memory_profiling")]
    let _profiler = dhat::Profiler::builder().testing().build();

    let db_path = PathBuf::from("../tmp/timescaledb_read_bench"); // Path is ignored but kept for consistency
    let csv_path = "../data/voltage_dataset.csv";
    run_read_benchmark::<TimescaleDB>(c, ValueType::UInteger64, &db_path, csv_path);
}

criterion_group!(
    name = insert_benches;
    config = get_criterion_config::<20>();
    targets = tachyon_insert_benchmark, sqlite_insert_benchmark, timescaledb_insert_benchmark
);

criterion_group!(
    name = read_benches;
    config = get_criterion_config::<100>();
    targets = tachyon_read_benchmark, sqlite_read_benchmark, timescaledb_read_benchmark
);

criterion_main!(read_benches, insert_benches);
