mod cli;
mod handlers;
pub mod input;
pub mod output;

use clap::Parser;
use cli::{EntryArgs, TachyonCli};
use rustyline::error::ReadlineError;
use tachyon_core::{Connection, ValueType};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CLIErr {
    #[error("Input '{input}' could not be converted to stream type = {value_type}.")]
    InputValueTypeErr {
        input: String,
        value_type: ValueType,
    },
    #[error("Line #{line_num} in CSV failed to parse {value}; expected type {value_type}")]
    CSVTypeErr {
        line_num: usize,
        value: String,
        value_type: ValueType,
    },
    #[error("Failed to read from CSV.")]
    CSVErr(#[from] csv::Error),
    #[error("Failed to read line.")]
    ReadLineErr(#[from] ReadlineError),
    #[error("IO Error.")]
    FileIOErr(#[from] std::io::Error),
    #[error("Unsupported file format #{extension}.")]
    UnsupportedFileErr { extension: String },
}

// fn export_as_csv(path: PathBuf, timeseries: &[(u64, f64)]) -> Result<(), CLIErr> {
//     let mut file = File::create(path)?;
//     file.write_all(b"Timestamp,Value\n")?;

//     for (timestamp, value) in timeseries {
//         file.write_all(format!("{},{}\n", timestamp, value).as_bytes())?;
//     }

//     Ok(())
// }

pub fn main() {
    let args = EntryArgs::parse();
    let connection = Connection::new(&args.db_dir).unwrap();

    let mut cli = TachyonCli::new(connection, args.db_dir);
    cli.repl().unwrap();
}
