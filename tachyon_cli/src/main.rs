mod cli;
mod handlers;
pub mod output;

use clap::Parser;
use cli::{EntryArgs, TachyonCli};
use csv::Reader;
use rustyline::error::ReadlineError;
use std::path::PathBuf;
use std::{
    fs::{self, File},
    io::Write,
    os::unix::fs::MetadataExt,
};
use tachyon_core::{Connection, ValueType, Vector};
use thiserror::Error;

const TACHYON_CLI_HEADER: &str = r"
 ______                 __                              ____    ____      
/\__  _\               /\ \                            /\  _`\ /\  _`\    
\/_/\ \/    __      ___\ \ \___   __  __    ___     ___\ \ \/\ \ \ \L\ \  
   \ \ \  /'__`\   /'___\ \  _ `\/\ \/\ \  / __`\ /' _ `\ \ \ \ \ \  _ <' 
    \ \ \/\ \L\.\_/\ \__/\ \ \ \ \ \ \_\ \/\ \L\ \/\ \/\ \ \ \_\ \ \ \L\ \
     \ \_\ \__/.\_\ \____\\ \_\ \_\/`____ \ \____/\ \_\ \_\ \____/\ \____/
      \/_/\/__/\/_/\/____/ \/_/\/_/`/___/> \/___/  \/_/\/_/\/___/  \/___/ 
                                      /\___/                              
                                      \/__/                               
";

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
}

// #[derive(Parser)]
// #[command(version, about, long_about = None)]
// pub struct Args {
//     db_dir: PathBuf,
//     #[command(subcommand)]
//     command: Option<Commands>,
// }

// #[derive(Subcommand)]
// pub enum Commands {
//     ListAllStreams,
//     ParseHeaders {
//         paths: Vec<PathBuf>,
//     },
//     Query {
//         #[arg(value_parser = NonEmptyStringValueParser::new())]
//         query: String,
//         start: Option<Timestamp>,
//         end: Option<Timestamp>,
//         export_csv_path: Option<PathBuf>,
//     },
//     CreateStream {
//         #[arg(value_parser = NonEmptyStringValueParser::new())]
//         stream: String,
//         #[arg(value_parser = PossibleValuesParser::new(["i64", "u64", "f64"]).map(|s| match s.as_str() {
//             "i64" => ValueType::Integer64,
//             "u64" => ValueType::UInteger64,
//             "f64" => ValueType::Float64,
//             _ => unreachable!()
//         }))]
//         value_type: ValueType,
//     },
//     Insert {
//         #[arg(value_parser = NonEmptyStringValueParser::new())]
//         stream: String,
//         timestamp: Timestamp,
//         #[arg(value_parser = NonEmptyStringValueParser::new())]
//         value: String,
//     },
//     ImportCSV {
//         #[arg(value_parser = NonEmptyStringValueParser::new())]
//         stream: String,
//         csv_file: PathBuf,
//     },
// }

// fn handle_parse_headers_command(paths: Vec<PathBuf>) -> Result<(), CLIErr> {
//     fn output_header(path: PathBuf, file: TimeDataFile) -> Result<(), CLIErr> {
//         let mut table = Table::new();
//         let file_size = File::open(&path)?.metadata()?.size();

//         // SAFETY: these paths are always our .ty files; assume it can be converted to str
//         table.add_row(row!["File", path.to_str().unwrap()]);

//         table.add_row(row!["Version", file.header.version.0]);
//         table.add_row(row!["Stream ID", file.header.stream_id.0]);

//         table.add_row(row!["Min Timestamp", file.header.min_timestamp]);
//         table.add_row(row!["Max Timestamp", file.header.max_timestamp]);

//         table.add_row(row!["Count", file.header.count]);
//         table.add_row(row!["Value Type", file.header.value_type]);

//         table.add_row(row![
//             "Value Sum",
//             file.header.value_sum.get_output(file.header.value_type)
//         ]);
//         table.add_row(row![
//             "Min Value",
//             file.header.min_value.get_output(file.header.value_type)
//         ]);
//         table.add_row(row![
//             "Max Value",
//             file.header.max_value.get_output(file.header.value_type)
//         ]);

//         table.add_row(row![
//             "First Value",
//             file.header.first_value.get_output(file.header.value_type)
//         ]);

//         table.add_row(row![
//             "Compression Ratio",
//             format!(
//                 "{:.2}x",
//                 ((file.header.count as f64 * 16_f64) / file_size as f64)
//             )
//         ]);

//         table.printstd();

//         Ok(())
//     }

//     fn recurse_subdirs_and_output_headers(path: PathBuf) -> Result<(), CLIErr> {
//         if path.is_dir() {
//             let files = fs::read_dir(path)?;
//             for file in files {
//                 recurse_subdirs_and_output_headers(file?.path())?;
//             }
//         } else if path
//             .extension()
//             .is_some_and(|extension| extension == FILE_EXTENSION)
//         {
//             let file = TimeDataFile::read_data_file(path.clone());
//             output_header(path, file)?;
//             println!();
//         }

//         Ok(())
//     }

//     for path in paths {
//         recurse_subdirs_and_output_headers(path)?;
//     }

//     Ok(())
// }

fn export_as_csv(path: PathBuf, timeseries: &[(u64, f64)]) -> Result<(), CLIErr> {
    let mut file = File::create(path)?;
    file.write_all(b"Timestamp,Value\n")?;

    for (timestamp, value) in timeseries {
        file.write_all(format!("{},{}\n", timestamp, value).as_bytes())?;
    }

    Ok(())
}

fn handle_import_csv_command(
    mut connection: Connection,
    stream: String,
    csv_file: PathBuf,
) -> Result<(), CLIErr> {
    fn read_from_csv(path: &PathBuf, value_type: ValueType) -> Result<Vec<Vector>, CLIErr> {
        let mut rdr = Reader::from_path(path)?;
        let mut vectors = Vec::new();
        for (idx, result) in rdr.records().enumerate() {
            // +2 because idx starts at 0 and the first line in the csv is a header
            let line_num = idx + 2;
            let record = result?;

            let csv_err = CLIErr::CSVTypeErr {
                line_num,
                value: record[1].to_string(),
                value_type,
            };

            vectors.push(Vector {
                timestamp: record[0].parse::<u64>().map_err(|_| CLIErr::CSVTypeErr {
                    line_num,
                    value: record[0].to_string(),
                    value_type: ValueType::UInteger64,
                })?,
                value: match value_type {
                    ValueType::Integer64 => record[1].parse::<i64>().map_err(|_| csv_err)?.into(),
                    ValueType::UInteger64 => record[1].parse::<u64>().map_err(|_| csv_err)?.into(),
                    ValueType::Float64 => record[1].parse::<f64>().map_err(|_| csv_err)?.into(),
                },
            });
        }
        Ok(vectors)
    }

    let mut inserter = connection.prepare_insert(stream);
    println!("Reading from: {:?}", &csv_file);

    let vectors = read_from_csv(&csv_file, inserter.value_type())?;
    println!("Done reading from: {:?}", &csv_file);
    for Vector { timestamp, value } in vectors {
        match inserter.value_type() {
            ValueType::Integer64 => inserter.insert_integer64(timestamp, value.get_integer64()),
            ValueType::UInteger64 => inserter.insert_uinteger64(timestamp, value.get_uinteger64()),
            ValueType::Float64 => inserter.insert_float64(timestamp, value.get_float64()),
        }
    }
    inserter.flush();
    Ok(())
}

pub fn main() {
    let args = EntryArgs::parse();
    let connection = Connection::new(&args.db_dir).unwrap();

    let mut cli = TachyonCli::new(connection, args.db_dir);
    cli.repl().unwrap();
}
