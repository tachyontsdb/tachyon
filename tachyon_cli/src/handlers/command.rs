use std::path::PathBuf;

use clap::{builder::{NonEmptyStringValueParser, PossibleValuesParser, TypedValueParser}, command, Parser};
use csv::Reader;
use tachyon_core::{Connection, ValueType, Vector};

use crate::CLIErr;

#[derive(Debug, Parser)]
#[command(name="", version, about, long_about = None)]
pub enum TachyonCommand {
    Exit,
    Info,
    Write {
        path: PathBuf,
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        format: Option<String>
    },
    Create {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        #[arg(value_parser = PossibleValuesParser::new(["i64", "u64", "f64"]).map(|s| match s.as_str() {
            "i64" => ValueType::Integer64,
            "u64" => ValueType::UInteger64,
            "f64" => ValueType::Float64,
            _ => unreachable!()
        }))]
        value_type: ValueType,
    },
}

pub fn handle_command(command: TachyonCommand, connection: &mut Connection) -> Result<(), CLIErr> {
    match command {
        TachyonCommand::Info => {
            Ok(())
        },
        TachyonCommand::Write { path, stream, format } => {
            let mut inserter = connection.prepare_insert(stream);
            println!("Reading from: {:?}", &path);


            // TODO: use formatter (either csv or json)
            let vectors = read_from_csv(&path, inserter.value_type())?;
            for Vector { timestamp, value } in &vectors {
                match inserter.value_type() {
                    ValueType::Integer64 => inserter.insert_integer64(*timestamp, value.get_integer64()),
                    ValueType::UInteger64 => inserter.insert_uinteger64(*timestamp, value.get_uinteger64()),
                    ValueType::Float64 => inserter.insert_float64(*timestamp, value.get_float64()),
                }
            }
            inserter.flush();
            println!("Successfully wrote {} entries to {:#?}", vectors.len(), path);
            Ok(())
        },
        TachyonCommand::Create { stream, value_type } => {
            connection.create_stream(stream, value_type).unwrap();
            Ok(())
        }
        TachyonCommand::Exit => {
            Ok(())
        }
    }
}

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