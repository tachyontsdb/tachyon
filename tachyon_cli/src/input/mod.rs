use std::path::PathBuf;

use tachyon_core::{ValueType, Vector};

use crate::CLIErr;

pub mod csv;

pub trait CliInput {
    fn input(path: &PathBuf, value_type: ValueType) -> Result<Vec<Vector>, CLIErr>;
}

pub fn vector_input(path: &PathBuf, value_type: ValueType) -> Result<Vec<Vector>, CLIErr> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("csv") => csv::Csv::input(path, value_type),
        None => Err(CLIErr::UnsupportedFileErr {
            extension: "N/A".to_string(),
        }),
        Some(extension) => Err(CLIErr::UnsupportedFileErr {
            extension: extension.to_string(),
        }), // Handle other extensions explicitly
    }
}
