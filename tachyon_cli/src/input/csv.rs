use std::path::Path;

use csv::Reader;
use tachyon_core::{ValueType, Vector};

use crate::CLIErr;

use super::CliInput;

pub struct Csv;

impl CliInput for Csv {
    fn input(path: &Path, value_type: ValueType) -> Result<Vec<Vector>, CLIErr> {
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
}
