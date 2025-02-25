use std::{fs::File, io::Write, path::PathBuf};

use tachyon_core::{Timestamp, Value, ValueType};

use crate::{cli::Config, CLIErr};

use super::CliOutput;

pub struct FileWriter;

impl CliOutput for FileWriter {
    fn output(
        timeseries: Vec<(Timestamp, Value)>,
        value_type: ValueType,
        config: &Config,
    ) -> Result<(), CLIErr> {
        match &config.path {
            Some(path) => {
                match path.extension().and_then(|ext| ext.to_str()) {
                    Some("csv") => FileWriter::output_csv(timeseries, value_type, path),
                    None => Err(CLIErr::UnsupportedFileErr {
                        extension: "N/A".to_string(),
                    }),
                    Some(extension) => Err(CLIErr::UnsupportedFileErr {
                        extension: extension.to_string(),
                    }), // Handle other extensions explicitly
                }
            }
            None => Ok(()),
        }
    }
}

impl FileWriter {
    fn output_csv(
        timeseries: Vec<(Timestamp, Value)>,
        value_type: ValueType,
        path: &PathBuf,
    ) -> Result<(), CLIErr> {
        let mut file = File::create(path)?;
        file.write_all(b"Timestamp,Value\n")?;

        for (timestamp, value) in timeseries {
            match value_type {
                ValueType::Integer64 => {
                    file.write_all(format!("{},{}\n", timestamp, value.get_integer64()).as_bytes())?
                }
                ValueType::UInteger64 => file
                    .write_all(format!("{},{}\n", timestamp, value.get_uinteger64()).as_bytes())?,
                ValueType::Float64 => {
                    file.write_all(format!("{},{}\n", timestamp, value.get_float64()).as_bytes())?
                }
            }
        }
        Ok(())
    }
}
