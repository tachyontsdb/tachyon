use tachyon_core::{Timestamp, Value, ValueType};

mod file;
mod graphical;
mod tabular;

use crate::{cli::Config, CLIErr};

pub trait CliOutput {
    fn output(
        data: Vec<(Timestamp, Value)>,
        value_type: ValueType,
        config: &Config,
    ) -> Result<(), CLIErr>;
}

pub fn vector_output(
    data: Vec<(Timestamp, Value)>,
    value_type: ValueType,
    config: &Config,
) -> Result<(), CLIErr> {
    match config.output_mode {
        crate::cli::OutputMode::Graphical => graphical::Graphical::output(data, value_type, config),
        crate::cli::OutputMode::Tabular => tabular::Tabular::output(data, value_type, config),
        crate::cli::OutputMode::File => file::FileWriter::output(data, value_type, config),
    }
}
