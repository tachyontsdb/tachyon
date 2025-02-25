use tachyon_core::{Timestamp, Value, ValueType};

mod graphical;
mod tabular;

use crate::cli::Config;

pub trait CliOutput {
    fn output(data: Vec<(Timestamp, Value)>, value_type: ValueType, config: &Config);
}

pub fn vector_output(data: Vec<(Timestamp, Value)>, value_type: ValueType, config: &Config) {
    match config.output_mode {
        crate::cli::OutputMode::Graphical => graphical::Graphical::output(data, value_type, config),
        crate::cli::OutputMode::Tabular => tabular::Tabular::output(data, value_type, config),
        crate::cli::OutputMode::File => todo!(),
    }
}
