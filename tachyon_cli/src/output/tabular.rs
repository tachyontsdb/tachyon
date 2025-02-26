use tabled::{
    settings::{object::Columns, Alignment, Style},
    Table, Tabled,
};
use tachyon_core::{Timestamp, Value};

use crate::CLIErr;

use super::CliOutput;

#[derive(Tabled)]
struct TimeseriesDataI64 {
    timestamp: Timestamp,
    value: i64,
}

#[derive(Tabled)]
struct TimeseriesDataU64 {
    timestamp: Timestamp,
    value: u64,
}

#[derive(Tabled)]
struct TimeseriesDataF64 {
    timestamp: Timestamp,
    value: f64,
}

pub struct Tabular;

impl CliOutput for Tabular {
    fn output(
        data: Vec<(Timestamp, Value)>,
        value_type: tachyon_core::ValueType,
        _: &crate::cli::Config,
    ) -> Result<(), CLIErr> {
        let table = match value_type {
            tachyon_core::ValueType::Integer64 => {
                let mut table =
                    Table::new(data.iter().map(|(timestamp, value)| TimeseriesDataI64 {
                        timestamp: *timestamp,
                        value: value.get_integer64(),
                    }));
                table.with(Style::modern_rounded());
                table.modify(Columns::first(), Alignment::right());
                table
            }
            tachyon_core::ValueType::UInteger64 => {
                let mut table =
                    Table::new(data.iter().map(|(timestamp, value)| TimeseriesDataU64 {
                        timestamp: *timestamp,
                        value: value.get_uinteger64(),
                    }));
                table.with(Style::modern_rounded());
                table.modify(Columns::first(), Alignment::right());
                table
            }
            tachyon_core::ValueType::Float64 => {
                let mut table =
                    Table::new(data.iter().map(|(timestamp, value)| TimeseriesDataF64 {
                        timestamp: *timestamp,
                        value: value.get_float64(),
                    }));
                table.with(Style::modern_rounded());
                table.modify(Columns::first(), Alignment::right());
                table
            }
        };

        println!("{}", table);
        Ok(())
    }
}
