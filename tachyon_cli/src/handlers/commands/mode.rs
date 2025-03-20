use clap::Subcommand;
use tabled::{builder::Builder, settings::{object::Rows, Color, Style}};
use tachyon_core::ValueType;

use crate::{cli::Config, CLIErr};

#[derive(Debug, Subcommand)]
pub enum Mode {
    Get,
}

pub fn handle_mode_get(command: Mode, config: &mut Config) -> Result<(), CLIErr> {
    match command {
        Mode::Get => {
            let mut rows = Vec::<Vec<String>>::new();
            rows.push(vec![
                "Configuration Option".to_string(),
                "Configuration Value".to_string()
            ]);

            rows.push(vec![
                "Output Mode".to_string(),
                config.output_mode.to_string()
            ]);

            if let Some(path) = &config.path {
                rows.push(vec![
                    "Output Path".to_string(),
                    path.to_string_lossy().to_string(),
                ]);
            }

            let value_type_str = if config.value_type == ValueType::Float64 {
                "f64"
            } else if config.value_type == ValueType::Integer64 {
                "i64"
            } else {
                "u64"
            };

            rows.push(vec![
                "Value Type".to_string(),
                value_type_str.to_string(),
            ]);


            let mut table = Builder::from(rows).build();
            table.with(Style::modern_rounded())
            .modify(Rows::single(0), Color::FG_BRIGHT_CYAN);

            println!("{}", table);

            Ok(())
        },
    }
}
