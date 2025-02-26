use std::path::PathBuf;

use clap::{
    builder::{NonEmptyStringValueParser, PossibleValuesParser, TypedValueParser},
    command, Parser,
};
use tachyon_core::{Connection, ValueType, Vector};

use crate::{
    cli::{Config, OutputMode},
    input, CLIErr,
};

mod debug;
mod info;

#[derive(Debug, Parser)]
#[command(name = "", version, about)]
pub enum TachyonCommand {
    Exit,
    Info {
        #[command(subcommand)]
        command: info::Info,
    },
    #[command(hide = true)]
    Debug {
        #[command(subcommand)]
        command: debug::Debug,
    },
    Write {
        path: PathBuf,
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        #[arg(short, long, default_value_t = false)]
        create: bool,
    },
    Create {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
    },
    Mode {
        #[arg(short, long, value_enum)]
        output_mode: Option<OutputMode>,

        #[arg(short, long)]
        path: Option<PathBuf>, // Optional argument

        #[arg(value_parser = PossibleValuesParser::new(["i64", "u64", "f64"]).map(|s| match s.as_str() {
            "i64" => ValueType::Integer64,
            "u64" => ValueType::UInteger64,
            "f64" => ValueType::Float64,
            _ => unreachable!()
        }), short, long)]
        value_type: Option<ValueType>, // Optional argument
    },
}

pub fn handle_command(
    command: TachyonCommand,
    connection: &mut Connection,
    config: &mut Config,
) -> Result<(), CLIErr> {
    match command {
        TachyonCommand::Info { command } => info::handle_info_command(connection, config, command),
        TachyonCommand::Debug { command } => debug::handle_debug_command(command),
        TachyonCommand::Write {
            path,
            stream,
            create,
        } => {
            if create && !connection.check_stream_exists(&stream) {
                connection
                    .create_stream(&stream, config.value_type)
                    .unwrap();
            }

            let mut inserter = connection.prepare_insert(&stream);
            println!("Reading from: {:?}", &path);

            let vectors = input::vector_input(&path, config.value_type)?;

            for Vector { timestamp, value } in &vectors {
                match inserter.value_type() {
                    ValueType::Integer64 => {
                        inserter.insert_integer64(*timestamp, value.get_integer64())
                    }
                    ValueType::UInteger64 => {
                        inserter.insert_uinteger64(*timestamp, value.get_uinteger64())
                    }
                    ValueType::Float64 => inserter.insert_float64(*timestamp, value.get_float64()),
                }
            }
            inserter.flush();
            println!(
                "Successfully wrote {} entries to {:#?}",
                vectors.len(),
                path
            );
            Ok(())
        }
        TachyonCommand::Create { stream } => {
            connection.create_stream(stream, config.value_type).unwrap();
            Ok(())
        }
        TachyonCommand::Exit => Ok(()),
        TachyonCommand::Mode {
            output_mode,
            path: output_path,
            value_type,
        } => {
            if let Some(output_mode) = output_mode {
                config.output_mode = output_mode;
            }

            if let Some(output_path) = output_path {
                config.path = Some(output_path);
            }

            if let Some(value_type) = value_type {
                config.value_type = value_type;
            }

            Ok(())
        }
    }
}
