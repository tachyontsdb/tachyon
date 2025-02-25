use clap::Subcommand;
use dir_size::get_size_in_human_bytes;
use tabled::{builder::Builder, settings::Style};
use tachyon_core::Connection;

use crate::{cli::Config, CLIErr};

#[derive(Debug, Subcommand)]
pub enum Info {
    Stat,
    Streams,
}

pub fn handle_info_command(
    connection: &Connection,
    config: &Config,
    command: Info,
) -> Result<(), CLIErr> {
    match command {
        Info::Stat => {
            let num_streams = match connection.get_all_streams() {
                Ok(streams) => streams.len() as i64,
                Err(_) => -1,
            };

            let dir_size = match get_size_in_human_bytes(&config.db_dir) {
                Ok(size) => size,
                Err(_) => "N/A".to_string(),
            };

            println!("Total Streams: {}", num_streams);
            println!("Storage Used: {}", dir_size);

            Ok(())
        }
        Info::Streams => {
            let mut rows = Vec::<Vec<String>>::new();
            rows.push(vec![
                "Stream ID".to_string(),
                "Stream Name + Matchers".to_string(),
                "Value Type".to_string(),
            ]);

            for stream in connection.get_all_streams().unwrap() {
                let matchers: Vec<String> = stream
                    .1
                    .into_iter()
                    .map(|(matcher_name, matcher_value)| {
                        format!("\"{matcher_name}\" = \"{matcher_value}\"")
                    })
                    .collect();
                rows.push(vec![
                    stream.0.to_string(),
                    matchers.join(" | "),
                    stream.2.to_string(),
                ]);
            }

            let mut table = Builder::from(rows).build();
            table.with(Style::modern_rounded());

            println!("{}", table);
            Ok(())
        }
    }
}
