use std::{
    fs::{self, File},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use clap::Subcommand;
use tabled::{builder::Builder, settings::Style};
use tachyon_core::{tachyon_benchmarks::TimeDataFile, FILE_EXTENSION};

use crate::CLIErr;

#[derive(Debug, Subcommand)]
pub enum Debug {
    Headers { paths: Vec<PathBuf> },
}

pub fn handle_debug_command(command: Debug) -> Result<(), CLIErr> {
    match command {
        Debug::Headers { paths } => handle_parse_headers_command(paths),
    }
}

fn handle_parse_headers_command(paths: Vec<PathBuf>) -> Result<(), CLIErr> {
    fn output_header(path: PathBuf, file: TimeDataFile) -> Result<(), CLIErr> {
        let file_size = File::open(&path)?.metadata()?.size();

        let rows = vec![
            vec!["File".to_string(), path.to_str().unwrap().to_string()],
            vec!["Version".to_string(), file.header.version.0.to_string()],
            vec!["Stream ID".to_string(), file.header.stream_id.0.to_string()],
            vec![
                "Min Timestamp".to_string(),
                file.header.min_timestamp.to_string(),
            ],
            vec![
                "Max Timestamp".to_string(),
                file.header.max_timestamp.to_string(),
            ],
            vec!["Count".to_string(), file.header.count.to_string()],
            vec!["Value Type".to_string(), file.header.value_type.to_string()],
            vec![
                "Value Sum".to_string(),
                file.header.value_sum.get_output(file.header.value_type),
            ],
            vec![
                "Min Value".to_string(),
                file.header.min_value.get_output(file.header.value_type),
            ],
            vec![
                "Max Value".to_string(),
                file.header.max_value.get_output(file.header.value_type),
            ],
            vec![
                "First Value".to_string(),
                file.header.first_value.get_output(file.header.value_type),
            ],
            vec![
                "Compression Ratio".to_string(),
                format!(
                    "{:.2}x",
                    (file.header.count as f64 * 16.0) / file_size as f64
                ),
            ],
        ];

        let mut table = Builder::from(rows).build();
        table.with(Style::modern_rounded());

        println!("{}", table);
        Ok(())
    }

    fn recurse_subdirs_and_output_headers(path: PathBuf) -> Result<(), CLIErr> {
        if path.is_dir() {
            let files = fs::read_dir(path)?;
            for file in files {
                recurse_subdirs_and_output_headers(file?.path())?;
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == FILE_EXTENSION)
        {
            let file = TimeDataFile::read_data_file(path.clone());
            output_header(path, file)?;
            println!();
        }

        Ok(())
    }

    for path in paths {
        recurse_subdirs_and_output_headers(path)?;
    }

    Ok(())
}
