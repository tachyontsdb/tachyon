use clap::{
    builder::{NonEmptyStringValueParser, PossibleValuesParser, TypedValueParser},
    Parser, Subcommand,
};
use csv::Reader;
use prettytable::{row, Table};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::path::PathBuf;
use std::{
    fs::{self, File},
    io::Write,
};
use tachyon_core::tachyon_benchmarks::TimeDataFile;
use tachyon_core::{Connection, Timestamp, ValueType, Vector, FILE_EXTENSION};
use textplots::{Chart, Plot, Shape};

const TACHYON_CLI_HEADER: &str = r"
 ______                 __                              ____    ____      
/\__  _\               /\ \                            /\  _`\ /\  _`\    
\/_/\ \/    __      ___\ \ \___   __  __    ___     ___\ \ \/\ \ \ \L\ \  
   \ \ \  /'__`\   /'___\ \  _ `\/\ \/\ \  / __`\ /' _ `\ \ \ \ \ \  _ <' 
    \ \ \/\ \L\.\_/\ \__/\ \ \ \ \ \ \_\ \/\ \L\ \/\ \/\ \ \ \_\ \ \ \L\ \
     \ \_\ \__/.\_\ \____\\ \_\ \_\/`____ \ \____/\ \_\ \_\ \____/\ \____/
      \/_/\/__/\/_/\/____/ \/_/\/_/`/___/> \/___/  \/_/\/_/\/___/  \/___/ 
                                      /\___/                              
                                      \/__/                               
";
const PROMPT: &str = "> ";

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Args {
    db_dir: PathBuf,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    ListAllStreams,
    ParseHeaders {
        paths: Vec<PathBuf>,
    },
    Query {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        query: String,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
        export_csv_path: Option<PathBuf>,
    },
    CreateStream {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        #[arg(value_parser = PossibleValuesParser::new(["i64", "u64", "f64"]).map(|s| match s.as_str() {
            "i64" => ValueType::Integer64,
            "u64" => ValueType::UInteger64,
            "f64" => ValueType::Float64,
            _ => unreachable!()
        }))]
        value_type: ValueType,
    },
    Insert {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        timestamp: Timestamp,
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        value: String,
    },
    ImportCSV {
        #[arg(value_parser = NonEmptyStringValueParser::new())]
        stream: String,
        csv_file: PathBuf,
    },
}

fn handle_parse_headers_command(paths: Vec<PathBuf>) {
    fn output_header(path: PathBuf, file: TimeDataFile) {
        let mut table = Table::new();

        table.add_row(row!["File", path.to_str().unwrap()]);
        table.add_row(row!["Version", file.header.version.0]);
        table.add_row(row!["Stream ID", file.header.stream_id.0]);

        table.add_row(row!["Min Timestamp", file.header.min_timestamp]);
        table.add_row(row!["Max Timestamp", file.header.max_timestamp]);

        table.add_row(row!["Count", file.header.count]);
        table.add_row(row!["Value Type", file.header.value_type]);

        table.add_row(row![
            "Value Sum",
            file.header.value_sum.get_output(file.header.value_type)
        ]);
        table.add_row(row![
            "Min Value",
            file.header.min_value.get_output(file.header.value_type)
        ]);
        table.add_row(row![
            "Max Value",
            file.header.max_value.get_output(file.header.value_type)
        ]);

        table.add_row(row![
            "First Value",
            file.header.first_value.get_output(file.header.value_type)
        ]);

        table.printstd();
    }

    fn recurse_subdirs_and_output_headers(path: PathBuf) {
        if path.is_dir() {
            let files = fs::read_dir(path).unwrap();
            for file in files {
                recurse_subdirs_and_output_headers(file.unwrap().path());
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == FILE_EXTENSION)
        {
            let file = TimeDataFile::read_data_file(path.clone());
            output_header(path, file);
            println!();
        }
    }

    for path in paths {
        recurse_subdirs_and_output_headers(path);
    }
}

fn export_as_csv(path: PathBuf, timeseries: &[(u64, f64)]) {
    let mut file = File::create(path).unwrap();

    file.write_all(b"Timestamp,Value\n").unwrap();

    for (timestamp, value) in timeseries {
        file.write_all(format!("{},{}\n", timestamp, value).as_bytes())
            .unwrap();
    }
}

fn handle_query_command(
    connection: &mut Connection,
    query: impl AsRef<str>,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    export_csv_path: Option<PathBuf>,
) {
    // TODO: Fix temporary start and end hack
    const HACK_TIME_START: u64 = 0;
    const HACK_TIME_END: u64 = 1719776339748;
    let mut query = connection.prepare_query(
        query,
        start.or(Some(HACK_TIME_START)),
        end.or(Some(HACK_TIME_END)),
    );

    let query_value_type = query.value_type();

    match query.return_type() {
        tachyon_core::ReturnType::Scalar => {
            while let Some(value) = query.next_scalar() {
                println!("{:?}", value.get_output(query_value_type));
            }
        }
        tachyon_core::ReturnType::Vector => {
            let mut timeseries = Vec::<(u64, f64)>::new();

            let mut max_value = f64::MIN;
            let mut min_value = f64::MAX;

            while let Some(Vector { timestamp, value }) = query.next_vector() {
                let value = value.convert_into_f64(query_value_type);

                max_value = f64::max(max_value, value);
                min_value = f64::min(min_value, value);

                timeseries.push((timestamp, value));
            }

            if let Some(path) = export_csv_path {
                export_as_csv(path, &timeseries);
            }

            let f32_timeseries: Vec<(f32, f32)> = timeseries
                .iter()
                .map(|(timestamp, value)| (*timestamp as f32, *value as f32))
                .collect();

            Chart::new(
                180,
                60,
                f32_timeseries[0].0,
                f32_timeseries.last().unwrap().0,
            )
            .lineplot(&Shape::Lines(&f32_timeseries))
            .display();
        }
    }
}

fn handle_import_csv_command(mut connection: Connection, stream: String, csv_file: PathBuf) {
    fn read_from_csv(path: &PathBuf, value_type: ValueType) -> Vec<Vector> {
        let mut rdr = Reader::from_path(path).unwrap();
        let mut vectors = Vec::new();
        for result in rdr.records() {
            let record = result.unwrap();
            vectors.push(Vector {
                timestamp: record[0].parse::<u64>().unwrap(),
                value: match value_type {
                    ValueType::Integer64 => record[1].parse::<i64>().unwrap().into(),
                    ValueType::UInteger64 => record[1].parse::<u64>().unwrap().into(),
                    ValueType::Float64 => record[1].parse::<f64>().unwrap().into(),
                },
            });
        }
        vectors
    }

    let mut inserter = connection.prepare_insert(stream);
    println!("Reading from: {:?}", &csv_file);
    let vectors = read_from_csv(&csv_file, inserter.value_type());
    println!("Done reading from: {:?}", &csv_file);
    for Vector { timestamp, value } in vectors {
        match inserter.value_type() {
            ValueType::Integer64 => inserter.insert_integer64(timestamp, value.get_integer64()),
            ValueType::UInteger64 => inserter.insert_uinteger64(timestamp, value.get_uinteger64()),
            ValueType::Float64 => inserter.insert_float64(timestamp, value.get_float64()),
        }
    }
    inserter.flush();
}

pub fn repl(mut connection: Connection) {
    println!("{}", TACHYON_CLI_HEADER);

    let mut rl = DefaultEditor::new().unwrap();
    loop {
        let input = rl.readline(PROMPT);
        match &input {
            Ok(line) => {
                rl.add_history_entry(line).unwrap();
                handle_query_command(&mut connection, line, None, None, None);
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!("EXITING");
                break;
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }
}

pub fn main() {
    let args = Args::parse();

    let mut connection = Connection::new(args.db_dir);

    match args.command {
        Some(Commands::ListAllStreams) => {
            let mut table = Table::new();
            table.add_row(row!["Stream ID", "Stream Name + Matchers", "Value Type"]);
            for stream in connection.get_all_streams() {
                let matchers: Vec<String> = stream
                    .1
                    .into_iter()
                    .map(|(matcher_name, matcher_value)| {
                        format!("\"{matcher_name}\" = \"{matcher_value}\"")
                    })
                    .collect();
                table.add_row(row![stream.0, matchers.join(" | "), stream.2]);
            }
            table.printstd();
        }
        Some(Commands::ParseHeaders { paths }) => {
            handle_parse_headers_command(paths);
        }
        Some(Commands::Query {
            query,
            start,
            end,
            export_csv_path,
        }) => {
            handle_query_command(&mut connection, query, start, end, export_csv_path);
        }
        Some(Commands::CreateStream { stream, value_type }) => {
            connection.create_stream(stream, value_type);
        }
        Some(Commands::Insert {
            stream,
            timestamp,
            value,
        }) => {
            let mut inserter = connection.prepare_insert(stream);

            match inserter.value_type() {
                ValueType::Integer64 => {
                    inserter.insert_integer64(timestamp, value.parse().unwrap())
                }
                ValueType::UInteger64 => {
                    inserter.insert_uinteger64(timestamp, value.parse().unwrap())
                }
                ValueType::Float64 => inserter.insert_float64(timestamp, value.parse().unwrap()),
            }

            inserter.flush();
        }
        Some(Commands::ImportCSV { stream, csv_file }) => {
            handle_import_csv_command(connection, stream, csv_file);
        }
        None => repl(connection),
    }
}
