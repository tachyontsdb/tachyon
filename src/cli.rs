use std::{
    fs::File,
    io::Write,
    iter::zip,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use csv::{Reader, Writer};
use prettytable::{row, Table};
use rustyline::{error::ReadlineError, DefaultEditor};
use tachyon::{
    api::{Connection, TachyonResultType},
    common::{Timestamp, Value},
    storage::file::TimeDataFile,
};
use textplots::{Chart, Plot, Shape};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    root_dir: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Insert {
        timestamp: Timestamp,
        value: Value,
        matcher: String,
    },

    Query {
        query: String,
        export_path: Option<String>,
    },

    Csv {
        file: String,
        matcher: String,
    },

    Debug {
        file: String,

        #[arg(short, long)]
        csv: Option<String>,
    },
}

fn repl(mut conn: Connection) {
    println!(
        r"
 ______                 __                              ____    ____      
/\__  _\               /\ \                            /\  _`\ /\  _`\    
\/_/\ \/    __      ___\ \ \___   __  __    ___     ___\ \ \/\ \ \ \L\ \  
   \ \ \  /'__`\   /'___\ \  _ `\/\ \/\ \  / __`\ /' _ `\ \ \ \ \ \  _ <' 
    \ \ \/\ \L\.\_/\ \__/\ \ \ \ \ \ \_\ \/\ \L\ \/\ \/\ \ \ \_\ \ \ \L\ \
     \ \_\ \__/.\_\ \____\\ \_\ \_\/`____ \ \____/\ \_\ \_\ \____/\ \____/
      \/_/\/__/\/_/\/____/ \/_/\/_/`/___/> \/___/  \/_/\/_/\/___/  \/___/ 
                                      /\___/                              
                                      \/__/                               
    "
    );
    let mut rl = DefaultEditor::new().unwrap();

    loop {
        let input = rl.readline(">>> ");
        match &input {
            Ok(line) => {
                rl.add_history_entry(line.as_str()).unwrap();
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        };

        let line = input.unwrap();

        handle_query_command(&mut conn, line, None)
    }
}

fn handle_query_command(conn: &mut Connection, query: String, path_opt: Option<String>) {
    let mut stmt = conn.prepare(&query, Some(0), Some(1719776339748));

    match stmt.return_type() {
        TachyonResultType::Scalar => println!("{}", stmt.next_scalar().unwrap()),
        TachyonResultType::Vector => {
            let mut timeseries = Vec::<(f32, f32)>::new();

            let mut max_value = Value::MIN;
            let mut min_value = Value::MAX;

            loop {
                let val = stmt.next_vector();
                if val.is_none() {
                    break;
                }
                let (time, val) = val.unwrap();
                max_value = max_value.max(val);
                min_value = min_value.min(val);

                timeseries.push((time as f32, val as f32));
            }

            Chart::new(180, 60, timeseries[0].0, timeseries.last().unwrap().0)
                .lineplot(&Shape::Lines(&timeseries))
                .display();

            if let Some(path) = path_opt {
                export_as_csv(&path.into() as &PathBuf, &timeseries);
            }
        }
        TachyonResultType::Done => println!(),
    }
}

fn handle_debug_command(_: Connection, file: String, output_csv: Option<String>) {
    let t_file = TimeDataFile::read_data_file(file.clone().into());

    let mut table = Table::new();
    table.add_row(row!["Property", "Value"]);
    table.add_row(row!["File Name", file]);
    table.add_row(row!["Stream ID", t_file.header.stream_id.to_string()]);
    table.add_row(row!["Version", t_file.header.version.to_string()]);

    table.add_row(row!["Count", t_file.header.count.to_string()]);
    table.add_row(row![
        "Min Timestamp",
        t_file.header.min_timestamp.to_string()
    ]);
    table.add_row(row![
        "Max Timestamp",
        t_file.header.max_timestamp.to_string()
    ]);
    table.add_row(row!["Min Value", t_file.header.min_value.to_string()]);
    table.add_row(row!["Max Value", t_file.header.max_value.to_string()]);
    table.add_row(row!["First Value", t_file.header.first_value.to_string()]);
    table.add_row(row!["Value Sum", t_file.header.value_sum.to_string()]);

    table.printstd();

    if let Some(path) = output_csv {
        let mut wtr = Writer::from_path(path).unwrap();
        wtr.write_record(["Timestamp", "Value"]).unwrap();
        for (t, v) in zip(t_file.timestamps, t_file.values) {
            wtr.write_record(&[t.to_string(), v.to_string()]).unwrap();
        }
        wtr.flush().unwrap();
    };
}

fn export_as_csv(path: &Path, timeseries: &Vec<(f32, f32)>) {
    let mut file = File::create(path).unwrap();

    file.write_all("timestamp,value\n".as_bytes()).unwrap();

    for (t, v) in timeseries {
        file.write_all(format!("{},{}\n", t, v).as_bytes()).unwrap();
    }
}

fn insert_from_csv(mut conn: Connection, matcher: String, file: String) {
    fn read_from_csv(path: &str) -> (Vec<u64>, Vec<u64>) {
        println!("Reading from: {}", path);
        let mut rdr = Reader::from_path(path).unwrap();

        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        for result in rdr.records() {
            let record = result.unwrap();
            timestamps.push(record[0].parse::<u64>().unwrap());
            values.push(record[1].parse::<u64>().unwrap());
        }
        println!("Done reading from: {}\n", path);

        (timestamps, values)
    }

    let (time, values) = read_from_csv(&file);
    let mut batch_writer = conn.batch_insert(&matcher);
    for (t, v) in zip(time, values) {
        batch_writer.insert(t, v);
    }
    drop(conn);
}

pub fn main() {
    let args = Args::parse();

    let mut conn = Connection::new(args.root_dir.into());

    match args.command {
        Some(commands) => match commands {
            Commands::Insert {
                timestamp,
                value,
                matcher,
            } => conn.insert(&matcher, timestamp, value),
            Commands::Query { query, export_path } => {
                handle_query_command(&mut conn, query, export_path)
            }
            Commands::Csv { file, matcher } => insert_from_csv(conn, matcher, file),
            Commands::Debug { file, csv } => handle_debug_command(conn, file, csv),
        },
        None => repl(conn),
    }
}
