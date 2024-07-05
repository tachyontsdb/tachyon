use clap::{Parser, Subcommand};
use rustyline::{error::ReadlineError, DefaultEditor};
use tachyon::{
    api::Connection,
    common::{Timestamp, Value},
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    root_dir: String,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Insert
    Insert {
        timestamp: Timestamp,
        value: Value,
        matcher: String,
    },

    Query {
        // Read queries from file
        file: String,
    },
}

fn repl(mut conn: Connection) {
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
        let mut stmt = conn.prepare(&line, None, None);
        loop {
            let val = stmt.next_vector();
            if val.is_none() {
                break;
            }
            let (time, val) = val.unwrap();
            println!("{} {}", time, val);
        }

        println!()
    }
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
            Commands::Query { .. } => todo!(),
        },
        None => repl(conn),
    }
}
