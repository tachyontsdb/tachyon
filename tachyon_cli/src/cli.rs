use std::path::PathBuf;

use clap::{builder::{NonEmptyStringValueParser, PossibleValuesParser, TypedValueParser}, command, Parser, Subcommand};
use rustyline::{error::ReadlineError, history::{FileHistory, History}, DefaultEditor};
use tachyon_core::{print_error, Connection, Timestamp, ValueType};

use crate::{handlers, CLIErr};

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
const PROMPT: &str = "$ ";
const COMMAND_PREFIX: &str = ".";

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct EntryArgs {
    pub db_dir: PathBuf,
}

pub struct TachyonCLI {
    rl: rustyline::Editor<(), FileHistory>,
    connection: Connection
}


impl TachyonCLI {
    pub fn new(connection: Connection) -> Self {
        let rl = DefaultEditor::new().unwrap();

        Self {
            rl,
            connection
        }
    }

    pub fn repl(&mut self) -> Result<(), CLIErr> {
        println!("{}", TACHYON_CLI_HEADER);
    
        loop {
            let input = self.rl.readline(PROMPT);
            match input {
                Ok(line) => {
                    let add_history_res = self.rl.add_history_entry(&line);
                    if add_history_res.is_err() {
                        println!("Warning: Failed to add line to history.");
                    }
    
                    if let Some(command_str) = line.strip_prefix(COMMAND_PREFIX) {
                        let args: Vec<&str> =  command_str.split_whitespace().collect();
    
                        match handlers::command::TachyonCommand::try_parse_from(std::iter::once("").chain(args)) {
                            Ok(command) => {
                                handlers::command::handle_command(command, &mut self.connection);
                            }
                            Err(err) => {
                                println!("{}", err);
                            }
                        }
                    } else {
                        handlers::query::handle_query(&line, &mut self.connection, None, None)?;
                    }
    
    
                    // handle_query_command(&mut connection, &line, None, None, None)?;
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    return Ok(());
                }
                Err(e) => {
                    return Err(CLIErr::ReadLineErr(e));
                }
            }
        }
    }
}