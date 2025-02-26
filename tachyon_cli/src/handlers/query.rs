use std::time::{SystemTime, UNIX_EPOCH};

use tachyon_core::{Connection, Timestamp, Value, Vector};

use crate::{cli::Config, output::vector_output, CLIErr};

pub fn handle_query(
    query: &str,
    connection: &mut Connection,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    config: &Config,
) -> Result<(), CLIErr> {
    let start_time: u64 = 0;
    let end_time: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let mut query =
        connection.prepare_query(query, start.or(Some(start_time)), end.or(Some(end_time)));

    let query_value_type = query.value_type();

    match query.return_type() {
        tachyon_core::ReturnType::Scalar => {
            while let Some(value) = query.next_scalar() {
                println!("{:?}", value.get_output(query_value_type));
            }
        }
        tachyon_core::ReturnType::Vector => {
            let mut timeseries = Vec::<(Timestamp, Value)>::new();
            while let Some(Vector { timestamp, value }) = query.next_vector() {
                timeseries.push((timestamp, value));
            }

            vector_output(timeseries, query_value_type, config)?;
        }
    }

    Ok(())
}
