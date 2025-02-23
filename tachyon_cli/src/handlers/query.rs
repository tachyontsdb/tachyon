use std::time::{SystemTime, UNIX_EPOCH};

use tachyon_core::{Connection, Timestamp, Vector};
use textplots::{Chart, Plot, Shape};

use crate::CLIErr;

pub fn handle_query(query: &str, connection: &mut Connection, start: Option<Timestamp>, end: Option<Timestamp>) -> Result<(), CLIErr> {
    let start_time: u64 = 0;
    let end_time: u64 = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .expect("Time went backwards")
    .as_millis() as u64;

    let mut query = connection.prepare_query(
        query,
        start.or(Some(start_time)),
        end.or(Some(end_time)),
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

            let f32_timeseries: Vec<(f32, f32)> = timeseries
                .iter()
                .map(|(timestamp, value)| (*timestamp as f32, *value as f32))
                .collect();

            if let Some((last_timestamp, _)) = f32_timeseries.last() {
                Chart::new(180, 60, f32_timeseries[0].0, *last_timestamp)
                    .lineplot(&Shape::Lines(&f32_timeseries))
                    .display();
            }
        }
    }

    Ok(())
}