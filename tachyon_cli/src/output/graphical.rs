use tachyon_core::ValueType;
use textplots::{Chart, Plot, Shape};

use crate::cli::Config;

use super::CliOutput;

pub struct Graphical;

impl CliOutput for Graphical {
    fn output(
        timeseries: Vec<(tachyon_core::Timestamp, tachyon_core::Value)>,
        value_type: ValueType,
        _: &Config,
    ) {
        let f32_timeseries: Vec<(f32, f32)> = timeseries
            .iter()
            .map(|(timestamp, value)| {
                (*timestamp as f32, value.convert_into_f64(value_type) as f32)
            })
            .collect();

        if let Some((last_timestamp, _)) = f32_timeseries.last() {
            Chart::new(180, 60, f32_timeseries[0].0, *last_timestamp)
                .lineplot(&Shape::Lines(&f32_timeseries))
                .display();
        }
    }
}
