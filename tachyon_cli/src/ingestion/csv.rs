use csv::Reader;
use tachyon_core::api::BatchWriter;

use super::Ingestor;

#[derive(Debug, Clone)]
pub struct Csv {
    path: String,

    timestamp_column: Option<String>,
    value_column: Option<String>,
    attribute_columns: Option<String>,

    timestamp_format: Option<String>,
}

impl Csv {
    pub fn new(
        path: String,
        timestamp_column: Option<String>,
        value_column: Option<String>,
        attribute_columns: Option<String>,
        timestamp_format: Option<String>,
    ) -> Self {
        Csv {
            path,
            timestamp_column,
            value_column,
            attribute_columns,
            timestamp_format,
        }
    }
}

impl Ingestor for Csv {
    fn ingest(&self, batch_writer: &mut BatchWriter) {
        println!("Ingesting CSV data from: {}", self.path);
        let mut rdr = Reader::from_path(&self.path).unwrap();

        for result in rdr.records() {
            let record = result.unwrap();
            let timestamp = record[0].parse::<u64>().unwrap();
            let value = record[1].parse::<u64>().unwrap();

            batch_writer.insert(timestamp, value);
        }
        println!("Done ingesting from: {}\n", self.path);
    }
}
