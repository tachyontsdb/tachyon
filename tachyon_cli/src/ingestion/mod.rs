use clap::Subcommand;
use tachyon_core::api::BatchWriter;

pub mod csv;

pub trait Ingestor {
    fn ingest(&self, batch_writer: &mut BatchWriter);
}

#[derive(Subcommand, Debug)]
pub enum Ingestion {
    Csv {
        path: String,

        timestamp_column: Option<String>,
        value_column: Option<String>,
        attribute_columns: Option<String>,

        timestamp_format: Option<String>,
    },
}
