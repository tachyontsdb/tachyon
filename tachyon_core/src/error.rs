use crate::Timestamp;
use promql_parser::label::Matchers;
use std::{error::Error, path::PathBuf, time::SystemTimeError};
use thiserror::Error;

pub fn print_error(err: &impl Error) {
    eprintln!("Encountered error: {}", err);
}

#[derive(Error, Debug)]
pub enum TachyonErr {
    #[error("Failed to perform desired operation. Inner error: {inner}")]
    MiscErr { inner: Box<dyn Error> },
    #[error(transparent)]
    ConnectionErr(#[from] ConnectionErr),
    #[error(transparent)]
    QueryErr(#[from] QueryErr),
}

#[derive(Error, Debug)]
pub enum QueryErr {
    #[error("Incorrect query syntax.")]
    QuerySyntaxErr,
    #[error("{expr_type} expressions are not supported.")]
    UnsupportedErr { expr_type: String },
    #[error("QueryPlanner requires {start_or_end} member to be set.")]
    StartEndTimeErr { start_or_end: String },
    #[error("Failed to handle @ modifier due to system time error.")]
    TimerErr(#[from] SystemTimeError),
    #[error("No streams match selector \"{name}{{{matchers}}}\" from \"{start}\" to \"{end}\".")]
    NoStreamsMatchedErr {
        name: String,
        matchers: Matchers,
        start: Timestamp,
        end: Timestamp,
    },
}

#[derive(Error, Debug)]
pub enum IndexerErr {
    #[error("SQLite Error.")]
    SQLiteErr(#[from] rusqlite::Error),
}

#[derive(Error, Debug)]
pub enum ConnectionErr {
    #[error(transparent)]
    IndexerErr(#[from] IndexerErr),
    #[error("Failed to create the directory for the database: {db_dir}.")]
    DatabaseCreationErr { db_dir: PathBuf },
    #[error("Failed to create stream: {stream}.")]
    StreamCreationErr { stream: String },
    #[error("Failed to get all streams.")]
    GetStreamsErr,
}
