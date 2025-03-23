use crate::{Timestamp, ValueType};
use promql_parser::label::Matchers;
use std::{error::Error, io, path::PathBuf, time::SystemTimeError};
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
    #[error(transparent)]
    WriterErr(#[from] WriterErr),
    #[error(transparent)]
    InserterErr(#[from] InserterErr),
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
    #[error("Failed to create stream because it already exists: {stream}")]
    ExistingStreamErr { stream: String },
    #[error("Failed to insert into stream: {stream}")]
    StreamInsertErr { stream: String },
    #[error("Failed to {op} on non-existent stream: {stream}.")]
    StreamNoExistErr { op: String, stream: String },
    #[error("Failed to get all streams.")]
    GetStreamsErr,
    #[error("Failed to parse stream {stream} as a vector selector.")]
    StreamParseErr { stream: String },
}

#[derive(Error, Debug)]
pub enum InserterErr {
    #[error("Can't insert type {this_type} into a stream of type {stream_type}.")]
    TypeErr {
        this_type: ValueType,
        stream_type: ValueType,
    },
}

#[derive(Error, Debug)]
pub enum WriterErr {
    #[error("Compressor not initialized.")]
    CompressorNotInitialized,
    #[error(
        "Write out of order. Tried to insert at {ts} when last entry is at timestamp {prev_ts}."
    )]
    OutOfOrderErr { ts: Timestamp, prev_ts: Timestamp },
    #[error(transparent)]
    IndexerErr(#[from] IndexerErr),
    #[error(transparent)]
    IOErr(#[from] io::Error),
}
