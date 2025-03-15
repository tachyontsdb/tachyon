use std::io::{Read, Write};

use crate::{storage::file::Header, tachyon_benchmarks::TimeDataFile, Timestamp};

use super::{CompressionEngine, DecompressionEngine};

mod v1;

#[allow(clippy::large_enum_variant)]
pub enum FloatCompressor<W: Write> {
    V1(v1::CompressionEngineV1<W>),
}

impl<W: Write> CompressionEngine<W> for FloatCompressor<W> {
    type PhysicalType = f64;
    fn new(writer: W, header: &Header) -> Self {
        Self::V1(v1::CompressionEngineV1::new(writer, header))
    }
    fn new_from_partial(writer: W, data_file: TimeDataFile) -> Self {
        Self::V1(v1::CompressionEngineV1::new_from_partial(writer, data_file))
    }
    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) -> usize {
        match self {
            Self::V1(engine) => engine.consume(timestamp, value),
        }
    }
    fn flush_all(&mut self) -> usize {
        match self {
            Self::V1(engine) => engine.flush_all(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum FloatDecompressor<R: Read> {
    V1(v1::DecompressionEngineV1<R>),
}

impl<R: Read> DecompressionEngine<R> for FloatDecompressor<R> {
    type PhysicalType = f64;
    fn new(reader: R, header: &Header) -> Self {
        Self::V1(v1::DecompressionEngineV1::new(reader, header))
    }

    fn next(&mut self) -> (Timestamp, f64) {
        match self {
            Self::V1(engine) => engine.next(),
        }
    }
}
