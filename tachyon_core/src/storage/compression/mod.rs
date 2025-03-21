use std::io::{Read, Write};

use crate::{Timestamp, Value, ValueType};

use super::file::{Header, TimeDataFile};

mod float;
mod int;

pub trait CompressionEngine<W: Write> {
    type PhysicalType;
    fn new(writer: W, header: &Header) -> Self
    where
        Self: Sized;
    fn new_from_partial(writer: W, data_file: TimeDataFile) -> Self
    where
        Self: Sized;
    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) -> usize;
    fn flush_all(&mut self) -> usize;
}

#[allow(clippy::large_enum_variant)]
pub enum Compressor<W: Write> {
    Int(int::IntCompressor<W>),
    Float(float::FloatCompressor<W>),
}

impl<W: Write> CompressionEngine<W> for Compressor<W> {
    type PhysicalType = Value;

    fn new(writer: W, header: &Header) -> Self {
        match header.value_type {
            ValueType::Integer64 => Compressor::Int(int::IntCompressor::new(writer, header)),
            ValueType::UInteger64 => Compressor::Int(int::IntCompressor::new(writer, header)),
            ValueType::Float64 => Compressor::Float(float::FloatCompressor::new(writer, header)),
        }
    }

    fn new_from_partial(writer: W, data_file: TimeDataFile) -> Self {
        match data_file.header.value_type {
            ValueType::Integer64 => {
                Compressor::Int(int::IntCompressor::new_from_partial(writer, data_file))
            }
            ValueType::UInteger64 => {
                Compressor::Int(int::IntCompressor::new_from_partial(writer, data_file))
            }
            ValueType::Float64 => {
                Compressor::Float(float::FloatCompressor::new_from_partial(writer, data_file))
            }
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) -> usize {
        match self {
            Self::Int(engine) => engine.consume(timestamp, value.get_uinteger64()),
            Self::Float(engine) => engine.consume(timestamp, value.get_float64()),
        }
    }

    fn flush_all(&mut self) -> usize {
        match self {
            Self::Int(engine) => engine.flush_all(),
            Self::Float(engine) => engine.flush_all(),
        }
    }
}

pub trait DecompressionEngine<R: Read> {
    type PhysicalType;
    fn new(reader: R, header: &Header) -> Self
    where
        Self: Sized;
    fn next(&mut self) -> (Timestamp, Self::PhysicalType);
}

#[allow(clippy::large_enum_variant)]
pub enum Decompressor<R: Read> {
    Int(int::IntDecompressor<R>),
    Float(float::FloatDecompressor<R>),
}

impl<R: Read> DecompressionEngine<R> for Decompressor<R> {
    type PhysicalType = Value;

    fn new(reader: R, header: &Header) -> Self {
        match header.value_type {
            ValueType::Integer64 => Decompressor::Int(int::IntDecompressor::new(reader, header)),
            ValueType::UInteger64 => Decompressor::Int(int::IntDecompressor::new(reader, header)),
            ValueType::Float64 => {
                Decompressor::Float(float::FloatDecompressor::new(reader, header))
            }
        }
    }

    fn next(&mut self) -> (Timestamp, Self::PhysicalType) {
        match self {
            Self::Int(engine) => {
                let entry = engine.next();
                (entry.0, entry.1.into())
            }
            Self::Float(engine) => {
                let entry = engine.next();
                (entry.0, entry.1.into())
            }
        }
    }
}
