use std::io::{Read, Write};

use crate::Timestamp;

use super::file::{Header, TimeDataFile};

pub mod float;
pub mod int;

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

pub trait DecompressionEngine<R: Read> {
    type PhysicalType;
    fn new(reader: R, header: &Header) -> Self
    where
        Self: Sized;
    fn next(&mut self) -> (Timestamp, Self::PhysicalType);
}
