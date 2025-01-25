/*
    Inspired by Gorilla Compression:
    https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

    Essentially, take XOR of consecutive floating point values to
    achieve "deltas".

    This blog post explains the general idea:
    https://clemenswinter.com/2024/04/07/the-simple-beauty-of-xor-floating-point-compression/

    However, we modify the compression method for a more performant version, albeit with a less efficient
    compression ratio

    // header
    (int info - 1 byte (4 chunks)) 00000000
    (float info - 24 bits (4 chunks)) 000000 000000 000000 000000

    Each 6 bit float info is composed of: 3 bits (length in bytes) and 3 bits shift (in bytes)

    Each chunk encodes V1_CHUNK_SIZE timestamps/value pairs. So, each header is followed by up
    to V1_NUM_CHUNKS_PER_LENGTH * V1_CHUNK_SIZE timestamp deltas and an equal number of floating
    point xors.
*/

use std::io::{Read, Write};

use crate::{
    storage::{
        compression::{int::IntCompressionUtils, CompressionEngine, DecompressionEngine},
        file::Header,
        FileReaderUtils,
    },
    utils::static_assert,
    Timestamp,
};

const NUM_BYTES_TO_INT_CODE: [u8; 9] = [0b00, 0b00, 0b01, 0b10, 0b10, 0b11, 0b11, 0b11, 0b11];
const INT_CODE_TO_BYTES: [u8; 4] = [1, 2, 4, 8];
const V1_INT_READERS: [fn(&[u8]) -> u64; 4] = [
    FileReaderUtils::read_u64_1,
    FileReaderUtils::read_u64_2,
    FileReaderUtils::read_u64_4,
    FileReaderUtils::read_u64_8,
];

// Dictates how many floats are encoded with each window size
const V1_CHUNK_SIZE: usize = 2;

// For now, code assumes 4
const V1_NUM_CHUNKS_PER_LENGTH: usize = 4;
static_assert!(V1_NUM_CHUNKS_PER_LENGTH == 4);

pub struct CompressionEngineV1<T: Write> {
    writer: T,
    last_timestamp: Timestamp,
    last_value: u64,

    last_ts_delta: i64,
    entries_written: u32,

    ts_d_deltas: [u64; V1_CHUNK_SIZE],
    v_xors: [u64; V1_CHUNK_SIZE],
    buffer_idx: usize,
    chunk_idx: usize,
    encoded_length_header: u8,
    encoded_xor_info_header: u32,

    result: Vec<u8>,
    temp_buffer: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for CompressionEngineV1<T> {
    type PhysicalType = f64;

    fn new(writer: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value.get_float64().to_bits(),

            last_ts_delta: 0,
            entries_written: 0,

            ts_d_deltas: [0; V1_CHUNK_SIZE],
            v_xors: [0; V1_CHUNK_SIZE],
            buffer_idx: 0,
            chunk_idx: 0,
            encoded_length_header: 0,
            encoded_xor_info_header: 0,

            result: Vec::new(),
            temp_buffer: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) {
        let ts_delta = (timestamp - self.last_timestamp) as i64;
        let double_ts_delta = ts_delta - self.last_ts_delta;
        self.ts_d_deltas[self.buffer_idx] = IntCompressionUtils::zig_zag_encode(double_ts_delta);

        let value = value.to_bits();
        let value_xored = self.last_value ^ value;
        self.v_xors[self.buffer_idx] = value_xored;

        self.buffer_idx += 1;
        if self.buffer_idx >= V1_CHUNK_SIZE {
            self.flush();
        }

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_ts_delta = ts_delta;
        self.last_value = value;
    }

    fn flush_all(&mut self) -> usize {
        self.flush();
        self.flush_chunk();
        self.writer.write_all(&self.result).unwrap();
        self.result.len()
    }
}

impl<T: Write> CompressionEngineV1<T> {
    fn flush(&mut self) {
        // nothing to write
        if self.buffer_idx == 0 {
            return;
        }
        // Handle partially-filled buffers (should technically never be read)
        for i in self.buffer_idx..self.ts_d_deltas.len() {
            self.ts_d_deltas[i] = 0;
            self.v_xors[i] = 0;
        }

        /*
           1. Encode timestamps
        */
        // TODO: Make this more efficient (use bit packing)
        let mut max_bytes_needed = 0;

        for encoded_ts in self.ts_d_deltas {
            let bits_needed = IntCompressionUtils::bits_needed_u64(encoded_ts);
            // Integer ceiling to find number of bytes needed
            let bytes_needed = bits_needed.div_ceil(8);
            max_bytes_needed = u8::max(max_bytes_needed, bytes_needed);
        }
        let length_code = NUM_BYTES_TO_INT_CODE[max_bytes_needed as usize];
        max_bytes_needed = INT_CODE_TO_BYTES[length_code as usize];
        self.encoded_length_header |= length_code << (6 - 2 * (self.chunk_idx as u8));

        for t_delta in self.ts_d_deltas {
            self.temp_buffer
                .extend_from_slice(&t_delta.to_le_bytes()[..max_bytes_needed as usize])
        }

        /*
           2. Encode float values
        */

        // < 8 after loop
        let mut min_start_block = u8::MAX;
        let mut max_end_block = u8::MIN;
        for xored_val in self.v_xors {
            if xored_val == 0 {
                // pretend it's some arbitrary value that fits in the interval - otherwise
                // choose the middle block in the float
                if min_start_block == u8::MAX {
                    min_start_block = 4;
                    max_end_block = 4;
                }
            } else {
                min_start_block = u8::min(min_start_block, (xored_val.leading_zeros() / 8) as u8);
                max_end_block = u8::max(max_end_block, (7 - xored_val.trailing_zeros() / 8) as u8);
            }
        }

        let meaningful_len = max_end_block - min_start_block + 1;
        let shift = 7 - max_end_block;

        self.encoded_xor_info_header |=
            ((meaningful_len - 1) as u32) << (21 - 6 * (self.chunk_idx as u32));
        self.encoded_xor_info_header |= (shift as u32) << (18 - 6 * (self.chunk_idx as u32));
        for value_xor in self.v_xors {
            let shifted = value_xor >> (shift * 8);
            self.temp_buffer
                .extend_from_slice(&shifted.to_le_bytes()[..meaningful_len as usize])
        }

        self.chunk_idx += 1;

        if self.chunk_idx >= V1_NUM_CHUNKS_PER_LENGTH {
            self.flush_chunk();
        }
        self.buffer_idx = 0;
    }

    fn flush_chunk(&mut self) {
        if self.chunk_idx == 0 {
            return;
        }
        self.result.push(self.encoded_length_header);
        self.result
            .extend_from_slice(&self.encoded_xor_info_header.to_be_bytes()[1..]);
        self.result.append(&mut self.temp_buffer);
        self.chunk_idx = 0;
        self.encoded_length_header = 0;
        self.encoded_xor_info_header = 0;
    }
}

pub struct DecompressionEngineV1<T: Read> {
    reader: T,

    values_read: u32,
    cur_encoded_length_header: u8,
    cur_encoded_xor_info_header: u32,

    chunk_idx: u32,
    buffer_idx: u32,

    current_timestamp: Timestamp,
    current_value: u64,

    last_ts_delta: i64,

    ts_d_deltas: [i64; V1_CHUNK_SIZE],
    v_xors: [u64; V1_CHUNK_SIZE],
}

impl<T: Read> DecompressionEngine<T> for DecompressionEngineV1<T> {
    type PhysicalType = f64;

    fn new(reader: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            reader,
            values_read: 1,
            cur_encoded_length_header: 0,
            cur_encoded_xor_info_header: 0,

            chunk_idx: V1_NUM_CHUNKS_PER_LENGTH as u32,
            buffer_idx: V1_CHUNK_SIZE as u32,

            current_timestamp: header.min_timestamp,
            current_value: header.first_value.get_float64().to_bits(),

            last_ts_delta: 0,

            ts_d_deltas: [0; V1_CHUNK_SIZE],
            v_xors: [0; V1_CHUNK_SIZE],
        }
    }

    fn next(&mut self) -> (Timestamp, Self::PhysicalType) {
        if self.buffer_idx >= V1_CHUNK_SIZE as u32 {
            if self.chunk_idx >= V1_NUM_CHUNKS_PER_LENGTH as u32 {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf).unwrap();
                self.cur_encoded_length_header = buf[0];
                self.cur_encoded_xor_info_header = u32::from_be_bytes(buf);
                self.chunk_idx = 0;
            }

            let length_code = (self.cur_encoded_length_header >> (6 - self.chunk_idx * 2)) & 0b11;
            let timestamp_bytes = INT_CODE_TO_BYTES[length_code as usize] as usize;
            let float_bytes = ((self.cur_encoded_xor_info_header >> (21 - self.chunk_idx * 6))
                & 0b111) as usize
                + 1;

            let shift =
                ((self.cur_encoded_xor_info_header >> (18 - self.chunk_idx * 6)) & 0b111) as usize;

            let total_bytes = (timestamp_bytes + float_bytes) * V1_CHUNK_SIZE;
            let mut buf = [0u8; V1_CHUNK_SIZE * 16];
            self.reader.read_exact(&mut buf[..total_bytes]).unwrap();

            for (i, x) in self.ts_d_deltas.iter_mut().enumerate().take(V1_CHUNK_SIZE) {
                let byte_idx = timestamp_bytes * i;
                let val = V1_INT_READERS[length_code as usize](
                    &buf[byte_idx..byte_idx + timestamp_bytes],
                );
                *x = IntCompressionUtils::zig_zag_decode(val);
            }
            for (i, x) in self.v_xors.iter_mut().enumerate().take(V1_CHUNK_SIZE) {
                let byte_idx = timestamp_bytes * V1_CHUNK_SIZE + i * float_bytes;
                let mut val =
                    IntCompressionUtils::varint_u64(&buf[byte_idx..byte_idx + float_bytes]);
                val <<= shift * 8;
                *x = val;
            }

            self.chunk_idx += 1;
            self.buffer_idx = 0;
        }

        self.last_ts_delta += self.ts_d_deltas[self.buffer_idx as usize];
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_ts_delta);
        self.current_value ^= self.v_xors[self.buffer_idx as usize];

        self.values_read += 1;
        self.buffer_idx += 1;

        (self.current_timestamp, f64::from_bits(self.current_value))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        storage::compression::{CompressionEngine, DecompressionEngine, Header},
        StreamId, ValueType, Version,
    };

    use super::{CompressionEngineV1, DecompressionEngineV1};

    #[test]
    fn test_compression_v2_basic() {
        let header = Header::new(Version(0), StreamId(0), ValueType::Float64);
        let timestamps = [1, 2, 20, 255, 324, 345, 1024, 2056, 5000];
        let values = [
            23.43,
            45.45,
            f64::MAX,
            24.3,
            -1.0,
            f64::MIN,
            23.0,
            f64::INFINITY,
            -34.4,
        ];

        let mut res: Vec<u8> = Vec::new();
        let mut engine = CompressionEngineV1::<&mut Vec<u8>>::new(&mut res, &header);
        for i in 0..timestamps.len() {
            engine.consume(timestamps[i], values[i])
        }
        engine.flush_all();

        let mut decomp = DecompressionEngineV1::<&[u8]>::new(&res, &header);
        for i in 0..timestamps.len() {
            let (t, v) = decomp.next();
            assert_eq!(t, timestamps[i]);
            assert_eq!(v, values[i]);
        }
    }
}
