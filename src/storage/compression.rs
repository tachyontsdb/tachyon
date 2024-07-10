use std::{
    cmp::max,
    io::{Read, Write},
    mem::size_of,
};

use crate::{
    common::{Timestamp, Value},
    utils::{common::static_assert, file_utils::FileReaderUtil},
};

use super::file::Header;

const EXPONENTS: [usize; 4] = [1, 2, 4, 8];

const VAR_U64_READERS: [fn(&[u8]) -> u64; 4] = [
    FileReaderUtil::read_u64_1,
    FileReaderUtil::read_u64_2,
    FileReaderUtil::read_u64_4,
    FileReaderUtil::read_u64_8,
];

pub struct CompressionUtils;
impl CompressionUtils {
    #[inline]
    pub fn zig_zag_decode(n: u64) -> i64 {
        (((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    #[inline]
    pub fn zig_zag_encode(n: i64) -> u64 {
        ((n >> (i64::BITS as usize - 1)) ^ (n << 1)) as u64
    }
}

pub trait CompressionEngine<T: Write> {
    fn new(writer: T, header: &Header) -> Self
    where
        Self: Sized;
    fn consume(&mut self, timestamp: Timestamp, value: Value);
    fn bytes_compressed(&self) -> usize;
    fn flush_all(&mut self);
}

pub trait DecompressionEngine<T: Read> {
    fn new(reader: T, header: &Header) -> Self
    where
        Self: Sized;
    fn next(&mut self) -> (Timestamp, Value);
}

pub trait CompressionScheme<R: Read, W: Write> {
    type Decompressor: DecompressionEngine<R>;
    type Compressor: CompressionEngine<W>;
}

/*
    Compression Scheme V1:
--------------------------------------------------------
    Encoded Header
    ---------------------
    | XX | XX | XX | XX |  ... [ NUMBER 1 ] ... [ NUMBER 2 ] ... [ NUMBER 3 ] ... [ NUMBER 4 ]
    ---------------------

    XX
    - 00 -> Number is 1 byte (0 to 255)
    - 01 -> Number is 2 bytes ( signed: -32,768 to 32,767 )
    - 10 -> Number is 4 bytes
    - 11 -> Number is 8 bytes

    Based on google compression algorithm: https://static.googleusercontent.com/media/research.google.com/en//people/jeff/WSDM09-keynote.pdf
*/

/*

Examples of Double Delta + Zig Zag

Example 1
1 5 6 9 12 13 19 23 24 29 32

1st deltas
4 1 3 3 1 6 4 1 5 3

2nd deltas
-3 2 0 -2 5 -3 -3 4 -2

zig zag:
5 4 0 3 10 5 5 8 3

----------------------
Example 2
1 3 5 7 10 13 15 16 18 20 24

1st
2 2 2 3 3 2 1 2 2 4

2nd
0 0 1 0 -1 -1 1 0 2

zig:
0 0 2 0 1 1 2 0 4

*/
pub struct V1;
impl<R: Read, W: Write> CompressionScheme<R, W> for V1 {
    type Decompressor = DecompressionEngineV1<R>;
    type Compressor = CompressionEngineV1<W>;
}

pub struct CompressionEngineV1<T: Write> {
    writer: T,
    last_timestamp: Timestamp,
    last_value: Value,
    last_deltas: (i64, i64),
    entries_written: u32,

    buffer: [u64; 4],
    buffer_idx: usize,

    result: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for CompressionEngineV1<T> {
    fn new(writer: T, header: &Header) -> Self {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value,
            last_deltas: (0, 0),
            entries_written: 0,

            buffer: [0; 4],
            buffer_idx: 0,
            result: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Value) {
        let curr_deltas = (
            (timestamp.wrapping_sub(self.last_timestamp)) as i64,
            (value.wrapping_sub(self.last_value)) as i64,
        );

        let double_delta_1 = curr_deltas.0 - self.last_deltas.0;
        let double_delta_2 = curr_deltas.1 - self.last_deltas.1;
        self.buffer[self.buffer_idx] = CompressionUtils::zig_zag_encode(double_delta_1);
        self.buffer[self.buffer_idx + 1] = CompressionUtils::zig_zag_encode(double_delta_2);

        self.buffer_idx += 2;
        if self.buffer_idx >= self.buffer.len() {
            self.flush();
        }

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
    }

    fn bytes_compressed(&self) -> usize {
        self.result.len()
    }

    fn flush_all(&mut self) {
        self.flush();
        self.writer.write_all(&self.result).unwrap();
    }
}

impl<T: Write> CompressionEngineV1<T> {
    // called when the local buffer can be written along with length byte
    fn flush(&mut self) {
        if self.buffer_idx == 0 {
            return;
        }

        let mut length = 0u8;
        let mut bytes_needed: u8;

        for j in 0..self.buffer_idx {
            bytes_needed = Self::bytes_needed_u64(self.buffer[j]);
            length |= Self::length_encoding(bytes_needed) << (6 - 2 * j);
        }
        self.result.push(length);

        for i in 0..self.buffer_idx {
            self.encode_value(self.buffer[i]);
        }

        self.buffer_idx = 0;
    }

    fn length_encoding(n: u8) -> u8 {
        if n == 1 {
            0
        } else if n == 2 {
            return 1;
        } else if n <= 4 {
            return 2;
        } else if n <= 8 {
            return 3;
        } else {
            panic!("Integer greater than 8 bytes: {}.", n);
        }
    }
    fn encode_value(&mut self, n: u64) {
        const EXPONENTS: [u8; 4] = [1, 2, 4, 8];
        let n_bytes = Self::bytes_needed_u64(n);
        let n_bytes = EXPONENTS[Self::length_encoding(n_bytes) as usize];
        let bytes = n.to_le_bytes();
        self.result.extend_from_slice(&bytes[0..n_bytes as usize]);
    }

    fn bytes_needed_u64(n: u64) -> u8 {
        if n == 0 {
            return 1;
        }
        let mut bytes = 0;
        let mut temp = n;
        while temp > 0 {
            bytes += 1;
            temp >>= 8; // Shift right by 8 bits (1 byte)
        }
        bytes
    }
}

pub struct DecompressionEngineV1<T: Read> {
    reader: T,

    values_read: u32,
    cur_length_byte: u8,

    current_timestamp: Timestamp,
    current_value: Value,
    last_deltas: (i64, i64),

    next_timestamp: Timestamp,
    next_value: Value,
}

impl<T: Read> DecompressionEngine<T> for DecompressionEngineV1<T> {
    fn new(mut reader: T, header: &Header) -> Self {
        let mut l_buf = [0u8; 1];
        if header.count > 1 {
            reader.read_exact(&mut l_buf).unwrap();
        }
        Self {
            reader,

            values_read: 1,
            cur_length_byte: l_buf[0],

            current_timestamp: header.min_timestamp,
            current_value: header.first_value,
            last_deltas: (0, 0),
            next_timestamp: 0,
            next_value: 0,
        }
    }

    fn next(&mut self) -> (Timestamp, Value) {
        if self.values_read % 2 == 0 {
            self.current_timestamp = self.next_timestamp;
            self.current_value = self.next_value;
            self.values_read += 1;
            return (self.current_timestamp, self.current_value);
        }

        // compute integer lengths
        let indexes: [usize; 4] = [
            ((self.cur_length_byte >> 6) & 0b11) as usize,
            ((self.cur_length_byte >> 4) & 0b11) as usize,
            ((self.cur_length_byte >> 2) & 0b11) as usize,
            ((self.cur_length_byte) & 0b11) as usize,
        ];

        let total_varint_lengths = EXPONENTS[indexes[0]]
            + EXPONENTS[indexes[1]]
            + EXPONENTS[indexes[2]]
            + EXPONENTS[indexes[3]];

        // read deltas + next length byte
        let mut buffer = [0u8; 2 * (size_of::<Timestamp>() + size_of::<Value>()) + 1];
        self.reader
            .read_exact(&mut buffer[0..total_varint_lengths + 1])
            .unwrap();

        let mut decoded_deltas = [0i64; 4];
        let mut buf_offset = 0;

        for i in 0..4 {
            let encoded = VAR_U64_READERS[indexes[i]](
                &buffer[buf_offset..buf_offset + EXPONENTS[indexes[i]]],
            );
            buf_offset += EXPONENTS[indexes[i]];
            decoded_deltas[i] = CompressionUtils::zig_zag_decode(encoded);
        }

        // compute timestamp / value
        self.last_deltas.0 += decoded_deltas[0];
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[1];
        self.current_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        // compute next timestamp / value
        self.last_deltas.0 += decoded_deltas[2];
        self.next_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[3];
        self.next_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        // update state
        self.values_read += 1;
        self.cur_length_byte = buffer[total_varint_lengths];

        (self.current_timestamp, self.current_value)
    }
}

/*
    Compression Scheme V2:
--------------------------------------------------------
    Encoded Header 3 Bytes (Big-endian encoded)
    ---------------------------------------------------------
    | XXX | XXX | XX || X | XXX | XXX | X || XX | XXX | XXX |
    ---------------------------------------------------------

    || -> Byte Boundary
    |  -> Chunk boundary (each 3 bits represents length encoding)

    Each XXX represents the length of each integer of the chunk. It will be max length
    among the integers in the chunk.

    XXX
    - 000 -> 1 bit
    - 001 -> 2 bits
    - 010 -> 4 bits
    - 011 -> 1 byte
    - 100 -> 2 bytes
    - 101 -> 3 bytes
    - 110 -> 4 bytes
    - 111 -> 8 bytes

    Following the 3-byte length header, V2_NUM_CHUNKS_PER_LENGTH chunks follow, with each chunk containing
    V2_CHUNK_SIZE integers.

    Each integer is either bitpacked (for 1,2,4 bit representations):
        - e.g. 2 bits: 00 10 11 00 (values: 0, 2, 3, 0)

    or encoded in little-endian format otherwise.
*/

pub struct V2;
impl<R: Read, W: Write> CompressionScheme<R, W> for V2 {
    type Decompressor = DecompressionEngineV2<R>;
    type Compressor = CompressionEngineV2<W>;
}

const V2_CHUNK_SIZE: usize = 16;
static_assert!(V2_CHUNK_SIZE % 8 == 0);

const V2_NUM_CHUNKS_PER_LENGTH: usize = 8;
static_assert!(V2_NUM_CHUNKS_PER_LENGTH <= 8);

const V2_CODE_TO_BITS: [u8; 8] = [1, 2, 4, 8, 16, 24, 32, 64];
const V2_INT_READERS: [fn(&[u8]) -> u64; 5] = [
    FileReaderUtil::read_u64_1,
    FileReaderUtil::read_u64_2,
    FileReaderUtil::read_u64_3,
    FileReaderUtil::read_u64_4,
    FileReaderUtil::read_u64_8,
];

pub struct CompressionEngineV2<T: Write> {
    writer: T,
    last_timestamp: Timestamp,
    last_value: Value,
    last_deltas: (i64, i64),
    entries_written: u32,

    ts_d_deltas: [u64; V2_CHUNK_SIZE],
    v_d_deltas: [u64; V2_CHUNK_SIZE],
    buffer_idx: usize,
    chunk_idx: usize,
    cur_length: u32,

    result: Vec<u8>,
    temp_buffer: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for CompressionEngineV2<T> {
    fn new(writer: T, header: &Header) -> Self {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value,
            last_deltas: (0, 0),
            entries_written: 0,

            ts_d_deltas: [0; V2_CHUNK_SIZE],
            v_d_deltas: [0; V2_CHUNK_SIZE],
            buffer_idx: 0,
            chunk_idx: 0,
            cur_length: 0,
            result: Vec::new(),
            temp_buffer: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Value) {
        let curr_deltas = (
            (timestamp.wrapping_sub(self.last_timestamp)) as i64,
            (value.wrapping_sub(self.last_value)) as i64,
        );

        let double_delta = curr_deltas.0 - self.last_deltas.0;
        self.ts_d_deltas[self.buffer_idx] = CompressionUtils::zig_zag_encode(double_delta);

        let double_delta = curr_deltas.1 - self.last_deltas.1;
        self.v_d_deltas[self.buffer_idx] = CompressionUtils::zig_zag_encode(double_delta);

        self.buffer_idx += 1;
        if self.buffer_idx >= V2_CHUNK_SIZE {
            self.flush();
        }

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
    }

    fn bytes_compressed(&self) -> usize {
        self.result.len()
    }

    fn flush_all(&mut self) {
        self.flush();
        self.flush_chunk();
        self.writer.write_all(&self.result).unwrap();
    }
}

impl<T: Write> CompressionEngineV2<T> {
    // called when the local buffer can be written along with length byte
    fn flush(&mut self) {
        if self.buffer_idx == 0 {
            return;
        }

        // handle partially-filled buffers
        for i in self.buffer_idx..self.ts_d_deltas.len() {
            self.ts_d_deltas[i] = 0;
            self.v_d_deltas[i] = 0;
        }

        // write the chunk for timestamps & deltas
        for arr in [&self.ts_d_deltas, &self.v_d_deltas] {
            let mut max_bits_needed = 0;
            for x in arr {
                max_bits_needed = max(max_bits_needed, Self::bits_needed_u64(*x));
            }
            self.cur_length |= (Self::length_encoding(max_bits_needed) as u32)
                << (21 - 3 * (self.chunk_idx as u32));

            if max_bits_needed < 8 {
                // bitpack each value
                let mut byte: u8 = 0;
                let values_per_byte = 8 / max_bits_needed as usize;
                for (i, x) in arr.iter().enumerate().take(V2_CHUNK_SIZE) {
                    let shift =
                        (8 - max_bits_needed - (max_bits_needed) * (i % values_per_byte) as u8);
                    byte |= (*x as u8) << shift;
                    if (i % values_per_byte == values_per_byte - 1) {
                        self.temp_buffer.push(byte);
                        byte = 0;
                    }
                }
            } else {
                // varint encode each integer
                for x in arr {
                    let bytes_needed = (max_bits_needed / 8) as usize;
                    self.temp_buffer
                        .extend_from_slice(&x.to_le_bytes()[..bytes_needed])
                }
            }

            self.chunk_idx += 1;
        }

        if self.chunk_idx >= V2_NUM_CHUNKS_PER_LENGTH {
            self.flush_chunk();
        }

        self.buffer_idx = 0;
    }

    fn flush_chunk(&mut self) {
        if self.chunk_idx == 0 {
            return;
        }

        self.result
            .extend_from_slice(&self.cur_length.to_be_bytes()[1..]);
        self.result.append(&mut self.temp_buffer);
        self.chunk_idx = 0;
        self.cur_length = 0;
    }

    fn length_encoding(n: u8) -> u8 {
        for i in 0u8..8u8 {
            if n == V2_CODE_TO_BITS[i as usize] {
                return i;
            }
        }
        panic!("Unknown bit length: {}.", n);
    }

    fn bits_needed_u64(n: u64) -> u8 {
        for bits in &V2_CODE_TO_BITS[..7] {
            if n < (1 << bits) {
                return *bits;
            }
        }
        64
    }
}

pub struct DecompressionEngineV2<T: Read> {
    reader: T,

    values_read: u32,
    cur_length: u32,

    chunk_idx: u32,
    buffer_idx: u32,

    current_timestamp: Timestamp,
    current_value: Value,
    last_deltas: (i64, i64),

    ts_d_deltas: [i64; V2_CHUNK_SIZE],
    v_d_deltas: [i64; V2_CHUNK_SIZE],
}

impl<T: Read> DecompressionEngine<T> for DecompressionEngineV2<T> {
    fn new(mut reader: T, header: &Header) -> Self {
        Self {
            reader,

            values_read: 1,
            cur_length: 0,

            chunk_idx: V2_NUM_CHUNKS_PER_LENGTH as u32,
            buffer_idx: V2_CHUNK_SIZE as u32,

            current_timestamp: header.min_timestamp,
            current_value: header.first_value,
            last_deltas: (0, 0),

            ts_d_deltas: [0; V2_CHUNK_SIZE],
            v_d_deltas: [0; V2_CHUNK_SIZE],
        }
    }

    fn next(&mut self) -> (Timestamp, Value) {
        if self.buffer_idx >= V2_CHUNK_SIZE as u32 {
            // read the next chunk
            if self.chunk_idx >= V2_NUM_CHUNKS_PER_LENGTH as u32 {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf[1..]).unwrap();
                self.cur_length = u32::from_be_bytes(buf);
                self.chunk_idx = 0;
            }

            for arr in [&mut self.ts_d_deltas, &mut self.v_d_deltas] {
                let length_code = (self.cur_length >> (21 - 3 * (self.chunk_idx))) & 0b111;
                let num_bits = V2_CODE_TO_BITS[length_code as usize];

                let num_bytes = (num_bits as usize) * V2_CHUNK_SIZE / 8;
                let mut buf = [0u8; V2_CHUNK_SIZE * 8];
                self.reader.read_exact(&mut buf[..num_bytes]).unwrap();

                // decode timestamp deltas
                if num_bits < 8 {
                    for (i, x) in arr.iter_mut().enumerate().take(V2_CHUNK_SIZE) {
                        let byte_idx = (num_bits as usize) * i / 8;
                        let bit_idx = ((num_bits as usize) * i) % 8;
                        let shift = 8 - num_bits as usize - bit_idx;

                        let encoded = ((buf[byte_idx] >> shift) & ((1 << num_bits) - 1)) as u64;
                        *x = CompressionUtils::zig_zag_decode(encoded);
                    }
                } else {
                    for (i, x) in arr.iter_mut().enumerate().take(V2_CHUNK_SIZE) {
                        let byte_idx = (num_bits as usize) / 8 * i;
                        let val = V2_INT_READERS[length_code as usize - 3](
                            &buf[byte_idx..byte_idx + num_bits as usize / 8],
                        );
                        *x = CompressionUtils::zig_zag_decode(val);
                    }
                }
                self.chunk_idx += 1;
            }

            self.buffer_idx = 0;
        }

        self.last_deltas.0 += self.ts_d_deltas[self.buffer_idx as usize];
        self.last_deltas.1 += self.v_d_deltas[self.buffer_idx as usize];

        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.current_value = self.current_value.wrapping_add_signed(self.last_deltas.1);
        self.values_read += 1;
        self.buffer_idx += 1;
        (self.current_timestamp, self.current_value)
    }
}

pub struct GoogleScheme;
impl<R: Read, W: Write> CompressionScheme<R, W> for GoogleScheme {
    type Decompressor = GoogleDecompressionEngine<R>;
    type Compressor = GoogleCompressionEngine<W>;
}

pub struct GoogleCompressionEngine<T: Write> {
    writer: T,

    last_timestamp: Timestamp,
    last_value: Value,
    last_deltas: (i64, i64),
    entries_written: u32,

    result: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for GoogleCompressionEngine<T> {
    fn new(writer: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value,
            last_deltas: (0, 0),
            entries_written: 0,
            result: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Value) {
        let curr_deltas = (
            (timestamp.wrapping_sub(self.last_timestamp)) as i64,
            (value.wrapping_sub(self.last_value)) as i64,
        );

        let double_delta = curr_deltas.0 - self.last_deltas.0;
        let ts_delta = CompressionUtils::zig_zag_encode(double_delta);
        self.encode(ts_delta);

        let double_delta = curr_deltas.1 - self.last_deltas.1;
        let v_delta = CompressionUtils::zig_zag_encode(double_delta);
        self.encode(v_delta);

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
    }

    fn bytes_compressed(&self) -> usize {
        self.result.len()
    }

    fn flush_all(&mut self) {
        self.writer.write_all(&self.result).unwrap();
    }
}

impl<T: Write> GoogleCompressionEngine<T> {
    fn encode(&mut self, mut val: u64) {
        let mask = ((1 << 7) - 1) as u64;

        if (val == 0) {
            self.result.push(0);
            return;
        }

        while (val > 0) {
            let mut byte = (val & mask) as u8;
            val >>= 7;
            if val > 0 {
                byte |= (1 << 7);
            }
            self.result.push(byte);
        }
    }
}

pub struct GoogleDecompressionEngine<T: Read> {
    reader: T,

    values_read: u32,

    current_timestamp: Timestamp,
    current_value: Value,
    last_deltas: (i64, i64),

    buf: [u8; V2_CHUNK_SIZE],
    buf_idx: usize,
}

impl<T: Read> DecompressionEngine<T> for GoogleDecompressionEngine<T> {
    fn new(reader: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            reader,
            values_read: 0,
            current_timestamp: header.min_timestamp,
            current_value: header.first_value,
            last_deltas: (0, 0),

            buf: [0; V2_CHUNK_SIZE],
            buf_idx: V2_CHUNK_SIZE,
        }
    }

    fn next(&mut self) -> (Timestamp, Value) {
        let decoded_delta_ts = self.decode();
        let decoded_delta_v = self.decode();

        self.last_deltas.0 += decoded_delta_ts;
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_delta_v;
        self.current_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        (self.current_timestamp, self.current_value)
    }
}

impl<T: Read> GoogleDecompressionEngine<T> {
    fn decode(&mut self) -> i64 {
        let mask = ((1 << 7) - 1) as u64;
        let mut temp: u64 = 0;
        let mut offset: u64 = 0;
        loop {
            if self.buf_idx >= V2_CHUNK_SIZE {
                let _ = self.reader.read(&mut self.buf).unwrap();
                self.buf_idx = 0;
            }
            let mut byte = self.buf[self.buf_idx];
            self.buf_idx += 1;
            temp |= ((byte as u64 & mask) << offset);
            offset += 7;
            if byte & (1 << 7) == 0 {
                break;
            }
            assert!(offset < 63);
        }

        CompressionUtils::zig_zag_decode(temp)
    }
}

pub type DefaultScheme = V2;

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, io::Write, rc::Rc};

    use crate::{
        storage::{
            compression::{
                CompressionEngine, CompressionUtils, DecompressionEngine, DecompressionEngineV2,
                GoogleDecompressionEngine, V2_CHUNK_SIZE,
            },
            file::Header,
        },
        utils::{common::static_assert, file_utils::FileReaderUtil},
    };

    use super::{CompressionEngineV2, CompressionScheme, DefaultScheme, GoogleCompressionEngine};
    #[test]
    fn test_shift() {
        fn case(max_bits_needed: u8, i: u8) -> u8 {
            let values_per_byte = 8 / max_bits_needed;
            (8 - max_bits_needed - (max_bits_needed) * (i % values_per_byte))
        }

        assert_eq!(case(1, 0), 7);
        assert_eq!(case(1, 1), 6);
        assert_eq!(case(1, 7), 0);
        assert_eq!(case(1, 8), 7);

        assert_eq!(case(2, 0), 6);
        assert_eq!(case(2, 1), 4);
        assert_eq!(case(2, 3), 0);

        assert_eq!(case(4, 0), 4);
        assert_eq!(case(4, 1), 0);
        assert_eq!(case(4, 2), 4);
    }

    #[test]
    fn test_compression_v2_basic() {
        let mut header = Header::default();
        let mut result = Vec::new();
        let mut engine = CompressionEngineV2::<&mut Vec<u8>>::new(&mut result, &header);

        // d2: 1 0
        // d2: 2 -3

        // d2: 2, 0 (2 bytes)
        // d2: 4, 5 (4 bytes)
        engine.consume(1, 2);
        engine.consume(2, 1);
        engine.flush_all();

        // 0010 1000
        assert_eq!(result.len(), (V2_CHUNK_SIZE / 8) * 6 + 3);
        assert_eq!(result[0], 0x28);
        assert_eq!(result[1], 0x0);
        assert_eq!(result[2], 0x0);

        // 0b1000
        assert_eq!(result[3], 0b10000000);
        assert_eq!(result[4], 0);

        assert_eq!(result[(V2_CHUNK_SIZE * 2) / 8 + 3], 0b01000101);
        assert_eq!(result[(V2_CHUNK_SIZE * 2) / 8 + 4], 0);

        let mut decomp = DecompressionEngineV2::<&[u8]>::new(&result, &Header::default());

        let (time, value) = decomp.next();
        assert_eq!(time, 1);
        assert_eq!(value, 2);

        let (time, value) = decomp.next();
        assert_eq!(time, 2);
        assert_eq!(value, 1);
    }

    #[test]
    fn test_compression_read_back() {
        let header = Header {
            min_timestamp: 1,
            first_value: 34,
            ..Default::default()
        };

        let timestamps = [
            1,
            2,
            20,
            255,
            2048,
            2049,
            10192,
            30000,
            120000,
            120001,
            120002,
            120003,
            130000,
            130000 + u32::MAX as u64,
            123456789012,
            9876543210123,
            u64::MAX,
        ];

        let values = [
            23,
            45,
            u64::MAX,
            2,
            34,
            1234567890123,
            324234,
            1,
            435345,
            345345,
            1,
            2,
            3,
            4,
            5,
            6,
            234,
        ];
        let mut res: Vec<u8> = Vec::new();
        let mut engine = <DefaultScheme as CompressionScheme<&[u8], &mut Vec<u8>>>::Compressor::new(
            &mut res, &header,
        );
        for i in 0..timestamps.len() {
            engine.consume(timestamps[i], values[i])
        }
        engine.flush_all();

        let mut decomp =
            <DefaultScheme as CompressionScheme<&[u8], &mut Vec<u8>>>::Decompressor::new(
                &res, &header,
            );
        for i in 0..timestamps.len() {
            let (t, v) = decomp.next();
            assert_eq!(t, timestamps[i]);
            assert_eq!(v, values[i]);
        }

        let header = Header {
            min_timestamp: 0,
            first_value: 0,
            ..Default::default()
        };

        let timestamps = [1, 2, 3];

        let values = [5, 9, 12];
        let mut res: Vec<u8> = Vec::new();
        let mut engine = CompressionEngineV2::<&mut Vec<u8>>::new(&mut res, &header);
        for i in 0..timestamps.len() {
            engine.consume(timestamps[i], values[i])
        }
        engine.flush_all();

        let mut decomp = DecompressionEngineV2::<&[u8]>::new(&res, &header);
        for i in 0..timestamps.len() {
            let (t, v) = decomp.next();
            assert_eq!(t, timestamps[i]);
            assert_eq!(v, values[i]);
        }
    }

    #[test]
    fn test_google_compression() {
        let mut header = Header::default();
        let mut result = Vec::new();
        let mut engine = GoogleCompressionEngine::<&mut Vec<u8>>::new(&mut result, &header);

        engine.consume(5, 130);
        engine.flush_all();

        assert!(result.len() == 3);
        assert_eq!(result[0], 10);
        assert_eq!(result[1], 0b10000100);
        assert_eq!(result[2], 0b00000010);

        let mut decomp = GoogleDecompressionEngine::<&[u8]>::new(&result, &header);
        let (t, v) = decomp.next();
        assert_eq!(t, 5);
        assert_eq!(v, 130);
    }

    #[test]
    fn test_zig_zag() {
        assert_eq!(0, CompressionUtils::zig_zag_encode(0));
        assert_eq!(1, CompressionUtils::zig_zag_encode(-1));
        assert_eq!(2, CompressionUtils::zig_zag_encode(1));
        assert_eq!(3, CompressionUtils::zig_zag_encode(-2));
        assert_eq!(4, CompressionUtils::zig_zag_encode(2));
        assert_eq!(379, CompressionUtils::zig_zag_encode(-190));
        assert_eq!(80, CompressionUtils::zig_zag_encode(40));
        assert_eq!(254, CompressionUtils::zig_zag_encode(127));
        assert_eq!(256, CompressionUtils::zig_zag_encode(128));

        assert_eq!(0, CompressionUtils::zig_zag_decode(0));
        assert_eq!(-1, CompressionUtils::zig_zag_decode(1));
        assert_eq!(1, CompressionUtils::zig_zag_decode(2));
        assert_eq!(-2, CompressionUtils::zig_zag_decode(3));
        assert_eq!(2, CompressionUtils::zig_zag_decode(4));
        assert_eq!(-5, CompressionUtils::zig_zag_decode(9));
        assert_eq!(-18, CompressionUtils::zig_zag_decode(35));

        assert_eq!(
            64,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(64))
        );

        assert_eq!(
            0,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(0))
        );

        assert_eq!(
            -17,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(-17))
        );

        assert_eq!(
            -12,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(-12))
        );

        assert_eq!(
            130,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(130))
        );

        assert_eq!(
            (i32::MAX) as i64,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode((i32::MAX) as i64))
        );

        assert_eq!(
            i32::MAX as i64 + 3,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(
                (i32::MAX as i64) + 3
            ))
        );

        assert_eq!(
            i64::MAX,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(i64::MAX))
        );

        assert_eq!(
            i64::MIN,
            CompressionUtils::zig_zag_decode(CompressionUtils::zig_zag_encode(i64::MIN))
        );
    }
}
