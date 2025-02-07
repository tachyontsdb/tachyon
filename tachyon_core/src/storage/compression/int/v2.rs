use std::io::{Read, Write};

use crate::{
    storage::{
        compression::{CompressionEngine, DecompressionEngine},
        file::Header,
        FileReaderUtils,
    },
    utils::static_assert,
    Timestamp,
};

use super::IntCompressionUtils;
use super::TimeDataFile;

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
pub type PhysicalType = u64;

const V2_CHUNK_SIZE: usize = 16;
static_assert!(V2_CHUNK_SIZE % 8 == 0);

const V2_NUM_CHUNKS_PER_LENGTH: usize = 8;
static_assert!(V2_NUM_CHUNKS_PER_LENGTH <= 8);

const V2_CODE_TO_BITS: [u8; 8] = [1, 2, 4, 8, 16, 24, 32, 64];
const V2_LENGTH_LOOKUP: [u8; 65] = [
    0, 0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6,
    6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7,
];

const V2_INT_READERS: [fn(&[u8]) -> u64; 5] = [
    FileReaderUtils::read_u64_1,
    FileReaderUtils::read_u64_2,
    FileReaderUtils::read_u64_3,
    FileReaderUtils::read_u64_4,
    FileReaderUtils::read_u64_8,
];

pub struct CompressionEngineV2<T: Write> {
    writer: T,
    last_timestamp: Timestamp,
    last_value: PhysicalType,
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
    type PhysicalType = u64;

    fn new(writer: T, header: &Header) -> Self {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value.get_uinteger64(),
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

    fn new_from_partial(writer: T, data_file: TimeDataFile) -> Self {
        Self {
            writer,
            last_timestamp: *data_file.timestamps.last().unwrap(),
            last_value: data_file.values.last().unwrap().get_uinteger64(),
            last_deltas: if data_file.num_entries() < 2 {
                (0, 0)
            } else {
                (
                    data_file.timestamps[data_file.num_entries() - 1] as i64
                        - data_file.timestamps[data_file.num_entries() - 2] as i64,
                    data_file.values[data_file.num_entries() - 1].get_integer64()
                        - data_file.values[data_file.num_entries() - 2].get_integer64(),
                )
            },
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

    fn consume(&mut self, timestamp: Timestamp, value: PhysicalType) -> usize {
        // TODO: Check wrapping logic here
        let mut bytes_written = 0;
        let curr_deltas = (
            (timestamp.wrapping_sub(self.last_timestamp)) as i64,
            (value.wrapping_sub(self.last_value)) as i64,
        );

        let double_delta = curr_deltas.0 - (self.last_deltas.0);
        self.ts_d_deltas[self.buffer_idx] = IntCompressionUtils::zig_zag_encode(double_delta);

        let double_delta = curr_deltas.1 - (self.last_deltas.1);
        self.v_d_deltas[self.buffer_idx] = IntCompressionUtils::zig_zag_encode(double_delta);

        self.buffer_idx += 1;
        if self.buffer_idx >= V2_CHUNK_SIZE {
            bytes_written += self.flush();
        }

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
        bytes_written
    }

    fn flush_all(&mut self) -> usize {
        self.flush();
        self.flush_chunk();
        self.writer.write(&self.result).unwrap()
    }
}

impl<T: Write> CompressionEngineV2<T> {
    // Called when the local buffer can be written along with length byte
    fn flush(&mut self) -> usize {
        if self.buffer_idx == 0 {
            return 0;
        }

        let mut bytes_written = 0;

        // Handle partially-filled buffers
        for i in self.buffer_idx..self.ts_d_deltas.len() {
            self.ts_d_deltas[i] = 0;
            self.v_d_deltas[i] = 0;
        }

        // Write the chunk for timestamps & deltas
        for arr in [&self.ts_d_deltas, &self.v_d_deltas] {
            let mut max_bits_needed = 0;
            for x in arr {
                max_bits_needed = u8::max(max_bits_needed, Self::bits_needed_u64(*x));
            }

            let length_code = V2_LENGTH_LOOKUP[max_bits_needed as usize];
            self.cur_length |= (length_code as u32) << (21 - 3 * (self.chunk_idx as u32));
            max_bits_needed = V2_CODE_TO_BITS[length_code as usize];

            if max_bits_needed < 8 {
                // Bitpack each value
                let mut byte: u8 = 0;
                let values_per_byte = 8 / max_bits_needed as usize;
                for (i, x) in arr.iter().enumerate().take(V2_CHUNK_SIZE) {
                    let shift =
                        8 - max_bits_needed - (max_bits_needed) * (i % values_per_byte) as u8;
                    byte |= (*x as u8) << shift;
                    if i % values_per_byte == values_per_byte - 1 {
                        self.temp_buffer.push(byte);
                        byte = 0;
                    }
                }
            } else {
                // Varint encode each integer
                for x in arr {
                    let bytes_needed = (max_bits_needed / 8) as usize;
                    self.temp_buffer
                        .extend_from_slice(&x.to_le_bytes()[..bytes_needed])
                }
            }

            self.chunk_idx += 1;
        }

        if self.chunk_idx >= V2_NUM_CHUNKS_PER_LENGTH {
            bytes_written += self.flush_chunk();
        }

        self.buffer_idx = 0;
        bytes_written
    }

    fn flush_chunk(&mut self) -> usize {
        if self.chunk_idx == 0 {
            return 0;
        }

        self.result
            .extend_from_slice(&self.cur_length.to_be_bytes()[1..]);
        self.result.append(&mut self.temp_buffer);
        let bytes_written = self.writer.write(&self.result).unwrap(); // we persist any data we have
        self.clear();
        bytes_written
    }

    fn clear(&mut self) {
        self.chunk_idx = 0;
        self.cur_length = 0;
        self.result = Vec::new();
        self.temp_buffer = Vec::new();
    }

    #[inline]
    fn bits_needed_u64(n: u64) -> u8 {
        64 - n.leading_zeros() as u8
    }
}

pub struct DecompressionEngineV2<T: Read> {
    reader: T,

    values_read: u32,
    cur_length: u32,

    chunk_idx: u32,
    buffer_idx: u32,

    current_timestamp: Timestamp,
    current_value: PhysicalType,
    last_deltas: (i64, i64),

    ts_d_deltas: [i64; V2_CHUNK_SIZE],
    v_d_deltas: [i64; V2_CHUNK_SIZE],
}

impl<T: Read> DecompressionEngine<T> for DecompressionEngineV2<T> {
    type PhysicalType = PhysicalType;

    fn new(reader: T, header: &Header) -> Self {
        Self {
            reader,

            values_read: 1,
            cur_length: 0,

            chunk_idx: V2_NUM_CHUNKS_PER_LENGTH as u32,
            buffer_idx: V2_CHUNK_SIZE as u32,

            current_timestamp: header.min_timestamp,
            current_value: header.first_value.get_uinteger64(),
            last_deltas: (0, 0),

            ts_d_deltas: [0; V2_CHUNK_SIZE],
            v_d_deltas: [0; V2_CHUNK_SIZE],
        }
    }

    fn next(&mut self) -> (Timestamp, PhysicalType) {
        if self.buffer_idx >= V2_CHUNK_SIZE as u32 {
            // Read the next chunk
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

                // Decode timestamp deltas
                if num_bits < 8 {
                    for (i, x) in arr.iter_mut().enumerate().take(V2_CHUNK_SIZE) {
                        let byte_idx = (num_bits as usize) * i / 8;
                        let bit_idx = ((num_bits as usize) * i) % 8;
                        let shift = 8 - num_bits as usize - bit_idx;

                        let encoded = ((buf[byte_idx] >> shift) & ((1 << num_bits) - 1)) as u64;
                        *x = IntCompressionUtils::zig_zag_decode(encoded);
                    }
                } else {
                    for (i, x) in arr.iter_mut().enumerate().take(V2_CHUNK_SIZE) {
                        let byte_idx = (num_bits as usize) / 8 * i;
                        let val = V2_INT_READERS[length_code as usize - 3](
                            &buf[byte_idx..byte_idx + num_bits as usize / 8],
                        );
                        *x = IntCompressionUtils::zig_zag_decode(val);
                    }
                }
                self.chunk_idx += 1;
            }

            self.buffer_idx = 0;
        }

        self.last_deltas.0 += self.ts_d_deltas[self.buffer_idx as usize];
        self.last_deltas.1 += self.v_d_deltas[self.buffer_idx as usize];

        // TODO: Check wrapping logic here
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);
        self.current_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        self.values_read += 1;
        self.buffer_idx += 1;

        (self.current_timestamp, self.current_value)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompressionEngine, CompressionEngineV2, DecompressionEngine, DecompressionEngineV2,
        V2_CHUNK_SIZE,
    };
    use crate::storage::file::Header;
    use crate::{StreamId, ValueType, Version};

    #[test]
    fn test_shift() {
        fn case(max_bits_needed: u8, i: u8) -> u8 {
            let values_per_byte = 8 / max_bits_needed;
            8 - max_bits_needed - (max_bits_needed) * (i % values_per_byte)
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
        let header = Header::new(Version(0), StreamId(0), ValueType::UInteger64);
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

        let mut decomp = DecompressionEngineV2::<&[u8]>::new(
            &result,
            &Header::new(Version(0), StreamId(0), ValueType::UInteger64),
        );

        let (time, value) = decomp.next();
        assert_eq!(time, 1);
        assert_eq!(value, 2);

        let (time, value) = decomp.next();
        assert_eq!(time, 2);
        assert_eq!(value, 1);
    }

    #[test]
    fn test_compression_v2_read_back() {
        let header = Header {
            min_timestamp: 1,
            first_value: 34u64.into(),
            ..Header::new(Version(0), StreamId(0), ValueType::UInteger64)
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
        let mut engine = CompressionEngineV2::<&mut Vec<u8>>::new(&mut res, &header);
        for i in 0..timestamps.len() {
            engine.consume(timestamps[i], values[i]);
        }
        engine.flush_all();

        let mut decomp = DecompressionEngineV2::<&[u8]>::new(&res, &header);
        for i in 0..timestamps.len() {
            let (t, v) = decomp.next();
            assert_eq!(t, timestamps[i]);
            assert_eq!(v, values[i]);
        }

        let header = Header {
            min_timestamp: 0,
            first_value: 0u64.into(),
            ..Header::new(Version(0), StreamId(0), ValueType::UInteger64)
        };

        let timestamps = [1, 2, 3];

        let values = [5, 9, 12];
        let mut res: Vec<u8> = Vec::new();
        let mut engine = CompressionEngineV2::<&mut Vec<u8>>::new(&mut res, &header);
        for i in 0..timestamps.len() {
            engine.consume(timestamps[i], values[i]);
        }
        engine.flush_all();

        let mut decomp = DecompressionEngineV2::<&[u8]>::new(&res, &header);
        for i in 0..timestamps.len() {
            let (t, v) = decomp.next();
            assert_eq!(t, timestamps[i]);
            assert_eq!(v, values[i]);
        }
    }
}
