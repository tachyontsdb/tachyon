use crate::storage::FileReaderUtils;

use super::*;

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

    Based on Google compression algorithm: https://static.googleusercontent.com/media/research.google.com/en//people/jeff/WSDM09-keynote.pdf
*/

/*
Examples of Double Delta + Zig Zag

Example 1
1 5 6 9 12 13 19 23 24 29 32

1st deltas
4 1 3 3 1 6 4 1 5 3

2nd deltas
-3 2 0 -2 5 -3 -3 4 -2

Zig Zag:
5 4 0 3 10 5 5 8 3

----------------------
Example 2
1 3 5 7 10 13 15 16 18 20 24

1st
2 2 2 3 3 2 1 2 2 4

2nd
0 0 1 0 -1 -1 1 0 2

Zig Zag:
0 0 2 0 1 1 2 0 4

*/
pub struct V1;
pub type PhysicalType = u64;

const EXPONENTS: [usize; 4] = [1, 2, 4, 8];

const VAR_U64_READERS: [fn(&[u8]) -> u64; 4] = [
    FileReaderUtils::read_u64_1,
    FileReaderUtils::read_u64_2,
    FileReaderUtils::read_u64_4,
    FileReaderUtils::read_u64_8,
];

pub struct CompressionEngineV1<T: Write> {
    writer: T,
    last_timestamp: Timestamp,
    last_value: PhysicalType,
    last_deltas: (i64, i64),
    entries_written: u32,

    buffer: [u64; 4],
    buffer_idx: usize,

    result: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for CompressionEngineV1<T> {
    type PhysicalType = PhysicalType;
    fn new(writer: T, header: &Header) -> Self {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value.get_uinteger64(),
            last_deltas: (0, 0),
            entries_written: 0,

            buffer: [0; 4],
            buffer_idx: 0,

            result: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: PhysicalType) {
        let curr_deltas = (
            (timestamp - self.last_timestamp) as i64,
            (value - self.last_value) as i64,
        );

        let double_delta_1 = curr_deltas.0 - self.last_deltas.0;
        let double_delta_2 = curr_deltas.1 - self.last_deltas.1;
        self.buffer[self.buffer_idx] = IntCompressionUtils::zig_zag_encode(double_delta_1);
        self.buffer[self.buffer_idx + 1] = IntCompressionUtils::zig_zag_encode(double_delta_2);

        self.buffer_idx += 2;
        if self.buffer_idx >= self.buffer.len() {
            self.flush();
        }

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
    }

    fn flush_all(&mut self) -> usize {
        self.flush();
        self.writer.write_all(&self.result).unwrap();
        self.result.len()
    }
}

impl<T: Write> CompressionEngineV1<T> {
    // Called when the local buffer can be written along with length byte
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
            1
        } else if n <= 4 {
            2
        } else if n <= 8 {
            3
        } else {
            panic!("Integer greater than 8 bytes: {}!", n);
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
    current_value: PhysicalType,
    last_deltas: (i64, i64),

    next_timestamp: Timestamp,
    next_value: PhysicalType,
}

impl<T: Read> DecompressionEngine<T> for DecompressionEngineV1<T> {
    type PhysicalType = u64;

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
            current_value: header.first_value.get_uinteger64(),
            last_deltas: (0, 0),

            next_timestamp: 0,
            next_value: 0,
        }
    }

    fn next(&mut self) -> (Timestamp, PhysicalType) {
        if self.values_read % 2 == 0 {
            self.current_timestamp = self.next_timestamp;
            self.current_value = self.next_value;
            self.values_read += 1;
            return (self.current_timestamp, self.current_value);
        }

        // Compute integer lengths
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

        // Read deltas + next length byte
        let mut buffer = [0u8; 2 * (size_of::<Timestamp>() + size_of::<PhysicalType>()) + 1];
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
            decoded_deltas[i] = IntCompressionUtils::zig_zag_decode(encoded);
        }

        // Compute timestamp / value
        self.last_deltas.0 += decoded_deltas[0];
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[1];
        self.current_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        // Compute next timestamp / value
        self.last_deltas.0 += decoded_deltas[2];
        self.next_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[3];
        self.next_value = self.current_value.wrapping_add_signed(self.last_deltas.1);

        // Update state
        self.values_read += 1;
        self.cur_length_byte = buffer[total_varint_lengths];

        (self.current_timestamp, self.current_value)
    }
}
