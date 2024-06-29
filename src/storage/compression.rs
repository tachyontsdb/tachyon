use std::{io::Read, mem::size_of};

use crate::{
    common::{Timestamp, Value},
    utils::file_utils::FileReaderUtil,
};

use super::file::Header;

const EXPONENTS: [usize; 4] = [1, 2, 4, 8];

const VAR_U64_READERS: [fn(&[u8]) -> u64; 4] = [
    FileReaderUtil::read_u64_1,
    FileReaderUtil::read_u64_2,
    FileReaderUtil::read_u64_4,
    FileReaderUtil::read_u64_8,
];

pub struct CompressionEngine;

impl CompressionEngine {
    // A list of bytes. Every 4 bytes is a u64

    /*

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
        A stream of bytes. 8 bytes combine to an integer.

        Timestamp deltas are unsigned.

        Value deltas are unsigned

    */
    pub fn compress(mut values: &Vec<u64>) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();

        let mut i = 0;
        while (i < values.len() / 4 * 4) {
            let mut length = 0u8;
            let mut bytes_needed: u8;
            for j in 0..4 {
                bytes_needed = Self::bytes_needed_u64(values[i + j]);
                length |= Self::length_encoding(bytes_needed) << (6 - 2 * j);
            }

            result.push(length);
            for j in 0..4 {
                Self::encode_value(values[i + j], &mut result);
            }
            i += 4;
        }

        if values.len() % 4 > 0 {
            let mut length = 0u8;
            for i in (values.len() - (values.len() % 4))..values.len() {
                let bytes_needed = Self::bytes_needed_u64(values[i]);
                length |= Self::length_encoding(bytes_needed)
                    << (6 - 2 * (i - (values.len() - (values.len() % 4))));
            }
            result.push(length);
        }

        for val in values[(values.len() - (values.len() % 4))..].iter() {
            Self::encode_value(*val, &mut result);
        }

        result
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
    fn encode_value(n: u64, result: &mut Vec<u8>) {
        const EXPONENTS: [u8; 4] = [1, 2, 4, 8];
        let n_bytes = Self::bytes_needed_u64(n);
        let n_bytes = EXPONENTS[Self::length_encoding(n_bytes) as usize];
        let bytes = n.to_le_bytes();
        result.extend_from_slice(&bytes[0..n_bytes as usize]);
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

    #[inline]
    pub fn zig_zag_decode(n: u64) -> i64 {
        (((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    #[inline]
    pub fn zig_zag_encode(n: i64) -> u64 {
        ((n >> (i64::BITS as usize - 1)) ^ (n << 1)) as u64
    }
}

pub struct DecompressionEngine<T: Read> {
    reader: T,

    values_read: u32,
    cur_length_byte: u8,

    current_timestamp: Timestamp,
    value: Value,
    last_deltas: (i64, i64),

    next_timestamp: Timestamp,
    next_value: Value,
}

impl<T: Read> DecompressionEngine<T> {
    pub fn new(mut reader: T, header: &Header) -> Self {
        let mut l_buf = [0u8; 1];
        if header.count > 1 {
            reader.read_exact(&mut l_buf).unwrap();
        }
        Self {
            reader,

            values_read: 1,
            cur_length_byte: l_buf[0],

            current_timestamp: header.min_timestamp,
            value: header.first_value,
            last_deltas: (0, 0),
            next_timestamp: 0,
            next_value: 0,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> (Timestamp, Value) {
        if self.values_read % 2 == 0 {
            self.current_timestamp = self.next_timestamp;
            self.value = self.next_value;
            self.values_read += 1;
            return (self.current_timestamp, self.value);
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
            decoded_deltas[i] = CompressionEngine::zig_zag_decode(encoded);
        }

        // compute timestamp / value
        self.last_deltas.0 += decoded_deltas[0];
        self.current_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[1];
        self.value = self.value.wrapping_add_signed(self.last_deltas.1);

        // compute next timestamp / value
        self.last_deltas.0 += decoded_deltas[2];
        self.next_timestamp = self
            .current_timestamp
            .wrapping_add_signed(self.last_deltas.0);

        self.last_deltas.1 += decoded_deltas[3];
        self.next_value = self.value.wrapping_add_signed(self.last_deltas.1);

        // update state
        self.values_read += 1;
        self.cur_length_byte = buffer[total_varint_lengths];

        (self.current_timestamp, self.value)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::compression::CompressionEngine;

    #[test]
    fn test_zig_zag() {
        assert_eq!(0, CompressionEngine::zig_zag_encode(0));
        assert_eq!(1, CompressionEngine::zig_zag_encode(-1));
        assert_eq!(2, CompressionEngine::zig_zag_encode(1));
        assert_eq!(3, CompressionEngine::zig_zag_encode(-2));
        assert_eq!(4, CompressionEngine::zig_zag_encode(2));
        assert_eq!(80, CompressionEngine::zig_zag_encode(40));
        assert_eq!(254, CompressionEngine::zig_zag_encode(127));
        assert_eq!(256, CompressionEngine::zig_zag_encode(128));

        assert_eq!(0, CompressionEngine::zig_zag_decode(0));
        assert_eq!(-1, CompressionEngine::zig_zag_decode(1));
        assert_eq!(1, CompressionEngine::zig_zag_decode(2));
        assert_eq!(-2, CompressionEngine::zig_zag_decode(3));
        assert_eq!(2, CompressionEngine::zig_zag_decode(4));
        assert_eq!(-5, CompressionEngine::zig_zag_decode(9));
        assert_eq!(-18, CompressionEngine::zig_zag_decode(35));

        assert_eq!(
            64,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(64))
        );

        assert_eq!(
            0,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(0))
        );

        assert_eq!(
            -17,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(-17))
        );

        assert_eq!(
            -12,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(-12))
        );

        println!("{}", CompressionEngine::zig_zag_encode(130));
        assert_eq!(
            130,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(130))
        );

        assert_eq!(
            (i32::MAX) as i64,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode((i32::MAX) as i64))
        );

        assert_eq!(
            i32::MAX as i64 + 3,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(
                (i32::MAX as i64) + 3
            ))
        );

        assert_eq!(
            i64::MAX,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(i64::MAX))
        );

        assert_eq!(
            i64::MIN,
            CompressionEngine::zig_zag_decode(CompressionEngine::zig_zag_encode(i64::MIN))
        );
    }
}
