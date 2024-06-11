use std::mem::size_of;

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
                bytes_needed = CompressionEngine::bytes_needed_u64(values[i + j]);
                length |= CompressionEngine::length_encoding(bytes_needed) << (6 - 2 * j);
            }

            result.push(length);
            for j in 0..4 {
                CompressionEngine::encode_value(values[i + j], &mut result);
            }
            i += 4;
        }

        if values.len() % 4 > 0 {
            let mut length = 0u8;
            for i in (values.len() - (values.len() % 4))..values.len() {
                let bytes_needed = CompressionEngine::bytes_needed_u64(values[i]);
                length |= CompressionEngine::length_encoding(bytes_needed)
                    << (6 - 2 * (i - (values.len() - (values.len() % 4))));
            }
            result.push(length);
        }

        for i in (values.len() - (values.len() % 4))..values.len() {
            CompressionEngine::encode_value(values[i], &mut result);
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
        let n_bytes = CompressionEngine::bytes_needed_u64(n);
        let n_bytes = EXPONENTS[CompressionEngine::length_encoding(n_bytes) as usize];
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

    pub fn zig_zag_decode(n: u64) -> i64 {
        (((n >> 1) as i64) ^ -((n & 1) as i64)) as i64
    }
    pub fn zig_zag_encode(n: i64) -> u64 {
        ((n >> (size_of::<i64>() * 8 - 1)) ^ (n << 1)) as u64
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
