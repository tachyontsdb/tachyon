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

        let val_buf = [0u8; 8];

        let mut i = 0;
        while (i < values.len() / 4 * 4) {
            let mut length = 0u8;
            for j in 0..4 {
                let bytes_needed = CompressionEngine::bytes_needed_u64(values[i + j]);
                length |= CompressionEngine::length_encoding(bytes_needed);
                length <<= 2;
            }
            result.push(length);
            for j in 0..4 {
                result.extend_from_slice(&values[i + j].to_le_bytes());
            }
            i += 4;
        }

        let mut length = 0u8;
        for i in (values.len() - (values.len() % 4))..values.len() {
            let bytes_needed = CompressionEngine::bytes_needed_u64(values[i]);
            length |= CompressionEngine::length_encoding(bytes_needed);
            length <<= 2;
        }
        result.push(length);
        for i in (values.len() - (values.len() % 4))..values.len() {
            result.extend_from_slice(&values[i].to_le_bytes());
        }

        result
    }

    fn length_encoding(n: u8) -> u8 {
        if n == 1 {
            return 0;
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

    fn encode_value(n: u64, &mut val_buf: &mut [u8; 8]) {
        let n_bytes = CompressionEngine::bytes_needed_u64(n);
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

    // fn bytes_needed_i64(n: i64) -> u8 {
    //     if n == 0 {
    //         return 1;
    //     }
    //     let mut bytes = 0;
    //     let mut temp = if n < 0 { !n as u64 + 1 } else { n as u64 }; // Two's complement representation for negative numbers

    //     CompressionEngine::bytes_needed_u64(temp.into())
    // }
}
