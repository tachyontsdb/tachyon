use std::io::{Read, Write};

use crate::{
    storage::{
        compression::{CompressionEngine, DecompressionEngine},
        file::Header,
    },
    Timestamp,
};

use super::IntCompressionUtils;

const CHUNK_SIZE: usize = 16;

pub struct GoogleCompressionEngine<T: Write> {
    writer: T,

    last_timestamp: Timestamp,
    last_value: u64,
    last_deltas: (i64, i64),
    entries_written: u32,

    result: Vec<u8>,
}

impl<T: Write> CompressionEngine<T> for GoogleCompressionEngine<T> {
    type PhysicalType = u64;
    fn new(writer: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            writer,
            last_timestamp: header.min_timestamp,
            last_value: header.first_value.get_uinteger64(),
            last_deltas: (0, 0),
            entries_written: 0,
            result: Vec::new(),
        }
    }

    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) {
        let curr_deltas = (
            (timestamp.wrapping_sub(self.last_timestamp)) as i64,
            (value.wrapping_sub(self.last_value)) as i64,
        );

        let double_delta = curr_deltas.0 - self.last_deltas.0;
        let ts_delta = IntCompressionUtils::zig_zag_encode(double_delta);
        self.encode(ts_delta);

        let double_delta = curr_deltas.1 - self.last_deltas.1;
        let v_delta = IntCompressionUtils::zig_zag_encode(double_delta);
        self.encode(v_delta);

        self.entries_written += 1;
        self.last_timestamp = timestamp;
        self.last_value = value;
        self.last_deltas = curr_deltas;
    }

    fn flush_all(&mut self) -> usize {
        self.writer.write_all(&self.result).unwrap();
        self.result.len()
    }
}

impl<T: Write> GoogleCompressionEngine<T> {
    fn encode(&mut self, mut val: u64) {
        let mask = ((1 << 7) - 1) as u64;

        if val == 0 {
            self.result.push(0);
            return;
        }

        while val > 0 {
            let mut byte = (val & mask) as u8;
            val >>= 7;
            if val > 0 {
                byte |= 1 << 7;
            }
            self.result.push(byte);
        }
    }
}

pub struct GoogleDecompressionEngine<T: Read> {
    reader: T,

    values_read: u32,

    current_timestamp: Timestamp,
    current_value: u64,
    last_deltas: (i64, i64),

    buf: [u8; CHUNK_SIZE],
    buf_idx: usize,
}

impl<T: Read> DecompressionEngine<T> for GoogleDecompressionEngine<T> {
    type PhysicalType = u64;
    fn new(reader: T, header: &Header) -> Self
    where
        Self: Sized,
    {
        Self {
            reader,
            values_read: 0,
            current_timestamp: header.min_timestamp,
            current_value: header.first_value.get_uinteger64(),
            last_deltas: (0, 0),

            buf: [0; CHUNK_SIZE],
            buf_idx: CHUNK_SIZE,
        }
    }

    fn next(&mut self) -> (Timestamp, Self::PhysicalType) {
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
            if self.buf_idx >= CHUNK_SIZE {
                let _ = self.reader.read(&mut self.buf).unwrap();
                self.buf_idx = 0;
            }
            let byte = self.buf[self.buf_idx];
            self.buf_idx += 1;
            temp |= (byte as u64 & mask) << offset;
            offset += 7;
            if byte & (1 << 7) == 0 {
                break;
            }
            assert!(offset < 63);
        }

        IntCompressionUtils::zig_zag_decode(temp)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::compression::int::google::{
        GoogleCompressionEngine, GoogleDecompressionEngine,
    };
    use crate::storage::{
        compression::{CompressionEngine, DecompressionEngine},
        file::Header,
    };
    use crate::{StreamId, ValueType, Version};

    #[test]
    fn test_google_compression() {
        let header = Header::new(Version(0), StreamId(0), ValueType::UInteger64);
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
}
