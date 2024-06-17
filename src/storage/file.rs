use super::page_cache::{self, FileId, PageCache, SeqPageRead};
use crate::common::{Timestamp, Value};
use crate::storage::compression::CompressionEngine;
use std::{
    fs::File,
    io::{Error, Read, Seek, Write},
    mem::size_of,
    path::PathBuf,
    sync::{Arc, Mutex},
};

const MAGIC_SIZE: usize = 4;
const MAGIC: [u8; MAGIC_SIZE] = [b'T', b'a', b'c', b'h'];

const EXPONENTS: [usize; 4] = [1, 2, 4, 8];

struct FileReaderUtil;
const VAR_U64_READERS: [fn(&[u8]) -> u64; 4] = [
    FileReaderUtil::read_u64_1,
    FileReaderUtil::read_u64_2,
    FileReaderUtil::read_u64_4,
    FileReaderUtil::read_u64_8,
];

// TODO: Check this, changed
impl FileReaderUtil {
    fn read_u16(buffer: [u8; size_of::<u16>()]) -> u16 {
        u16::from_le_bytes(buffer)
    }

    fn read_u32(buffer: [u8; size_of::<u32>()]) -> u32 {
        u32::from_le_bytes(buffer)
    }

    fn read_u64(buffer: [u8; size_of::<u64>()]) -> u64 {
        u64::from_le_bytes(buffer)
    }

    // Varint decoding
    #[inline]
    fn read_u64_1(buf: &[u8]) -> u64 {
        buf[0] as u64
    }

    #[inline]
    fn read_u64_2(buf: &[u8]) -> u64 {
        ((buf[1] as u64) << 8) | (buf[0] as u64)
    }
    #[inline]
    fn read_u64_4(buf: &[u8]) -> u64 {
        ((buf[3] as u64) << 24) | ((buf[2] as u64) << 16) | ((buf[1] as u64) << 8) | (buf[0] as u64)
    }

    #[inline]
    fn read_u64_8(buf: &[u8]) -> u64 {
        let mut res = 0u64;
        for (i, byte) in buf.iter().enumerate().take(8) {
            res |= (*byte as u64) << (i * 8);
        }
        res
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Header {
    version: u16,

    stream_id: u64,

    min_timestamp: Timestamp,
    max_timestamp: Timestamp,

    value_sum: Value,
    count: u32,
    min_value: Value,
    max_value: Value,

    first_value: Value,
}

const HEADER_SIZE: usize = 62;

impl Header {
    fn parse(file_id: FileId, page_cache: &mut PageCache) -> Self {
        let mut buffer = [0x00u8; MAGIC_SIZE + HEADER_SIZE];
        page_cache.read(file_id, 0, &mut buffer);
        if buffer[0..MAGIC_SIZE] != MAGIC {
            panic!("Corrupted file - invalid magic for .ty file");
        }
        let buffer = &mut buffer[MAGIC_SIZE..];

        Self {
            version: FileReaderUtil::read_u16(buffer[0..2].try_into().unwrap()),
            stream_id: FileReaderUtil::read_u64(buffer[2..10].try_into().unwrap()),
            min_timestamp: FileReaderUtil::read_u64(buffer[10..18].try_into().unwrap()),
            max_timestamp: FileReaderUtil::read_u64(buffer[18..26].try_into().unwrap()),
            value_sum: FileReaderUtil::read_u64(buffer[26..34].try_into().unwrap()),
            count: FileReaderUtil::read_u32(buffer[34..38].try_into().unwrap()),
            min_value: FileReaderUtil::read_u64(buffer[38..46].try_into().unwrap()),
            max_value: FileReaderUtil::read_u64(buffer[46..54].try_into().unwrap()),
            first_value: FileReaderUtil::read_u64(buffer[54..62].try_into().unwrap()),
        }
    }

    fn write(&self, file: &mut File) -> Result<usize, Error> {
        file.write_all(&MAGIC)?;

        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.stream_id.to_le_bytes())?;

        file.write_all(&self.min_timestamp.to_le_bytes())?;
        file.write_all(&self.max_timestamp.to_le_bytes())?;

        file.write_all(&self.value_sum.to_le_bytes())?;
        file.write_all(&self.count.to_le_bytes())?;
        file.write_all(&self.min_value.to_le_bytes())?;
        file.write_all(&self.max_value.to_le_bytes())?;

        file.write_all(&self.first_value.to_le_bytes())?;

        Ok(HEADER_SIZE + MAGIC_SIZE)
    }
}

pub struct Cursor<'a> {
    file_id: FileId,
    file_index: usize,
    header: Header,
    end: Timestamp,
    current_timestamp: Timestamp,
    value: Value,
    values_read: u64,
    offset: usize,
    last_deltas: (i64, i64),

    // length byte
    cur_length_byte: u8,
    file_paths: Arc<[PathBuf]>,

    seq_reader: SeqPageRead<'a>,

    next_timestamp: Timestamp,
    next_value: Value,
}

// TODO: Remove this
impl<'a> Iterator for Cursor<'a> {
    type Item = (Timestamp, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

impl<'a> Cursor<'a> {
    // pre: file_paths[0] contains at least one timestamp t such that start <= t
    pub fn new(
        file_paths: Arc<[PathBuf]>,
        start: Timestamp,
        end: Timestamp,
        page_cache: &'a mut PageCache,
    ) -> Result<Self, Error> {
        assert!(file_paths.len() > 0);
        assert!(start <= end);

        let file_id = page_cache.register_or_get_file_id(&file_paths[0]);
        let header = Header::parse(file_id, page_cache);

        let mut cursor = Self {
            file_id,
            file_index: 0,
            current_timestamp: header.min_timestamp,
            value: header.first_value,
            header,
            end,
            values_read: 1,
            offset: MAGIC_SIZE + HEADER_SIZE,
            cur_length_byte: 0,
            file_paths,
            last_deltas: (0, 0),
            seq_reader: page_cache.sequential_read(file_id, MAGIC_SIZE + HEADER_SIZE),

            next_timestamp: 0,
            next_value: 0,
        };

        if cursor.header.count > 1 {
            let mut l_buf = [0u8; 1];
            cursor.offset += cursor.seq_reader.read(&mut l_buf);
            cursor.cur_length_byte = l_buf[0];
        }

        while cursor.current_timestamp < start {
            if let Some((timestamp, value)) = cursor.next() {
                cursor.current_timestamp = timestamp;
                cursor.value = value;
            } else {
                panic!("Unexpected end of file! File does not contain start timestamp.");
            }
        }

        Ok(cursor)
    }

    fn load_next_file(&mut self) -> Option<()> {
        self.file_index += 1;

        if self.file_index == self.file_paths.len() {
            return None;
        }
        self.file_id = self
            .seq_reader
            .page_cache
            .register_or_get_file_id(&self.file_paths[self.file_index]);
        self.header = Header::parse(self.file_id, self.seq_reader.page_cache);
        self.offset = MAGIC_SIZE + HEADER_SIZE;

        self.current_timestamp = self.header.min_timestamp;
        self.value = self.header.first_value;
        self.values_read = 1;
        self.last_deltas = (0, 0);

        self.seq_reader.reset(self.file_id, self.offset);

        if self.header.count > 1 {
            let mut l_buf = [0u8; 1];
            self.offset += self.seq_reader.read(&mut l_buf);
            self.cur_length_byte = l_buf[0];
        }
        Some(())
    }

    pub fn next(&mut self) -> Option<(Timestamp, Value)> {
        if self.current_timestamp > self.end {
            return None;
        }

        if self.values_read == self.header.count as u64 {
            self.load_next_file()?;

            // this should never be triggered
            if self.current_timestamp > self.end {
                panic!("Unexpected file change! Cursor timestamp is greater then end timestamp.");
            }

            return Some((self.current_timestamp, self.value));
        }

        if self.values_read % 2 == 0 {
            self.current_timestamp = self.next_timestamp;
            self.value = self.next_value;
            if self.current_timestamp > self.end {
                return None;
            }

            self.values_read += 1;
            return Some((self.current_timestamp, self.value));
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
        self.offset += self
            .seq_reader
            .read(&mut buffer[0..total_varint_lengths + 1]);

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

        if self.current_timestamp > self.end {
            return None;
        }

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

        Some((self.current_timestamp, self.value))
    }

    // not valid after next returns none
    pub fn fetch(&self) -> (Timestamp, Value) {
        (self.current_timestamp, self.value)
    }
}

#[derive(Debug, Default)]
pub struct TimeDataFile {
    pub header: Header,
    pub timestamps: Vec<Timestamp>,
    pub values: Vec<Value>,
}

impl TimeDataFile {
    pub fn new() -> Self {
        Self {
            header: Header::default(),
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn read_data_file(path: PathBuf) -> Self {
        let mut page_cache = PageCache::new(100);

        let mut cursor = Cursor::new(Arc::new([path]), 0, u64::MAX, &mut page_cache).unwrap();

        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        loop {
            let (timestamp, value) = cursor.fetch();
            timestamps.push(timestamp);
            values.push(value);

            if (cursor.next().is_none()) {
                break;
            }
        }

        Self {
            header: cursor.header,
            timestamps,
            values,
        }
    }

    pub fn write(&self, path: PathBuf) -> usize {
        let mut file = File::create(path).unwrap();

        let header_bytes = self.header.write(&mut file).unwrap();

        let mut body = Vec::<u64>::new();

        let mut last_deltas: (i64, i64) = (0, 0);
        for i in 1usize..(self.header.count as usize) {
            // Assumption: difference will never exceed i64
            let curr_deltas = (
                (self.timestamps[i].wrapping_sub(self.timestamps[i - 1])) as i64,
                (self.values[i].wrapping_sub(self.values[i - 1])) as i64,
            );

            body.push(CompressionEngine::zig_zag_encode(
                curr_deltas.0 - last_deltas.0,
            ));
            body.push(CompressionEngine::zig_zag_encode(
                curr_deltas.1 - last_deltas.1,
            ));
            last_deltas = curr_deltas
        }
        let body_compressed = CompressionEngine::compress(&body);
        println!(
            "Original {}, compressed: {}",
            (8 * body.len()),
            (body_compressed.len()),
        );
        file.write_all(&body_compressed).unwrap();
        body_compressed.len() + header_bytes
    }

    pub fn write_data_to_file_in_mem(&mut self, timestamp: Timestamp, value: Value) {
        if self.header.count == 0 {
            self.header.first_value = value;
            self.header.min_timestamp = timestamp;
            self.header.max_timestamp = timestamp;
            self.header.min_value = value;
            self.header.max_value = value;
        }

        self.header.count += 1;
        self.header.value_sum += Value::from(value);

        self.header.max_timestamp = Timestamp::max(self.header.max_timestamp, timestamp);
        self.header.min_timestamp = Timestamp::min(self.header.min_timestamp, timestamp);

        self.header.max_value = Value::max(self.header.max_value, value);
        self.header.min_value = Value::min(self.header.min_value, value);

        self.timestamps.push(timestamp);
        self.values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::utils::test_utils::*;

    #[test]
    fn test_write() {
        set_up_files!(paths, "cool.ty");
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        model.write(paths[0].clone());
    }

    #[test]
    fn test_header_write_parse() {
        set_up_files!(paths, "temp_file.ty");
        let mut temp_file: File = File::create(&paths[0]).unwrap();

        let mut t_header = Header {
            count: 11,
            value_sum: 101,
            ..Header::default()
        };

        t_header.write(&mut temp_file);

        let mut temp_file: File = File::open(&paths[0]).unwrap();
        let mut page_cache = PageCache::new(100);
        let file_id = page_cache.register_or_get_file_id(&paths[0]);
        let parsed_header = Header::parse(file_id, &mut page_cache);
        assert!(t_header == parsed_header);
    }

    #[test]
    fn test_cursor() {
        set_up_files!(paths, "1.ty");
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        model.write(paths[0].clone());

        let mut page_cache = PageCache::new(10);
        let cursor = Cursor::new(Arc::new([paths[0].clone()]), 0, 100, &mut page_cache);
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 0;
        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, model.timestamps[i]);
            assert_eq!(value, model.values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
    }

    #[test]
    fn test_single_valued_file() {
        set_up_files!(paths, "1.ty");
        generate_ty_file(paths[0].clone(), &[1], &[2]);

        let mut page_cache = PageCache::new(10);
        page_cache.register_or_get_file_id(&paths[0]);
        let mut cursor =
            Cursor::new(Arc::new([paths[0].clone()]), 0, 100, &mut page_cache).unwrap();

        let mut i = 0;
        loop {
            let (timestamp, value) = cursor.fetch();
            println!("{} {}", timestamp, value);
            assert_eq!(timestamp, 1);
            assert_eq!(value, 2);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
    }

    #[test]
    fn test_cursor_multiple_files() {
        set_up_files!(file_paths, "1.ty", "2.ty", "3.ty",);

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in &file_paths {
            let mut local_timestamps = Vec::new();
            let mut local_values = Vec::new();
            for i in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push(timestamp + 10);
                timestamp += 1;
            }

            generate_ty_file(file_path.clone(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let file_paths_arc: Arc<[PathBuf]> = file_paths.into();
        let mut page_cache = PageCache::new(10);

        let cursor = Cursor::new(file_paths_arc, 0, 100, &mut page_cache);
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 0;

        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
    }

    #[test]
    fn test_cursor_multiple_files_partial() {
        set_up_files!(file_paths, "1.ty", "2.ty", "3.ty",);

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in &file_paths {
            let mut local_timestamps = Vec::new();
            let mut local_values = Vec::new();
            for i in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push(timestamp + 10);
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let file_paths_arc = file_paths.into();
        let mut page_cache = PageCache::new(10);
        let cursor = Cursor::new(file_paths_arc, 5, 23, &mut page_cache);
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 5;

        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
        assert_eq!(i, 24);
    }

    #[test]
    fn test_compression() {
        set_up_files!(paths, "1.ty");
        let mut timestamps = Vec::<u64>::new();
        let mut values = Vec::<u64>::new();

        for i in 1..100000u64 {
            timestamps.push(i);
            values.push(i * 200000);
        }

        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(paths.into(), 1, 100000, &mut page_cache).unwrap();

        let mut i = 0;
        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
    }

    #[test]
    fn test_compression_2() {
        set_up_files!(paths, "1.ty");
        let mut timestamps: Vec<u64> = vec![1, 257, 69000, (u32::MAX as u64) + 69000];
        let mut values = vec![1, 257, 69000, (u32::MAX as u64) + 69000];
        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            paths.into(),
            1,
            timestamps[timestamps.len() - 1],
            &mut page_cache,
        )
        .unwrap();

        let mut i = 0;
        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
        assert_eq!(i, timestamps.len());
    }

    #[test]
    fn test_compression_negative_deltas() {
        set_up_files!(paths, "1.ty");

        let mut timestamps: Vec<u64> = vec![
            1,
            25,
            27,
            35,
            (u32::MAX as u64),
            (u32::MAX as u64) + 69000,
            (u32::MAX as u64) + 69001,
        ];
        let mut values = vec![100, 3, 23, 0, 100, (u32::MAX as u64), 1];
        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            paths.into(),
            1,
            timestamps[timestamps.len() - 1],
            &mut page_cache,
        )
        .unwrap();

        let mut i = 0;
        loop {
            let (timestamp, value) = cursor.fetch();
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
        assert_eq!(i, timestamps.len());
    }
}
