use super::page_cache::{self, FileId, PageCache};
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
        let mut magic = [0x00u8; 4];
        page_cache.read(file_id, 0, &mut magic);

        if magic != MAGIC {
            panic!("Corrupted file - invalid magic for .ty file");
        }

        let mut buffer = [0x00u8; HEADER_SIZE];
        page_cache.read(file_id, 4, &mut buffer);

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

pub struct Cursor {
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

    page_cache: Arc<Mutex<PageCache>>,
}

// TODO: Remove this
impl Iterator for Cursor {
    type Item = (Timestamp, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_vector()
    }
}

impl Cursor {
    // pre: file_paths[0] contains at least one timestamp t such that start <= t
    pub fn new(
        file_paths: Arc<[PathBuf]>,
        start: Timestamp,
        end: Timestamp,
        page_cache: Arc<Mutex<PageCache>>,
    ) -> Result<Self, Error> {
        assert!(file_paths.len() > 0);
        assert!(start <= end);

        let mut page_cache_lock = page_cache.lock().unwrap();
        let file_id = page_cache_lock.register_or_get_file_id(&file_paths[0]);
        let header = Header::parse(file_id, &mut page_cache_lock);
        drop(page_cache_lock);

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
            page_cache,
            last_deltas: (0, 0),
        };

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

    pub fn next_vector(&mut self) -> Option<(Timestamp, Value)> {
        let mut page_cache_lock = self.page_cache.lock().unwrap();

        if self.values_read == self.header.count as u64 {
            self.file_index += 1;

            if self.file_index == self.file_paths.len() {
                return None;
            }

            self.file_id =
                page_cache_lock.register_or_get_file_id(&self.file_paths[self.file_index]);
            self.header = Header::parse(self.file_id, &mut page_cache_lock);
            self.offset = MAGIC_SIZE + HEADER_SIZE;

            self.current_timestamp = self.header.min_timestamp;
            self.value = self.header.first_value;
            self.values_read = 1;
            self.last_deltas = (0, 0);

            // this should never be triggered
            if self.current_timestamp > self.end {
                panic!("Unexpected file change! Cursor timestamp is greater then end timestamp.");
            }

            return Some((self.current_timestamp, self.value));
        }

        if self.values_read % 2 == 1 {
            let mut l_buf = [0u8; 1];
            self.offset += page_cache_lock.read(self.file_id, self.offset, &mut l_buf);
            // self.file.read_exact(&mut l_buf);
            self.cur_length_byte = l_buf[0];
        }

        let int_length = EXPONENTS
            [((self.cur_length_byte >> (6 - 4 * (1 - (self.values_read % 2)))) & 0b11) as usize];
        let mut ts_buf = [0x00; size_of::<u64>()];
        self.offset += page_cache_lock.read(self.file_id, self.offset, &mut ts_buf[0..int_length]);

        let time_delta =
            CompressionEngine::zig_zag_decode(u64::from_le_bytes(ts_buf)) + self.last_deltas.0;
        let new_timestamp = self.current_timestamp.wrapping_add_signed(time_delta);
        if new_timestamp > self.end {
            return None;
        }

        let int_length = EXPONENTS[((self.cur_length_byte
            >> (6 - 4 * (1 - (self.values_read % 2)) - 2))
            & 0b11) as usize];
        let mut v_buf = [0x00u8; size_of::<i64>()];
        self.offset += page_cache_lock.read(self.file_id, self.offset, &mut v_buf[0..int_length]);
        let value_delta =
            CompressionEngine::zig_zag_decode(u64::from_le_bytes(v_buf)) + self.last_deltas.1;
        let new_value = self.value.wrapping_add_signed(value_delta);

        self.last_deltas = (time_delta, value_delta);
        self.current_timestamp = new_timestamp;
        self.value = new_value;
        self.values_read += 1;

        Some((new_timestamp, new_value))
    }

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

        let mut cursor = Cursor::new(
            Arc::new([path]),
            0,
            u64::MAX,
            Arc::new(Mutex::new(page_cache)),
        )
        .unwrap();

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
            last_deltas = curr_deltas.clone()
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

    #[test]
    fn test_write() {
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        model.write("./tmp/cool.ty".into());
    }

    #[test]
    fn test_header_write_parse() {
        let mut temp_file: File = File::create("./tmp/temp_file").unwrap();

        let mut t_header = Header {
            count: 11,
            value_sum: 101,
            ..Header::default()
        };

        t_header.write(&mut temp_file);

        let mut temp_file: File = File::open("./tmp/temp_file").unwrap();
        let mut page_cache = PageCache::new(100);
        let file_id = page_cache.register_or_get_file_id(&"./tmp/temp_file".into());
        let parsed_header = Header::parse(file_id, &mut page_cache);
        assert!(t_header == parsed_header);

        std::fs::remove_file("./tmp/temp_file");
    }

    #[test]
    fn test_cursor() {
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, i + 10);
        }
        model.write("./tmp/test_cursor.ty".into());

        let file_paths = [PathBuf::from_str("./tmp/test_cursor.ty").unwrap()];

        let mut page_cache = PageCache::new(10);
        let cursor = Cursor::new(
            Arc::new(file_paths),
            0,
            100,
            Arc::new(Mutex::new(page_cache)),
        );
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

        std::fs::remove_file("./tmp/test_cursor.ty");
    }

    fn generate_ty_file(path: PathBuf, timestamps: &Vec<Timestamp>, values: &Vec<Value>) {
        assert!(timestamps.len() == values.len());
        let mut model = TimeDataFile::new();

        for i in 0..timestamps.len() {
            model.write_data_to_file_in_mem(timestamps[i], values[i])
        }
        model.write(path);
    }

    #[test]
    fn test_cursor_multiple_files() {
        let file_paths = [
            "./tmp/test_cursor_multiple_files_1.ty",
            "./tmp/test_cursor_multiple_files_2.ty",
            "./tmp/test_cursor_multiple_files_3.ty",
        ];

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in file_paths {
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

        let file_paths = file_paths.map(|path| PathBuf::from_str(path).unwrap());
        let file_paths = Arc::new(file_paths);
        let mut page_cache = PageCache::new(10);
        let cursor = Cursor::new(file_paths.clone(), 0, 100, Arc::new(Mutex::new(page_cache)));
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 0;

        loop {
            let (timestamp, value) = cursor.fetch();
            println!("{} {}", timestamp, value);
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }

        for path in file_paths.iter() {
            std::fs::remove_file(path);
        }
    }

    #[test]
    fn test_cursor_multiple_files_partial() {
        let file_paths = [
            "./tmp/test_cursor_multiple_files_partial_1.ty",
            "./tmp/test_cursor_multiple_files_partial_2.ty",
            "./tmp/test_cursor_multiple_files_partial_3.ty",
        ];

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in file_paths {
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

        let file_paths = file_paths.map(|path| PathBuf::from_str(path).unwrap());
        let file_paths = Arc::new(file_paths);
        let mut page_cache = PageCache::new(10);
        let cursor = Cursor::new(file_paths.clone(), 5, 23, Arc::new(Mutex::new(page_cache)));
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 5;

        loop {
            let (timestamp, value) = cursor.fetch();
            println!("{} {}", timestamp, value);
            assert_eq!(timestamp, timestamps[i]);
            assert_eq!(value, values[i]);
            i += 1;
            if cursor.next().is_none() {
                break;
            }
        }
        assert_eq!(i, 24);

        for path in file_paths.iter() {
            std::fs::remove_file(path);
        }
    }

    #[test]
    fn test_compression() {
        let mut timestamps = Vec::<u64>::new();
        let mut values = Vec::<u64>::new();

        for i in 1..100000u64 {
            timestamps.push(i.into());
            values.push((i * 200000).into());
        }

        generate_ty_file("./tmp/compressed_file.ty".into(), &timestamps, &values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            Arc::new(["./tmp/compressed_file.ty".into()]),
            1,
            100000,
            Arc::new(Mutex::new(page_cache)),
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
        std::fs::remove_file("./tmp/compressed_file.ty");
    }

    #[test]
    fn test_compression_2() {
        let mut timestamps: Vec<u64> = vec![1, 257, 69000, (u32::MAX as u64) + 69000];
        let mut values = vec![1, 257, 69000, (u32::MAX as u64) + 69000];
        generate_ty_file("./tmp/compressed_file_2.ty".into(), &timestamps, &values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            Arc::new(["./tmp/compressed_file_2.ty".into()]),
            1,
            timestamps[timestamps.len() - 1],
            Arc::new(Mutex::new(page_cache)),
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
        std::fs::remove_file("./tmp/compressed_file_2.ty");
    }

    #[test]
    fn test_compression_negative_deltas() {
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
        generate_ty_file(
            "./tmp/compressed_file_neg_deltas.ty".into(),
            &timestamps,
            &values,
        );

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            Arc::new(["./tmp/compressed_file_neg_deltas.ty".into()]),
            1,
            timestamps[timestamps.len() - 1],
            Arc::new(Mutex::new(page_cache)),
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
        std::fs::remove_file("./tmp/compressed_file_neg_deltas.ty");
    }

    #[test]
    fn test_shift_dependent() {
        let mut values_read = 1;
        assert_eq!(6, (6 - 4 * (1 - (values_read % 2))));
        assert_eq!(4, (6 - 4 * (1 - (values_read % 2)) - 2));
        values_read += 1;
        assert_eq!(2, (6 - 4 * (1 - (values_read % 2))));
        assert_eq!(0, (6 - 4 * (1 - (values_read % 2)) - 2));
        values_read += 1;
        assert_eq!(6, (6 - 4 * (1 - (values_read % 2))));
        assert_eq!(4, (6 - 4 * (1 - (values_read % 2)) - 2));
        values_read += 1;
        assert_eq!(2, (6 - 4 * (1 - (values_read % 2))));
        assert_eq!(0, (6 - 4 * (1 - (values_read % 2)) - 2));
    }
}
