use super::page_cache::{self, PageCache};
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
    fn parse(file_id: usize, page_cache: &mut PageCache) -> Self {
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

pub struct Cursor<'a> {
    file_id: usize,
    file_index: usize,
    header: Header,
    end: Timestamp,
    current_timestamp: Timestamp,
    value: Value,
    values_read: u64,
    offset: usize,

    // length byte
    cur_length_byte: u8,
    file_paths: Arc<[PathBuf]>,

    page_cache: &'a mut PageCache,
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
            page_cache,
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

    pub fn next(&mut self) -> Option<(Timestamp, Value)> {
        if self.values_read == self.header.count as u64 {
            self.file_index += 1;

            if self.file_index == self.file_paths.len() {
                return None;
            }

            self.file_id = self
                .page_cache
                .register_or_get_file_id(&self.file_paths[self.file_index]);
            self.header = Header::parse(self.file_id, self.page_cache);
            self.offset = MAGIC_SIZE + HEADER_SIZE;

            println!("New file: {:#?}", self.header);

            self.current_timestamp = self.header.min_timestamp;
            self.value = self.header.first_value;
            self.values_read = 1;

            // this should never be triggered
            if self.current_timestamp > self.end {
                panic!("Unexpected file change! Cursor timestamp is greater then end timestamp.");
            }

            return Some((self.current_timestamp, self.value));
        }

        if self.values_read % 2 == 1 {
            let mut l_buf = [0u8; 1];
            self.offset += self.page_cache.read(self.file_id, self.offset, &mut l_buf);
            // self.file.read_exact(&mut l_buf);
            self.cur_length_byte = l_buf[0];
        }

        let int_length = EXPONENTS
            [(self.cur_length_byte >> (6 - 4 * (1 - self.values_read % 2)) & 0b11) as usize];
        let mut ts_buf = [0x00; size_of::<Timestamp>()];
        self.offset += self
            .page_cache
            .read(self.file_id, self.offset, &mut ts_buf[0..int_length]);
        let new_timestamp = self.current_timestamp + Timestamp::from_le_bytes(ts_buf);
        if new_timestamp > self.end {
            return None;
        }

        let int_length = EXPONENTS
            [(self.cur_length_byte >> (6 - 4 * (1 - self.values_read % 2) - 2) & 0b11) as usize];
        let mut v_buf = [0x00u8; size_of::<Value>()];
        self.offset += self
            .page_cache
            .read(self.file_id, self.offset, &mut v_buf[0..int_length]);
        let new_value = self.value + Value::from_le_bytes(v_buf);

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
        // TODO: remove unwraps
        let mut page_cache = PageCache::new(100);
        let file_id = page_cache.register_or_get_file_id(&path);
        let header = Header::parse(file_id, &mut page_cache);
        let mut offset = MAGIC_SIZE + HEADER_SIZE;

        let mut length = [0u8; 1];

        let mut int_val = [0u8; 8];

        let mut i = 0;

        let mut timestamp = header.min_timestamp;
        let mut value = header.first_value;

        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        timestamps.push(timestamp);
        values.push(value);

        while (i < (header.count - 1) / 2 * 4) {
            offset += page_cache.read(file_id, offset, &mut length);
            for j in 0..4 {
                let int_length = EXPONENTS[((length[0] >> (6 - (j * 2))) & 0b11) as usize];
                offset += page_cache.read(file_id, offset, &mut int_val[0..int_length]);

                let mut butter = [0u8; 8];
                for q in 0..int_length {
                    butter[q] = int_val[q];
                }

                if j % 2 == 0 {
                    timestamp += u64::from_le_bytes(butter);
                    timestamps.push(timestamp);
                } else {
                    value += u64::from_le_bytes(butter);
                    values.push(value);
                }
            }
            i += 4;
        }

        if header.count % 2 == 0 {
            offset += page_cache.read(file_id, offset, &mut length);
            for j in 0..2 {
                let int_length = EXPONENTS[((length[0] >> (6 - (j * 2))) & 0b11) as usize];
                offset += page_cache.read(file_id, offset, &mut int_val[0..int_length]);

                let mut butter = [0u8; 8];
                for q in 0..int_length {
                    butter[q] = int_val[q];
                }

                if j % 2 == 0 {
                    timestamp += u64::from_le_bytes(butter);
                    timestamps.push(timestamp);
                } else {
                    value += u64::from_le_bytes(butter);
                    values.push(value);
                }
            }
            i += 2;
        }

        Self {
            header,
            timestamps,
            values,
        }
    }

    pub fn write(&self, path: PathBuf) -> usize {
        let mut file = File::create(path).unwrap();

        let header_bytes = self.header.write(&mut file).unwrap();

        let mut body = Vec::<u64>::new();
        // write timestamps & values deltas
        for i in 1usize..(self.header.count as usize) {
            body.push(self.timestamps[i] - self.timestamps[i - 1]);
            body.push(self.values[i] - self.values[i - 1]);
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
        let cursor = Cursor::new(Arc::new(file_paths), 0, 100, &mut page_cache);
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
        let cursor = Cursor::new(file_paths.clone(), 0, 100, &mut page_cache);
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
        let cursor = Cursor::new(file_paths.clone(), 5, 23, &mut page_cache);
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
            timestamps.push(i);
            values.push((i * 200000));
        }

        generate_ty_file("./tmp/compressed_file.ty".into(), &timestamps, &values);

        let res = TimeDataFile::read_data_file("./tmp/compressed_file.ty".into());

        assert_eq!(res.timestamps.len(), timestamps.len());

        assert!(res.timestamps == timestamps);
        assert!(res.values == values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            Arc::new(["./tmp/compressed_file.ty".into()]),
            1,
            100000,
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

        std::fs::remove_file("./tmp/compressed_file.ty");
    }

    #[test]
    fn test_compression_2() {
        let mut timestamps = Vec::<u64>::new();
        let mut values = Vec::<u64>::new();

        for i in 1..100000u64 {
            timestamps.push(i);
            values.push((i * 200000));
        }

        generate_ty_file("./tmp/compressed_file.ty".into(), &timestamps, &values);

        let res = TimeDataFile::read_data_file("./tmp/compressed_file.ty".into());

        assert_eq!(res.timestamps.len(), timestamps.len());

        assert!(res.timestamps == timestamps);
        assert!(res.values == values);

        let mut page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            Arc::new(["./tmp/compressed_file.ty".into()]),
            1,
            100000,
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
        std::fs::remove_file("./tmp/compressed_file.ty");
    }
}
