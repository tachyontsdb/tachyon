use crate::common::{Timestamp, Value};
use crate::storage::compression::CompressionEngine;
use std::{
    fs::File,
    io::{Error, Read, Seek, Write},
    mem::size_of,
    path::PathBuf,
    sync::Arc,
};

const MAGIC: [u8; 4] = [b'T', b'a', b'c', b'h'];

pub struct Cursor {
    file: File,
    file_index: usize,
    header: Header,
    end: Timestamp,
    current_timestamp: Timestamp,
    value: Value,
    values_read: u64,

    file_paths: Arc<[PathBuf]>,
}

impl Cursor {
    // pre: file_paths[0] contains at least one timestamp t such that start <= t
    pub fn new(
        file_paths: Arc<[PathBuf]>,
        start: Timestamp,
        end: Timestamp,
    ) -> Result<Self, Error> {
        assert!(file_paths.len() > 0);
        assert!(start <= end);

        let mut file = File::open(&file_paths[0])?;
        let header = Header::parse(&mut file);

        let mut cursor = Self {
            file,
            file_index: 0,
            current_timestamp: header.min_timestamp,
            value: header.first_value,
            header,
            end,
            values_read: 1,
            file_paths,
        };
        println!("HERE: {}", cursor.value);

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
            if self.file_index == self.file_paths.len() - 1 {
                return None;
            }

            self.file_index += 1;
            self.file = File::open(&self.file_paths[self.file_index]).unwrap();
            self.header = Header::parse(&mut self.file);

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

        let mut ts_buf: [u8; size_of::<Timestamp>()] = [0x00; size_of::<Timestamp>()];
        self.file.read_exact(&mut ts_buf).unwrap();
        let new_timestamp = self.current_timestamp + Timestamp::from_le_bytes(ts_buf);
        if new_timestamp > self.end {
            return None;
        }

        let mut v_buf = [0x00u8; size_of::<Value>()];
        self.file.read_exact(&mut v_buf).unwrap();
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

#[derive(Default, Debug, PartialEq, Eq)]
#[repr(C, packed)]
struct Header {
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

const HEADER_SIZE: usize = size_of::<Header>();

struct FileReaderUtil;

impl FileReaderUtil {
    fn read_u16(buffer: &[u8]) -> u16 {
        u16::from_le_bytes([buffer[0], buffer[1]])
    }

    fn read_u32(buffer: &[u8]) -> u32 {
        u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]])
    }

    fn read_u64(buffer: &[u8]) -> u64 {
        u64::from_le_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ])
    }
}

impl Header {
    fn parse(file: &mut File) -> Self {
        let mut magic = [0x00u8; 4];
        file.read_exact(&mut magic).unwrap();

        if magic != MAGIC {
            panic!("Corrupted file - invalid magic for .ty file (uh oh stinky)");
        }

        let mut buffer = [0x00u8; HEADER_SIZE];
        file.read_exact(&mut buffer).unwrap();

        Self {
            version: FileReaderUtil::read_u16(&buffer[0..2]),
            stream_id: FileReaderUtil::read_u64(&buffer[2..10]),
            min_timestamp: FileReaderUtil::read_u64(&buffer[10..18]),
            max_timestamp: FileReaderUtil::read_u64(&buffer[18..26]),
            value_sum: FileReaderUtil::read_u64(&buffer[26..34]),
            count: FileReaderUtil::read_u32(&buffer[34..38]),
            min_value: FileReaderUtil::read_u64(&buffer[38..46]),
            max_value: FileReaderUtil::read_u64(&buffer[46..54]),
            first_value: FileReaderUtil::read_u64(&buffer[54..62]),
        }
    }

    fn write(&self, file: &mut File) -> Result<(), std::io::Error> {
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

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct TimeDataFile {
    header: Header,
    timestamps: Vec<Timestamp>,
    values: Vec<Value>,
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
        let mut file = File::open(path).unwrap();

        let header = Header::parse(&mut file);

        let mut buf = [0x00u8; size_of::<Timestamp>()];
        let mut timestamps = Vec::<Timestamp>::new();
        for _ in 0..header.count {
            file.read_exact(&mut buf).unwrap();
            timestamps.push(Timestamp::from_le_bytes(buf));
        }

        let mut buf = [0x00u8; size_of::<Value>()];
        let mut values = Vec::<Value>::new();
        for _ in 0..header.count {
            file.read_exact(&mut buf).unwrap();
            values.push(Value::from_le_bytes(buf));
        }

        Self {
            header,
            timestamps,
            values,
        }
    }

    pub fn write(&self, path: PathBuf) {
        let mut file = File::create(path).unwrap();

        self.header.write(&mut file).unwrap();

        let mut body = Vec::<u64>::new();
        // write timestamps & values deltas
        for i in 1usize..(self.header.count as usize) {
            body.push(self.timestamps[i] - self.timestamps[i - 1]);
            body.push(self.values[i] - self.values[i - 1]);
            // file.write_all(&(self.timestamps[i] - self.timestamps[i - 1]).to_le_bytes())
            //     .unwrap();

            // file.write_all(&(self.values[i] - self.values[i - 1]).to_le_bytes())
            //     .unwrap();
        }
        let body_compressed = CompressionEngine::compress(&body);
        println!(
            "Original {}, compressed: {}",
            (8 * body.len()),
            (body_compressed.len()),
        );
        file.write_all(&body_compressed).unwrap();
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
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_write() {
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i.into(), i + 10);
        }
        model.write("./tmp/cool.ty".into());
    }

    // #[test]
    // fn test_read() {
    //     let model = TimeDataFile::read_data_file("./tmp/cool.ty".into());
    //     assert_eq!(model.header.count, 10);
    //     assert_eq!(model.timestamps[0], 0);
    //     assert_eq!(model.values[0], 10);
    //     println!("FILE: {:#?}", model);
    // }

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
        let parsed_header = Header::parse(&mut temp_file);
        assert!(t_header == parsed_header);

        std::fs::remove_file("./tmp/temp_file");
    }

    #[test]
    fn test_cursor() {
        let mut model = TimeDataFile::new();
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i.into(), i + 10);
        }
        model.write("./tmp/test_cursor.ty".into());

        let file_paths = [PathBuf::from_str("./tmp/test_cursor.ty").unwrap()];
        let cursor = Cursor::new(Arc::new(file_paths), 0, 100);
        assert!(!cursor.is_err());

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
        model.write(path)
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
        let cursor = Cursor::new(file_paths.clone(), 0, 100);
        assert!(!cursor.is_err());

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
            std::fs::remove_file(&path);
        }
    }

    #[test]
    fn test_cursor_multiple_files_partial() {
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
        let cursor = Cursor::new(file_paths.clone(), 5, 23);
        assert!(!cursor.is_err());

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
            std::fs::remove_file(&path);
        }
    }

    #[test]
    fn test_compression() {
        let mut timestamps = Vec::<u64>::new();
        let mut values = Vec::<u64>::new();

        for i in 0..10u64 {
            timestamps.push(i.into());
            values.push((i + 10).into());
        }

        generate_ty_file("./tmp/compressed_file.ty".into(), &timestamps, &values);
    }
}
