use super::compression::DecompressionEngine;
use super::page_cache::{self, FileId, PageCache, SeqPageRead};
use crate::common::{Timestamp, Value};
use crate::storage::compression::{CompressionEngine, CompressionUtils};
use crate::storage::page_cache::page_cache_sequential_read;
use crate::utils::file_utils::FileReaderUtil;
use std::cell::RefCell;
use std::mem;
use std::{
    fs::File,
    io::{Error, Read, Seek, Write},
    mem::size_of,
    path::PathBuf,
    rc::Rc,
};

const MAGIC_SIZE: usize = 4;
const MAGIC: [u8; MAGIC_SIZE] = [b'T', b'a', b'c', b'h'];

const EXPONENTS: [usize; 4] = [1, 2, 4, 8];

pub const MAX_NUM_ENTRIES: usize = 62500;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Header {
    pub version: u16,

    pub stream_id: u64,

    pub min_timestamp: Timestamp,
    pub max_timestamp: Timestamp,

    pub value_sum: Value,
    pub count: u32,
    pub min_value: Value,
    pub max_value: Value,

    pub first_value: Value,
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
            version: FileReaderUtil::read_u64_2(&buffer[0..2]) as u16,
            stream_id: FileReaderUtil::read_u64_8(&buffer[2..10]),
            min_timestamp: FileReaderUtil::read_u64_8(&buffer[10..18]),
            max_timestamp: FileReaderUtil::read_u64_8(&buffer[18..26]),
            value_sum: FileReaderUtil::read_u64_8(&buffer[26..34]),
            count: FileReaderUtil::read_u64_4(&buffer[34..38]) as u32,
            min_value: FileReaderUtil::read_u64_8(&buffer[38..46]),
            max_value: FileReaderUtil::read_u64_8(&buffer[46..54]),
            first_value: FileReaderUtil::read_u64_8(&buffer[54..62]),
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

pub enum ScanHint {
    None,
    Sum,
    Count,
    Min,
    Max,
}

pub struct Cursor {
    file_id: FileId,
    file_index: usize,
    header: Header,
    start: Timestamp,
    end: Timestamp,
    current_timestamp: Timestamp,
    value: Value,
    values_read: u64,

    file_paths: Rc<[PathBuf]>,

    page_cache: Rc<RefCell<PageCache>>,
    decomp_engine: DecompressionEngine<SeqPageRead>,

    scan_hint: ScanHint,
}

// TODO: Remove this
impl Iterator for Cursor {
    type Item = (Timestamp, Value);

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

impl Cursor {
    // pre: file_paths[0] contains at least one timestamp t such that start <= t
    pub fn new(
        file_paths: Rc<[PathBuf]>,
        start: Timestamp,
        end: Timestamp,
        page_cache: Rc<RefCell<PageCache>>,
        scan_hint: ScanHint,
    ) -> Result<Self, Error> {
        assert!(file_paths.len() > 0);
        assert!(start <= end);

        let mut page_cache_ref = page_cache.borrow_mut();

        let file_id = page_cache_ref.register_or_get_file_id(&file_paths[0]);
        let header = Header::parse(file_id, &mut page_cache_ref);

        drop(page_cache_ref);

        let decomp_engine = DecompressionEngine::new(
            page_cache_sequential_read(page_cache.clone(), file_id, MAGIC_SIZE + HEADER_SIZE),
            &header,
        );

        let mut cursor = Self {
            file_id,
            file_index: 0,
            current_timestamp: header.min_timestamp,
            value: header.first_value,
            header,
            start,
            end,
            values_read: 1,
            file_paths,
            page_cache,
            decomp_engine,

            scan_hint,
        };

        // check if we can use hint
        if !matches!(cursor.scan_hint, ScanHint::None)
            && start <= cursor.header.min_timestamp
            && cursor.header.max_timestamp <= end
        {
            cursor.use_query_hint();
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

    // Use the query hint
    fn use_query_hint(&mut self) {
        self.current_timestamp = self.header.max_timestamp;
        self.value = match self.scan_hint {
            ScanHint::Sum => self.header.value_sum as Value,
            ScanHint::Count => self.header.count as Value,
            ScanHint::Min => self.header.min_value,
            ScanHint::Max => self.header.max_value,
            ScanHint::None => unreachable!(),
        };
        self.values_read = self.header.count as u64;
    }

    fn load_next_file(&mut self) -> Option<()> {
        self.file_index += 1;

        if self.file_index == self.file_paths.len() {
            return None;
        }
        self.file_id = self
            .page_cache
            .borrow_mut()
            .register_or_get_file_id(&self.file_paths[self.file_index]);
        self.header = Header::parse(self.file_id, &mut self.page_cache.borrow_mut());

        if self.header.min_timestamp > self.end {
            return None;
        }

        self.current_timestamp = self.header.min_timestamp;
        self.value = self.header.first_value;
        self.values_read = 1;
        self.decomp_engine = DecompressionEngine::new(
            page_cache_sequential_read(
                self.page_cache.clone(),
                self.file_id,
                MAGIC_SIZE + HEADER_SIZE,
            ),
            &self.header,
        );

        // use the query hint if applicable on the next file
        if !matches!(self.scan_hint, ScanHint::None)
            && self.start <= self.header.min_timestamp
            && self.header.max_timestamp <= self.end
        {
            self.use_query_hint();
        }
        Some(())
    }

    #[allow(clippy::should_implement_trait)]
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

        (self.current_timestamp, self.value) = self.decomp_engine.next();
        if self.current_timestamp > self.end {
            return None;
        }
        self.values_read += 1;

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

        let mut cursor = Cursor::new(
            Rc::new([path]),
            0,
            u64::MAX,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
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
        let mut comp_engine = CompressionEngine::new(file, &self.header);

        for i in 1usize..(self.header.count as usize) {
            comp_engine.consume(self.timestamps[i], self.values[i]);
        }

        println!(
            "Original {}, compressed: {}",
            (8 * (self.timestamps.len() + self.values.len())),
            (header_bytes + comp_engine.bytes_compressed()),
        );
        comp_engine.flush_all();
        header_bytes + comp_engine.bytes_compressed()
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

    // Returns the number of entries written in memory
    pub fn write_batch_data_to_file_in_mem(&mut self, batch: &[(Timestamp, Value)]) -> usize {
        let space = MAX_NUM_ENTRIES - self.num_entries();
        let n = usize::min(space, batch.len());

        for pair in batch.iter().take(n) {
            self.write_data_to_file_in_mem(pair.0, pair.1);
        }

        n
    }

    pub fn get_file_name(&self) -> String {
        self.header.max_timestamp.to_string()
    }

    pub fn num_entries(&self) -> usize {
        self.header.count as usize
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
        let cursor = Cursor::new(
            Rc::new([paths[0].clone()]),
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
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
    }

    #[test]
    fn test_single_valued_file() {
        set_up_files!(paths, "1.ty");
        generate_ty_file(paths[0].clone(), &[1], &[2]);

        let mut page_cache = PageCache::new(10);
        page_cache.register_or_get_file_id(&paths[0]);
        let mut cursor = Cursor::new(
            Rc::new([paths[0].clone()]),
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

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

        let file_paths_arc: Rc<[PathBuf]> = file_paths.into();
        let mut page_cache = PageCache::new(10);

        let cursor = Cursor::new(
            file_paths_arc,
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        );
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
        let cursor = Cursor::new(
            file_paths_arc,
            5,
            23,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        );
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
        let mut cursor = Cursor::new(
            paths.into(),
            1,
            100000,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
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
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
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
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
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
    fn test_scan_hints_sum() {
        set_up_files!(file_paths, "1.ty", "2.ty", "3.ty",);

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in &file_paths {
            let mut local_timestamps = Vec::new();
            let mut local_values = Vec::new();
            for i in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push(timestamp + 1);
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let file_paths_arc: Rc<[PathBuf]> = file_paths.into();
        let mut page_cache = Rc::new(RefCell::new(PageCache::new(10)));

        let get_value = |start: Timestamp, end: Timestamp, hint: ScanHint| -> (Value, i32) {
            let mut cursor =
                Cursor::new(file_paths_arc.clone(), start, end, page_cache.clone(), hint).unwrap();
            let mut i = 0;
            let mut res = 0;
            loop {
                let (timestamp, value) = cursor.fetch();
                res += value;
                i += 1;
                if cursor.next().is_none() {
                    break;
                }
            }

            (res, i)
        };

        let (res, i) = get_value(0, 30, ScanHint::Sum);
        assert_eq!(i, 3);
        assert_eq!(res, 465);

        let (res, i) = get_value(5, 28, ScanHint::Sum);
        assert_eq!(res, 420);
        assert_eq!(i, 15);

        let (res, i) = get_value(0, 9, ScanHint::Sum);
        assert_eq!(res, 55);
    }

    #[test]
    fn test_scan_hints_min() {
        set_up_files!(file_paths, "1.ty", "2.ty", "3.ty",);

        let mut timestamp = 0;
        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for file_path in &file_paths {
            let mut local_timestamps = Vec::new();
            let mut local_values = Vec::new();
            for i in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push(timestamp + 1);
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let file_paths_arc: Rc<[PathBuf]> = file_paths.into();
        let mut page_cache = Rc::new(RefCell::new(PageCache::new(10)));

        let get_value = |start: Timestamp, end: Timestamp, hint: ScanHint| -> (Value, i32) {
            let mut cursor =
                Cursor::new(file_paths_arc.clone(), start, end, page_cache.clone(), hint).unwrap();

            let mut i = 0;
            let mut res = Value::MAX;
            loop {
                let (timestamp, value) = cursor.fetch();
                res = res.min(value);
                i += 1;
                if cursor.next().is_none() {
                    break;
                }
            }

            (res, i)
        };

        let (res, i) = get_value(0, 30, ScanHint::Min);
        assert_eq!(res, 1);
        assert_eq!(i, 3);

        let (res, i) = get_value(5, 28, ScanHint::Min);
        assert_eq!(res, 6);
        assert_eq!(i, 15);

        let (res, i) = get_value(2, 9, ScanHint::Min);
        assert_eq!(res, 3);
    }
}
