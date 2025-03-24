use super::compression::int::{IntCompressor, IntDecompressor};
use super::compression::CompressionEngine;
use super::page_cache::{FileId, PageCache, SeqPageRead};
use super::{FileReaderUtils, MAX_NUM_ENTRIES};
use crate::error::WriterErr;
use crate::storage::compression::DecompressionEngine;
use crate::storage::page_cache::page_cache_sequential_read;
use crate::{StreamId, Timestamp, Value, ValueType, Vector, Version};
use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::path::PathBuf;
use std::rc::Rc;

const MAGIC_SIZE: usize = 4;
const MAGIC: [u8; MAGIC_SIZE] = [b'T', b'a', b'c', b'h'];

const HEADER_SIZE: usize = 71;

pub struct FileLockGuard {
    fd: RawFd,
    released: bool,
}

impl FileLockGuard {
    pub fn new_shared(fd: &File) -> Result<Self, io::Error> {
        rustix::fs::flock(fd.as_fd(), rustix::fs::FlockOperation::LockShared)?;
        Ok(Self {
            fd: fd.as_raw_fd(),
            released: false,
        })
    }

    pub fn new_exclusive(fd: &File) -> Result<Self, io::Error> {
        rustix::fs::flock(fd.as_fd(), rustix::fs::FlockOperation::LockExclusive)?;
        Ok(Self {
            fd: fd.as_raw_fd(),
            released: false,
        })
    }

    pub fn non_blocking_new_shared(fd: &File) -> Result<Self, io::Error> {
        rustix::fs::flock(
            fd.as_fd(),
            rustix::fs::FlockOperation::NonBlockingLockShared,
        )?;
        Ok(Self {
            fd: fd.as_raw_fd(),
            released: false,
        })
    }

    pub fn non_blocking_new_exclusive(fd: &File) -> Result<Self, io::Error> {
        rustix::fs::flock(
            fd.as_fd(),
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        )?;
        Ok(Self {
            fd: fd.as_raw_fd(),
            released: false,
        })
    }

    pub fn release(&mut self) -> Result<(), io::Error> {
        if self.released {
            return Ok(());
        }
        rustix::fs::flock(
            unsafe { BorrowedFd::borrow_raw(self.fd) },
            rustix::fs::FlockOperation::Unlock,
        )?;
        self.released = true;

        Ok(())
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

#[derive(Clone)]
pub struct Header {
    pub version: Version,
    pub stream_id: StreamId,

    pub min_timestamp: Timestamp,
    pub max_timestamp: Timestamp,

    pub count: u32,
    pub value_type: ValueType,

    pub value_sum: Value,
    pub min_value: Value,
    pub max_value: Value,

    pub first_value: Value,
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.stream_id == other.stream_id
            && self.min_timestamp == other.min_timestamp
            && self.max_timestamp == other.max_timestamp
            && self.count == other.count
            && self.value_type == other.value_type
            && self.value_sum.eq_same(self.value_type, &other.value_sum)
            && self.min_value.eq_same(self.value_type, &other.min_value)
            && self.max_value.eq_same(self.value_type, &other.max_value)
            && self
                .first_value
                .eq_same(self.value_type, &other.first_value)
    }
}

impl Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Header")
            .field("version", &self.version)
            .field("stream_id", &self.stream_id)
            .field("min_timestamp", &self.min_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("count", &self.count)
            .field("value_type", &self.value_type)
            .field("value_sum", &self.value_sum.get_output(self.value_type))
            .field("min_value", &self.min_value.get_output(self.value_type))
            .field("max_value", &self.max_value.get_output(self.value_type))
            .field("first_value", &self.first_value.get_output(self.value_type))
            .finish()
    }
}

impl Header {
    pub fn new(version: Version, stream_id: StreamId, value_type: ValueType) -> Self {
        Self {
            version,
            stream_id,

            min_timestamp: Timestamp::default(),
            max_timestamp: Timestamp::default(),

            count: 0,
            value_type,

            value_sum: Value::get_default(value_type),
            min_value: Value::get_default(value_type),
            max_value: Value::get_default(value_type),

            first_value: Value::get_default(value_type),
        }
    }

    fn parse_value(value_type: ValueType, buf: &[u8]) -> Value {
        match value_type {
            ValueType::Integer64 => Value {
                integer64: FileReaderUtils::read_i64_8(buf),
            },
            ValueType::UInteger64 => Value {
                uinteger64: FileReaderUtils::read_u64_8(buf),
            },
            ValueType::Float64 => Value {
                float64: FileReaderUtils::read_f64_8(buf),
            },
        }
    }

    fn parse(file_id: FileId, page_cache: &mut PageCache) -> Self {
        let mut buffer = [0x00u8; MAGIC_SIZE + HEADER_SIZE];
        page_cache.read(file_id, 0, &mut buffer);
        if buffer[0..MAGIC_SIZE] != MAGIC {
            panic!("Corrupted file - invalid magic for .ty file!");
        }
        let buffer = &buffer[MAGIC_SIZE..];

        let value_type = (FileReaderUtils::read_u64_1(&buffer[38..39]) as u8)
            .try_into()
            .unwrap();
        Self {
            version: Version(
                FileReaderUtils::read_u64_2(&buffer[0..2])
                    .try_into()
                    .unwrap(),
            ),
            stream_id: StreamId(FileReaderUtils::read_u128_16(&buffer[2..18])),
            min_timestamp: FileReaderUtils::read_u64_8(&buffer[18..26]),
            max_timestamp: FileReaderUtils::read_u64_8(&buffer[26..34]),
            count: FileReaderUtils::read_u64_4(&buffer[34..38])
                .try_into()
                .unwrap(),
            value_type,
            value_sum: Self::parse_value(value_type, &buffer[39..47]),
            min_value: Self::parse_value(value_type, &buffer[47..55]),
            max_value: Self::parse_value(value_type, &buffer[55..63]),
            first_value: Self::parse_value(value_type, &buffer[63..71]),
        }
    }

    fn write_value_to_buffer(&self, buffer: &mut [u8], offset: usize, value: Value) -> usize {
        let bytes = match self.value_type {
            ValueType::Integer64 => value.get_integer64().to_le_bytes(),
            ValueType::UInteger64 => value.get_uinteger64().to_le_bytes(),
            ValueType::Float64 => value.get_float64().to_le_bytes(),
        };
        buffer[offset..offset + 8].copy_from_slice(&bytes);
        8
    }

    fn write_from_path(&self, path: &PathBuf) -> Result<usize, io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true) // may be the case that this file already exists due to previous panic!
            .write(true)
            .open(path)?;

        self.write(&mut file)
    }

    fn as_bytes(&self) -> [u8; HEADER_SIZE + MAGIC_SIZE] {
        let mut buffer = [0u8; HEADER_SIZE + MAGIC_SIZE];
        let mut offset = 0;

        buffer[offset..offset + MAGIC_SIZE].copy_from_slice(&MAGIC);
        offset += MAGIC_SIZE;

        let version_bytes = self.version.0.to_le_bytes();
        buffer[offset..offset + 2].copy_from_slice(&version_bytes);
        offset += 2;

        let stream_id_bytes = self.stream_id.0.to_le_bytes();
        buffer[offset..offset + 16].copy_from_slice(&stream_id_bytes);
        offset += 16;

        let min_ts_bytes = self.min_timestamp.to_le_bytes();
        buffer[offset..offset + 8].copy_from_slice(&min_ts_bytes);
        offset += 8;

        let max_ts_bytes = self.max_timestamp.to_le_bytes();
        buffer[offset..offset + 8].copy_from_slice(&max_ts_bytes);
        offset += 8;

        let count_bytes = self.count.to_le_bytes();
        buffer[offset..offset + 4].copy_from_slice(&count_bytes);
        offset += 4;

        buffer[offset] = self.value_type as u8;
        offset += 1;

        offset += self.write_value_to_buffer(&mut buffer, offset, self.value_sum);
        offset += self.write_value_to_buffer(&mut buffer, offset, self.min_value);
        offset += self.write_value_to_buffer(&mut buffer, offset, self.max_value);
        offset += self.write_value_to_buffer(&mut buffer, offset, self.first_value);

        debug_assert_eq!(
            offset,
            HEADER_SIZE + MAGIC_SIZE,
            "Header buffer size mismatch"
        );
        buffer
    }

    fn write(&self, file: &mut File) -> Result<usize, io::Error> {
        let buffer = self.as_bytes();

        file.write_all(&buffer)?;

        Ok(HEADER_SIZE + MAGIC_SIZE)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ScanHint {
    None,
    Sum,
    Count,
    Min,
    Max,
}

struct CursorImpl {
    file_id: FileId,
    file_index: usize,
    header: Header,
    start: Timestamp,
    end: Timestamp,
    current_timestamp: Timestamp,
    value: Value,
    values_read: u64,

    file_paths: Vec<PathBuf>,

    page_cache: Rc<RefCell<PageCache>>,
    decomp_engine: IntDecompressor<SeqPageRead>,

    scan_hint: ScanHint,

    is_done: bool,

    file_lock: FileLockGuard,
}

impl CursorImpl {
    /// Precondition: file_paths\[0] contains at least one timestamp t such that start <= t
    pub fn new(
        file_paths: Vec<PathBuf>,
        start: Timestamp,
        end: Timestamp,
        page_cache: Rc<RefCell<PageCache>>,
        scan_hint: ScanHint,
    ) -> Result<Self, io::Error> {
        assert!(!file_paths.is_empty());
        assert!(start <= end);

        let mut page_cache_ref = page_cache.borrow_mut();

        let file_id = page_cache_ref.register_or_get_file_id(&file_paths[0]);
        let header = Header::parse(file_id, &mut page_cache_ref);

        let file_lock = FileLockGuard::new_shared(page_cache_ref.get_file(file_id))?;
        drop(page_cache_ref);

        let decomp_engine = IntDecompressor::new(
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

            is_done: false,
            file_lock,
        };

        cursor.use_query_hint_for_value(cursor.value);

        // Check if we can use hint
        if cursor.scan_hint != ScanHint::None
            && start <= cursor.header.min_timestamp
            && cursor.header.max_timestamp <= end
        {
            cursor.use_query_hint();
        }

        while cursor.current_timestamp < start {
            if cursor.next_vector().is_none() {
                panic!("Unexpected end of file! File does not contain start timestamp!");
            }
        }

        Ok(cursor)
    }

    // Use the query hint
    fn use_query_hint(&mut self) {
        self.current_timestamp = self.header.max_timestamp;
        self.value = match self.scan_hint {
            ScanHint::Sum => self.header.value_sum,
            ScanHint::Count => match self.header.value_type {
                ValueType::UInteger64 => (self.header.count as u64).into(),
                ValueType::Integer64 => (self.header.count as i64).into(),
                ValueType::Float64 => (self.header.count as f64).into(),
            },
            ScanHint::Min => self.header.min_value,
            ScanHint::Max => self.header.max_value,
            ScanHint::None => unreachable!(),
        };
        self.values_read = self.header.count as u64;
    }

    fn use_query_hint_for_value(&mut self, value: Value) {
        self.value = match self.scan_hint {
            ScanHint::Count => match self.header.value_type {
                ValueType::UInteger64 => 1u64.into(),
                ValueType::Integer64 => 1i64.into(),
                ValueType::Float64 => 1f64.into(),
            },
            _ => value,
        };
    }

    fn load_next_file(&mut self) -> Option<()> {
        self.file_index += 1;

        if self.file_index == self.file_paths.len() {
            // flush all pages from the last file
            self.page_cache
                .borrow_mut()
                .flush_pages_for_file(self.file_id);
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
        self.decomp_engine = IntDecompressor::new(
            page_cache_sequential_read(
                self.page_cache.clone(),
                self.file_id,
                MAGIC_SIZE + HEADER_SIZE,
            ),
            &self.header,
        );

        self.file_lock =
            FileLockGuard::new_shared(self.page_cache.borrow_mut().get_file(self.file_id)).unwrap();

        // Use the query hint if applicable on the next file
        if self.scan_hint != ScanHint::None
            && self.start <= self.header.min_timestamp
            && self.header.max_timestamp <= self.end
        {
            self.use_query_hint();
        }
        Some(())
    }

    pub fn next_vector(&mut self) -> Option<Vector> {
        if self.is_done {
            return None;
        }

        let to_return = Vector {
            timestamp: self.current_timestamp,
            value: self.value,
        };

        if self.values_read == self.header.count as u64 {
            if self.load_next_file().is_none() {
                self.is_done = true;
                self.file_lock.release().unwrap();
            }
            return Some(to_return);
        }

        let current = self.decomp_engine.next();
        self.current_timestamp = current.0;
        self.value = current.1.into();
        self.use_query_hint_for_value(self.value);

        if self.current_timestamp > self.end {
            self.file_lock.release().unwrap();
            self.is_done = true;
        }
        self.values_read += 1;

        Some(to_return)
    }

    pub fn is_done(&self) -> bool {
        self.is_done
    }

    pub fn value_type(&self) -> ValueType {
        self.header.value_type
    }
}

pub struct Cursor {
    cursor: Option<CursorImpl>,
}

impl Cursor {
    pub fn new(
        file_paths: Vec<PathBuf>,
        start: Timestamp,
        end: Timestamp,
        page_cache: Rc<RefCell<PageCache>>,
        scan_hint: ScanHint,
    ) -> Result<Self, io::Error> {
        if file_paths.is_empty() {
            Ok(Cursor { cursor: None })
        } else {
            let cursor = CursorImpl::new(file_paths, start, end, page_cache, scan_hint)?;
            Ok(Cursor {
                cursor: Some(cursor),
            })
        }
    }

    pub fn is_done(&self) -> bool {
        if let Some(cursor) = &self.cursor {
            cursor.is_done()
        } else {
            true
        }
    }

    pub fn value_type(&self) -> Option<ValueType> {
        self.cursor.as_ref().map(|c| c.value_type())
    }
}

impl Iterator for Cursor {
    type Item = Vector;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cursor) = &mut self.cursor {
            cursor.next_vector()
        } else {
            None
        }
    }
}

pub struct TimeDataFile {
    pub header: Header,
    pub timestamps: Vec<Timestamp>,
    pub values: Vec<Value>,
}

impl TimeDataFile {
    pub fn new(version: Version, stream_id: StreamId, value_type: ValueType) -> Self {
        Self {
            header: Header::new(version, stream_id, value_type),
            timestamps: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn read_data_file(path: PathBuf) -> Self {
        let page_cache = PageCache::new(100);

        let mut cursor = Cursor::new(
            vec![path],
            0,
            u64::MAX,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

        let mut timestamps = Vec::new();
        let mut values = Vec::new();

        for Vector { timestamp, value } in cursor.by_ref() {
            timestamps.push(timestamp);
            values.push(value);
        }

        // SAFETY: cursor.cursor is only None in the case of 0 files, which is not possible
        //         here since we pass in a non-empty file_paths to Cursor::new
        Self {
            header: cursor.cursor.unwrap().header,
            timestamps,
            values,
        }
    }

    pub fn write(&self, path: PathBuf) -> usize {
        let mut file = File::create(path).unwrap();

        let header_bytes = self.header.write(&mut file).unwrap();
        let mut comp_engine = IntCompressor::new(file, &self.header);

        let mut compressed_bytes = 0;

        for i in 1usize..(self.header.count as usize) {
            compressed_bytes +=
                comp_engine.consume(self.timestamps[i], self.values[i].get_uinteger64());
        }

        compressed_bytes += comp_engine.flush_all();
        header_bytes + compressed_bytes
    }

    /// Precondition: The ValueType of value must be the same as self.header.value_type
    pub fn write_data_to_file_in_mem(&mut self, timestamp: Timestamp, value: Value) {
        if self.header.count == 0 {
            self.header.first_value = value;
            self.header.min_timestamp = timestamp;
            self.header.max_timestamp = timestamp;
            self.header.min_value = value;
            self.header.max_value = value;
        }

        self.header.count += 1;

        self.header.max_timestamp = Timestamp::max(self.header.max_timestamp, timestamp);
        self.header.min_timestamp = Timestamp::min(self.header.min_timestamp, timestamp);

        self.header.value_sum = self
            .header
            .value_sum
            .add_same(self.header.value_type, &value);
        self.header.max_value = self
            .header
            .max_value
            .max_same(self.header.value_type, &value);
        self.header.min_value = self
            .header
            .min_value
            .min_same(self.header.value_type, &value);

        self.timestamps.push(timestamp);
        self.values.push(value);
    }

    /// Precondition: The ValueType of all the vectors in the batch must be the same as self.header.value_type
    /// Returns the number of entries written in memory
    pub fn write_batch_data_to_file_in_mem(&mut self, batch: &[Vector]) -> usize {
        let space = MAX_NUM_ENTRIES - self.num_entries();
        let n = usize::min(space, batch.len());

        for pair in batch.iter().take(n) {
            self.write_data_to_file_in_mem(pair.timestamp, pair.value);
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

pub struct PartiallyPersistentDataFile {
    pub header: Rc<RefCell<Header>>,
    pub physical_path: PathBuf,
    pub virtual_path: PathBuf,
    compressor: Option<IntCompressor<PartiallyPersistentDataFileWriter>>,
}

impl PartiallyPersistentDataFile {
    pub fn new(
        version: Version,
        stream_id: StreamId,
        value_type: ValueType,
        physical_path: PathBuf,
        virtual_path: PathBuf,
    ) -> Self {
        let header = Rc::new(RefCell::new(Header::new(version, stream_id, value_type)));

        Self {
            header,
            physical_path,
            virtual_path,
            compressor: None,
        }
    }

    pub fn lazy_init(mut self, ts: Timestamp, v: Value) -> Result<Self, WriterErr> {
        self.update_header(ts, v);
        self.header.borrow().write_from_path(&self.physical_path)?;
        let writer =
            PartiallyPersistentDataFileWriter::new(self.header.clone(), &(self.physical_path));
        self.compressor = Option::Some(IntCompressor::new(writer, &self.header.borrow().clone()));

        Ok(self)
    }

    pub fn partial_init(mut self, ts: Timestamp, v: Value) -> Result<Self, WriterErr> {
        let data_file = TimeDataFile::read_data_file(self.physical_path.clone());
        self.header = Rc::new(RefCell::new(data_file.header.clone()));

        if ts < self.header.borrow().max_timestamp {
            return Err(WriterErr::OutOfOrderErr {
                ts,
                prev_ts: self.header.borrow().max_timestamp,
            });
        }

        let writer =
            PartiallyPersistentDataFileWriter::new(self.header.clone(), &(self.physical_path));
        self.compressor = Option::Some(IntCompressor::new_from_partial(writer, data_file));

        self.write(ts, v)?;
        Ok(self)
    }

    fn update_header(&mut self, timestamp: Timestamp, value: Value) {
        let mut header = self.header.borrow_mut();

        if header.count == 0 {
            header.first_value = value;
            header.min_timestamp = timestamp;
            header.max_timestamp = timestamp;
            header.min_value = value;
            header.max_value = value;
        }

        header.count += 1;

        // Update max and min timestamps
        header.max_timestamp = Timestamp::max(header.max_timestamp, timestamp);
        header.min_timestamp = Timestamp::min(header.min_timestamp, timestamp);

        // Update value_sum, max_value, and min_value
        header.value_sum = header.value_sum.add_same(header.value_type, &value);
        header.max_value = header.max_value.max_same(header.value_type, &value);
        header.min_value = header.min_value.min_same(header.value_type, &value);
    }

    pub fn write(&mut self, ts: Timestamp, v: Value) -> Result<(), WriterErr> {
        if ts < self.header.borrow().max_timestamp {
            return Err(WriterErr::OutOfOrderErr {
                ts,
                prev_ts: self.header.borrow().max_timestamp,
            });
        }

        self.update_header(ts, v);

        match self.compressor {
            Some(ref mut compressor) => {
                compressor.consume(ts, v.get_uinteger64());
                Ok(())
            }
            None => Err(WriterErr::CompressorNotInitialized),
        }
    }

    pub fn flush(&mut self) -> Result<(), WriterErr> {
        match self.compressor {
            Some(ref mut compressor) => {
                compressor.flush_all();
                Ok(())
            }
            None => Err(WriterErr::CompressorNotInitialized),
        }
    }

    pub fn num_entries(&self) -> usize {
        self.header.borrow().count as usize
    }
}

struct PartiallyPersistentDataFileWriter {
    header: Rc<RefCell<Header>>,
    file: File,
    path: PathBuf,
}

impl PartiallyPersistentDataFileWriter {
    pub fn new(header: Rc<RefCell<Header>>, path: &PathBuf) -> Self {
        Self {
            header,
            file: OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
                .unwrap(),
            path: path.clone(),
        }
    }
}

impl Write for PartiallyPersistentDataFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _guard = FileLockGuard::new_exclusive(&self.file)?;
        let header_bytes = self.header.borrow().as_bytes();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.seek(SeekFrom::End(0))?;
        let written = self.file.write(buf)?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::*;

    #[test]
    fn test_write() {
        set_up_files!(paths, "cool.ty");
        let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, (i + 10).into());
        }
        model.write(paths[0].clone());
    }

    #[test]
    fn test_header_write_parse() {
        set_up_files!(paths, "temp_file.ty");
        let mut temp_file: File = File::create(&paths[0]).unwrap();

        let t_header = Header {
            count: 11,
            value_sum: 101u64.into(),
            ..Header::new(Version(0), StreamId(0), ValueType::UInteger64)
        };

        t_header.write(&mut temp_file).unwrap();

        let _temp_file: File = File::open(&paths[0]).unwrap();
        let mut page_cache = PageCache::new(100);
        let file_id = page_cache.register_or_get_file_id(&paths[0]);
        let parsed_header = Header::parse(file_id, &mut page_cache);
        assert!(t_header == parsed_header);
    }

    #[test]
    fn test_cursor() {
        set_up_files!(paths, "1.ty");
        let mut model = TimeDataFile::new(Version(0), StreamId(0), ValueType::UInteger64);
        for i in 0..10u64 {
            model.write_data_to_file_in_mem(i, (i + 10).into());
        }
        model.write(paths[0].clone());

        let page_cache = PageCache::new(10);
        let cursor = Cursor::new(
            vec![paths[0].clone()],
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        );
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        for (i, Vector { timestamp, value }) in cursor.by_ref().enumerate() {
            assert_eq!(timestamp, model.timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &model.values[i]));
        }
    }

    #[test]
    fn test_single_valued_file() {
        set_up_files!(paths, "1.ty");
        generate_ty_file(paths[0].clone(), &[1], &[2u64.into()]);

        let mut page_cache = PageCache::new(10);
        page_cache.register_or_get_file_id(&paths[0]);
        let mut cursor = Cursor::new(
            vec![paths[0].clone()],
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

        for Vector { timestamp, value } in cursor.by_ref() {
            println!("{} {}", timestamp, value.get_uinteger64());
            assert_eq!(timestamp, 1);
            assert!(value.eq_same(ValueType::UInteger64, &2u64.into()));
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
            for _ in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push((timestamp + 10).into());
                timestamp += 1;
            }

            generate_ty_file(file_path.clone(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let page_cache = PageCache::new(10);

        let cursor = Cursor::new(
            file_paths,
            0,
            100,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        );
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();

        for (i, Vector { timestamp, value }) in cursor.by_ref().enumerate() {
            assert_eq!(timestamp, timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &values[i]));
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
            for _ in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push((timestamp + 10).into());
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let page_cache = PageCache::new(10);
        let cursor = Cursor::new(
            file_paths,
            5,
            23,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        );
        assert!(cursor.is_ok());

        let mut cursor = cursor.unwrap();
        let mut i = 5;

        for Vector { timestamp, value } in cursor.by_ref() {
            assert_eq!(timestamp, timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &values[i]));
            i += 1;
        }
        assert_eq!(i, 24);
    }

    #[test]
    fn test_compression() {
        set_up_files!(paths, "1.ty");
        let mut timestamps = Vec::<u64>::new();
        let mut values = Vec::new();

        for i in 1..100000u64 {
            timestamps.push(i);
            values.push((i * 200000).into());
        }

        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            paths,
            1,
            100000,
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

        for (i, Vector { timestamp, value }) in cursor.by_ref().enumerate() {
            assert_eq!(timestamp, timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &values[i]));
        }
    }

    #[test]
    fn test_compression_2() {
        set_up_files!(paths, "1.ty");
        let timestamps: Vec<u64> = vec![1, 257, 69000, (u32::MAX as u64) + 69000];
        let values = vec![
            1u64.into(),
            257u64.into(),
            69000u64.into(),
            ((u32::MAX as u64) + 69000).into(),
        ];
        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            paths,
            1,
            timestamps[timestamps.len() - 1],
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

        let mut i = 0;
        for Vector { timestamp, value } in cursor.by_ref() {
            assert_eq!(timestamp, timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &values[i]));
            i += 1;
        }
        assert_eq!(i, timestamps.len());
    }

    #[test]
    fn test_compression_negative_deltas() {
        set_up_files!(paths, "1.ty");

        let timestamps = vec![
            1,
            25,
            27,
            35,
            u32::MAX as u64,
            (u32::MAX as u64) + 69000,
            (u32::MAX as u64) + 69001,
        ];
        let values = vec![
            100u64.into(),
            3u64.into(),
            23u64.into(),
            0u64.into(),
            100u64.into(),
            (u32::MAX as u64).into(),
            1u64.into(),
        ];
        generate_ty_file(paths[0].clone(), &timestamps, &values);

        let page_cache = PageCache::new(100);
        let mut cursor = Cursor::new(
            paths,
            1,
            timestamps[timestamps.len() - 1],
            Rc::new(RefCell::new(page_cache)),
            ScanHint::None,
        )
        .unwrap();

        let mut i = 0;
        for Vector { timestamp, value } in cursor.by_ref() {
            assert_eq!(timestamp, timestamps[i]);
            assert!(value.eq_same(ValueType::UInteger64, &values[i]));
            i += 1;
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
            for _ in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push((timestamp + 1).into());
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let page_cache = Rc::new(RefCell::new(PageCache::new(10)));

        let get_value = |start: Timestamp, end: Timestamp, hint: ScanHint| -> (Value, i32) {
            let mut cursor =
                Cursor::new(file_paths.clone(), start, end, page_cache.clone(), hint).unwrap();
            let mut i = 0;
            let mut res: Value = 0u64.into();
            for Vector { value, .. } in cursor.by_ref() {
                res = res.add(ValueType::UInteger64, &value, ValueType::UInteger64);
                i += 1;
            }

            (res, i)
        };

        let (res, i) = get_value(0, 30, ScanHint::Sum);
        assert_eq!(i, 3);
        assert!(res.eq_same(ValueType::UInteger64, &465u64.into()));

        let (res, i) = get_value(5, 28, ScanHint::Sum);
        assert!(res.eq_same(ValueType::UInteger64, &420u64.into()));
        assert_eq!(i, 15);

        let (res, _) = get_value(0, 9, ScanHint::Sum);
        assert!(res.eq_same(ValueType::UInteger64, &55u64.into()));
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
            for _ in 0..10u64 {
                local_timestamps.push(timestamp);
                local_values.push((timestamp + 1).into());
                timestamp += 1;
            }

            generate_ty_file(file_path.into(), &local_timestamps, &local_values);
            timestamps.append(&mut local_timestamps);
            values.append(&mut local_values);
        }

        let page_cache = Rc::new(RefCell::new(PageCache::new(10)));

        let get_value = |start: Timestamp, end: Timestamp, hint: ScanHint| -> (Value, i32) {
            let mut cursor =
                Cursor::new(file_paths.clone(), start, end, page_cache.clone(), hint).unwrap();

            let mut i = 0;
            let mut res: Value = u64::MAX.into();
            for Vector { value, .. } in cursor.by_ref() {
                res = res.min(ValueType::UInteger64, &value, ValueType::UInteger64);
                i += 1;
            }

            (res, i)
        };

        let (res, i) = get_value(0, 30, ScanHint::Min);
        assert!(res.eq_same(ValueType::UInteger64, &1u64.into()));
        assert_eq!(i, 3);

        let (res, i) = get_value(5, 28, ScanHint::Min);
        assert!(res.eq_same(ValueType::UInteger64, &6u64.into()));
        assert_eq!(i, 15);

        let (res, _) = get_value(2, 9, ScanHint::Min);
        assert!(res.eq_same(ValueType::UInteger64, &3u64.into()));
    }

    #[test]
    fn test_empty_files() {
        let mut cursor = Cursor::new(
            vec![],
            0,
            100,
            Rc::new(RefCell::new(PageCache::new(10))),
            ScanHint::None,
        )
        .unwrap();
        assert!(cursor.next().is_none());
    }
}
