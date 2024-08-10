use super::page_cache::{FileId, PageCache};
use super::FileReaderUtils;
use crate::{Timestamp, Value, ValueType};
use std::cell::RefCell;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::rc::Rc;

const MAGIC_SIZE: usize = 4;
const MAGIC: [u8; MAGIC_SIZE] = [b'T', b'a', b'c', b'h'];

pub const MAX_NUM_ENTRIES: usize = 62500;

const HEADER_SIZE: usize = 63;
#[derive(PartialEq, Debug)]
pub struct Header {
    pub version: u16,
    pub stream_id: u64,

    pub min_timestamp: Timestamp,
    pub max_timestamp: Timestamp,

    pub count: u32,
    pub value_type: ValueType,

    pub value_sum: Value,
    pub min_value: Value,
    pub max_value: Value,

    pub first_value: Value,
}

impl Header {
    pub fn new(version: u16, stream_id: u64, value_type: ValueType) -> Self {
        Self {
            version,
            stream_id,

            min_timestamp: Timestamp::default(),
            max_timestamp: Timestamp::default(),

            count: 0,
            value_type,

            value_sum: Value::default(),
            min_value: Value::default(),
            max_value: Value::default(),

            first_value: Value::default(),
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
        let buffer = &mut buffer[MAGIC_SIZE..];

        let value_type = FileReaderUtils::read_u64_1(&buffer[30..31])
            .try_into()
            .unwrap();
        Self {
            version: FileReaderUtils::read_u64_2(&buffer[0..2])
                .try_into()
                .unwrap(),
            stream_id: FileReaderUtils::read_u64_8(&buffer[2..10]),
            min_timestamp: FileReaderUtils::read_u64_8(&buffer[10..18]),
            max_timestamp: FileReaderUtils::read_u64_8(&buffer[18..26]),
            count: FileReaderUtils::read_u64_4(&buffer[26..30])
                .try_into()
                .unwrap(),
            value_type,
            value_sum: Self::parse_value(value_type, &buffer[31..39]),
            min_value: Self::parse_value(value_type, &buffer[39..47]),
            max_value: Self::parse_value(value_type, &buffer[47..55]),
            first_value: Self::parse_value(value_type, &buffer[55..63]),
        }
    }

    fn write_value(&self, file: &mut File, value: Value) -> Result<usize, io::Error> {
        match self.value_type {
            ValueType::Integer64 => file.write_all(&value.get_integer64().to_le_bytes())?,
            ValueType::UInteger64 => file.write_all(&value.get_uinteger64().to_le_bytes())?,
            ValueType::Float64 => file.write_all(&value.get_float64().to_le_bytes())?,
        }
        Ok(8)
    }

    fn write(&self, file: &mut File) -> Result<usize, io::Error> {
        file.write_all(&MAGIC)?;

        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.stream_id.to_le_bytes())?;

        file.write_all(&self.min_timestamp.to_le_bytes())?;
        file.write_all(&self.max_timestamp.to_le_bytes())?;

        file.write_all(&self.count.to_le_bytes())?;
        file.write_all(&(self.value_type as u8).to_le_bytes())?;

        self.write_value(file, self.value_sum);
        self.write_value(file, self.min_value);
        self.write_value(file, self.max_value);

        self.write_value(file, self.first_value);

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

    file_paths: Vec<PathBuf>,

    page_cache: Rc<RefCell<PageCache>>,
    decomp_engine: <DefaultScheme as CompressionScheme<SeqPageRead, File>>::Decompressor,

    scan_hint: ScanHint,

    is_done: bool,
}
