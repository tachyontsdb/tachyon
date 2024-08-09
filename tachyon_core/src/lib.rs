use crate::storage::page_cache::PageCache;
use std::cell::RefCell;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use uuid::Uuid;

pub type Timestamp = u64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ValueType {
    Integer64,
    UInteger64,
    Float64,
}

impl TryFrom<u64> for ValueType {
    type Error = ();
    fn try_from(value: u64) -> Result<Self, ()> {
        match value {
            0 => Ok(ValueType::Integer64),
            1 => Ok(ValueType::UInteger64),
            2 => Ok(ValueType::Float64),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ReturnType {
    Scalar,
    Vector,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub union Value {
    pub integer64: i64,
    pub uinteger64: u64,
    pub float64: f64,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.uinteger64 == other.uinteger64 }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { f.write_fmt(format_args!("u64r {}", self.uinteger64)) }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct Vector {
    pub timestamp: Timestamp,
    pub value: Value,
}

pub struct Connection {
    db_dir: PathBuf,
    page_cache: Rc<RefCell<PageCache>>,
    indexer: Rc<RefCell<Indexer>>,
    writer: Rc<RefCell<Writer>>,
}

impl Connection {
    pub fn new(db_dir: impl AsRef<Path>) -> Self {
        fs::create_dir_all(&db_dir).unwrap();

        Self {
            db_dir: db_dir.as_ref().to_path_buf(),
            page_cache: Rc::new(RefCell::new(PageCache::new(10))),
            indexer: todo!(),
            writer: todo!(),
        }
    }

    pub fn create_stream(&mut self, stream: impl AsRef<str>, value_type: ValueType) {
        todo!();
    }

    pub fn delete_stream(&mut self, stream: impl AsRef<str>) {
        todo!();
    }

    pub fn check_stream_exists(&self, stream: impl AsRef<str>) -> bool {
        todo!();
    }

    pub fn prepare_insert(&mut self, stream: impl AsRef<str>) -> Inserter {
        todo!();
    }

    pub fn prepare_query(
        &mut self,
        query: impl AsRef<str>,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
    ) -> Query {
        todo!();
    }
}

pub struct Inserter {
    value_type: ValueType,
    stream_id: Uuid,
}

macro_rules! create_inserter_insert {
    ($function_name: ident, $type: ty, $value_type: expr, $value_field: ident) => {
        pub fn $function_name(&mut self, timestamp: Timestamp, value: $type) {
            if self.value_type != $value_type {
                panic!("Invalid value type on insert!");
            }

            self.insert(
                timestamp,
                Value {
                    $value_field: value,
                },
            );
        }
    };
}

impl Inserter {
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    fn insert(&mut self, timestamp: Timestamp, value: Value) {
        todo!();
    }

    create_inserter_insert!(insert_integer64, i64, ValueType::Integer64, integer64);
    create_inserter_insert!(insert_uinteger64, u64, ValueType::UInteger64, uinteger64);
    create_inserter_insert!(insert_float64, f64, ValueType::Float64, float64);

    pub fn flush(&mut self) {
        todo!();
    }
}

pub struct Query;

impl Query {
    pub fn value_type(&self) -> ValueType {
        todo!();
    }

    pub fn return_type(&self) -> ReturnType {
        todo!();
    }

    pub fn next_scalar(&mut self) -> Option<Value> {
        todo!();
    }

    pub fn next_vector(&mut self) -> Option<Vector> {
        todo!();
    }
}

mod ffi;

mod storage;
mod utils;
