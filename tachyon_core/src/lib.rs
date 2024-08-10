use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
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
    integer64: i64,
    uinteger64: u64,
    float64: f64,
}

impl Default for Value {
    fn default() -> Self {
        Self { uinteger64: 0 }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.get_uinteger64() == other.get_uinteger64()
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self { uinteger64: value }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self { integer64: value }
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self { float64: value }
    }
}

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_fmt(format_args!("u64r {}", self.get_uinteger64()))
    }
}

impl Value {
    #[inline]
    pub fn get_integer64(&self) -> i64 {
        unsafe { self.integer64 }
    }

    #[inline]
    pub fn get_uinteger64(&self) -> u64 {
        unsafe { self.uinteger64 }
    }

    #[inline]
    pub fn get_float64(&self) -> f64 {
        unsafe { self.float64 }
    }

    pub fn add(
        &self,
        value_type_self: ValueType,
        other: &Value,
        value_type_other: ValueType,
    ) -> Self {
        if value_type_self == value_type_other {
            todo!();
        }

        todo!();
    }

    pub fn add_same(&self, value_type: ValueType, other: &Value) -> Self {
        self.add(value_type, other, value_type)
    }

    pub fn min(
        &self,
        value_type_self: ValueType,
        other: &Value,
        value_type_other: ValueType,
    ) -> Self {
        if value_type_self == value_type_other {
            todo!();
        }

        todo!();
    }

    pub fn min_same(&self, value_type: ValueType, other: &Value) -> Self {
        self.min(value_type, other, value_type)
    }

    pub fn max(
        &self,
        value_type_self: ValueType,
        other: &Value,
        value_type_other: ValueType,
    ) -> Self {
        if value_type_self == value_type_other {
            todo!();
        }

        todo!();
    }

    pub fn max_same(&self, value_type: ValueType, other: &Value) -> Self {
        self.max(value_type, other, value_type)
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
