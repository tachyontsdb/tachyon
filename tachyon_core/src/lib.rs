use crate::query::indexer::Indexer;
use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
use promql_parser::parser;
use std::cell::RefCell;
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use uuid::Uuid;

pub const CURRENT_VERSION: u16 = 2;

pub type Timestamp = u64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ValueType {
    Integer64,
    UInteger64,
    Float64,
}

impl TryFrom<u8> for ValueType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueType::Integer64),
            1 => Ok(ValueType::UInteger64),
            2 => Ok(ValueType::Float64),
            _ => Err(()),
        }
    }
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
            match value_type_self {
                ValueType::Integer64 => (self.get_integer64() + other.get_integer64()).into(),
                ValueType::UInteger64 => (self.get_uinteger64() + other.get_uinteger64()).into(),
                ValueType::Float64 => (self.get_float64() + other.get_float64()).into(),
            }
        } else {
            todo!();
        }
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
            match value_type_self {
                ValueType::Integer64 => (self.get_integer64().min(other.get_integer64())).into(),
                ValueType::UInteger64 => (self.get_uinteger64().min(other.get_uinteger64())).into(),
                ValueType::Float64 => (self.get_float64().min(other.get_float64())).into(),
            }
        } else {
            todo!();
        }
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
            match value_type_self {
                ValueType::Integer64 => (self.get_integer64().max(other.get_integer64())).into(),
                ValueType::UInteger64 => (self.get_uinteger64().max(other.get_uinteger64())).into(),
                ValueType::Float64 => (self.get_float64().max(other.get_float64())).into(),
            }
        } else {
            todo!();
        }
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

        let indexer = Rc::new(RefCell::new(Indexer::new(db_dir.as_ref().to_path_buf())));
        indexer.borrow_mut().create_store();
        Self {
            db_dir: db_dir.as_ref().to_path_buf(),
            page_cache: Rc::new(RefCell::new(PageCache::new(10))),
            indexer: indexer.clone(),
            writer: Rc::new(RefCell::new(Writer::new(db_dir, indexer, CURRENT_VERSION))),
        }
    }

    pub fn create_stream(&mut self, stream: impl AsRef<str>, value_type: ValueType) {
        let stream_id = self.try_get_stream_id_from_matcher(stream);
        if stream_id.0.is_some() {
            panic!("Attempting to create a stream that already exists!");
        }

        let vec_sel = stream_id.1;
        let stream_id = self.indexer.borrow_mut().insert_new_id(
            vec_sel.name.as_ref().unwrap(),
            &vec_sel.matchers,
            value_type,
        );
        self.writer.borrow_mut().create_stream(stream_id);
    }

    pub fn delete_stream(&mut self, stream: impl AsRef<str>) {
        todo!();
    }

    pub fn check_stream_exists(&self, stream: impl AsRef<str>) -> bool {
        self.try_get_stream_id_from_matcher(stream).0.is_some()
    }

    pub fn prepare_insert(&mut self, stream: impl AsRef<str>) -> Inserter {
        let stream_id = self.try_get_stream_id_from_matcher(stream).0.unwrap();

        Inserter {
            value_type: self
                .indexer
                .borrow()
                .get_stream_value_type(stream_id)
                .unwrap(),
            stream_id,
            writer: self.writer.clone(),
        }
    }

    pub fn prepare_query(
        &mut self,
        query: impl AsRef<str>,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
    ) -> Query {
        todo!();
    }

    fn try_get_stream_id_from_matcher(
        &self,
        stream: impl AsRef<str>,
    ) -> (Option<Uuid>, parser::VectorSelector) {
        let ast = parser::parse(stream.as_ref()).unwrap();

        let parser::Expr::VectorSelector(vec_sel) = ast else {
            panic!("Expected a vector selector!");
        };

        if vec_sel.at.is_some() || vec_sel.offset.is_some() {
            panic!("Cannot include at / offset for insert query!");
        }

        let stream_ids = self
            .indexer
            .borrow()
            .get_stream_ids(vec_sel.name.as_ref().unwrap(), &vec_sel.matchers);

        (
            match stream_ids.len() {
                0 => None,
                1 => Some(stream_ids.into_iter().next().unwrap()),
                _ => panic!(
                    "Multiple streams matched selector, can only insert into one stream at a time!"
                ),
            },
            vec_sel,
        )
    }
}

pub struct Inserter {
    value_type: ValueType,
    stream_id: Uuid,
    writer: Rc<RefCell<Writer>>,
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
        self.writer
            .borrow_mut()
            .write(self.stream_id, timestamp, value, self.value_type);
    }

    create_inserter_insert!(insert_integer64, i64, ValueType::Integer64, integer64);
    create_inserter_insert!(insert_uinteger64, u64, ValueType::UInteger64, uinteger64);
    create_inserter_insert!(insert_float64, f64, ValueType::Float64, float64);

    pub fn flush(&mut self) {
        self.writer.borrow_mut().flush_all();
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

mod query;
mod storage;
mod utils;
