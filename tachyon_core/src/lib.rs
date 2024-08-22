#![allow(dead_code)]

use crate::query::indexer::Indexer;
use crate::query::node::TNode;
use crate::query::planner::QueryPlanner;
use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
use promql_parser::parser;
use query::node::ExecutorNode;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::fs;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use uuid::Uuid;

pub const CURRENT_VERSION: u16 = 2;
pub const FILE_EXTENSION: &str = "ty";

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
            0 => Ok(Self::Integer64),
            1 => Ok(Self::UInteger64),
            2 => Ok(Self::Float64),
            _ => Err(()),
        }
    }
}

impl TryFrom<u64> for ValueType {
    type Error = ();

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Integer64),
            1 => Ok(Self::UInteger64),
            2 => Ok(Self::Float64),
            _ => Err(()),
        }
    }
}

impl FromStr for ValueType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "i" => Ok(Self::Integer64),
            "u" => Ok(Self::UInteger64),
            "f" => Ok(Self::Float64),
            _ => Err(()),
        }
    }
}

impl Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer64 => f.write_str("Integer64"),
            Self::UInteger64 => f.write_str("UInteger64"),
            Self::Float64 => f.write_str("Float64"),
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

macro_rules! create_value_primitive_fn {
    ($function_name: ident, $function_name_same: ident, $called_fn: ident) => {
        pub fn $function_name(
            &self,
            value_type_self: crate::ValueType,
            other: &Self,
            value_type_other: crate::ValueType,
        ) -> Self {
            if value_type_self == value_type_other {
                match value_type_self {
                    crate::ValueType::Integer64 => {
                        (self.get_integer64().$called_fn(other.get_integer64())).into()
                    }
                    crate::ValueType::UInteger64 => {
                        (self.get_uinteger64().$called_fn(other.get_uinteger64())).into()
                    }
                    crate::ValueType::Float64 => {
                        (self.get_float64().$called_fn(other.get_float64())).into()
                    }
                }
            } else {
                todo!();
            }
        }

        pub fn $function_name_same(&self, value_type: crate::ValueType, other: &Self) -> Self {
            self.$function_name(value_type, other, value_type)
        }
    };
}

impl Value {
    #[inline]
    pub const fn get_integer64(&self) -> i64 {
        unsafe { self.integer64 }
    }

    #[inline]
    pub const fn get_uinteger64(&self) -> u64 {
        unsafe { self.uinteger64 }
    }

    #[inline]
    pub const fn get_float64(&self) -> f64 {
        unsafe { self.float64 }
    }

    #[inline]
    pub const fn convert_into_f64(&self, value_type: ValueType) -> f64 {
        match value_type {
            ValueType::Integer64 => self.get_integer64() as f64,
            ValueType::UInteger64 => self.get_uinteger64() as f64,
            ValueType::Float64 => self.get_float64(),
        }
    }

    #[inline]
    pub const fn get_default(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Integer64 => Value { integer64: 0i64 },
            ValueType::UInteger64 => Value { uinteger64: 0u64 },
            ValueType::Float64 => Value { float64: 0f64 },
        }
    }

    #[inline]
    pub fn eq(
        &self,
        value_type_self: ValueType,
        other: &Self,
        value_type_other: ValueType,
    ) -> bool {
        if value_type_self != value_type_other {
            false
        } else {
            match value_type_self {
                ValueType::Integer64 => self.get_integer64() == other.get_integer64(),
                ValueType::UInteger64 => self.get_uinteger64() == other.get_uinteger64(),
                ValueType::Float64 => self.get_float64() == other.get_float64(),
            }
        }
    }

    #[inline]
    pub fn eq_same(&self, value_type: ValueType, other: &Self) -> bool {
        self.eq(value_type, other, value_type)
    }

    #[inline]
    pub fn partial_cmp(
        &self,
        value_type_self: ValueType,
        other: &Self,
        value_type_other: ValueType,
    ) -> Option<Ordering> {
        if value_type_self != value_type_other {
            None
        } else {
            match value_type_self {
                ValueType::Integer64 => self.get_integer64().partial_cmp(&other.get_integer64()),
                ValueType::UInteger64 => self.get_uinteger64().partial_cmp(&other.get_uinteger64()),
                ValueType::Float64 => self.get_float64().partial_cmp(&other.get_float64()),
            }
        }
    }

    #[inline]
    pub fn partial_cmp_same(&self, value_type: ValueType, other: &Self) -> Option<Ordering> {
        self.partial_cmp(value_type, other, value_type)
    }

    pub fn get_output(&self, value_type: ValueType) -> String {
        match value_type {
            ValueType::Integer64 => self.get_uinteger64().to_string(),
            ValueType::UInteger64 => self.get_integer64().to_string(),
            ValueType::Float64 => self.get_float64().to_string(),
        }
    }

    pub fn try_div(
        &self,
        value_type_self: ValueType,
        other: &Self,
        value_type_other: ValueType,
    ) -> Option<Self> {
        let other = other.convert_into_f64(value_type_other);
        if other == 0f64 {
            None
        } else {
            Some(self.convert_into_f64(value_type_self).div(other).into())
        }
    }

    pub fn try_div_same(&self, value_type: ValueType, other: &Self) -> Option<Self> {
        self.try_div(value_type, other, value_type)
    }

    pub fn try_mod(
        &self,
        value_type_self: ValueType,
        other: &Self,
        value_type_other: ValueType,
    ) -> Option<Self> {
        let other = other.convert_into_f64(value_type_other);
        if other == 0f64 {
            None
        } else {
            Some(self.convert_into_f64(value_type_self).rem(other).into())
        }
    }

    pub fn try_mod_same(&self, value_type: ValueType, other: &Self) -> Option<Self> {
        self.try_mod(value_type, other, value_type)
    }

    create_value_primitive_fn!(add, add_same, add);
    create_value_primitive_fn!(sub, sub_same, sub);
    create_value_primitive_fn!(mul, mul_same, mul);

    create_value_primitive_fn!(min, min_same, min);
    create_value_primitive_fn!(max, max_same, max);
}

#[derive(Clone, Copy)]
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

        let indexer = Rc::new(RefCell::new(Indexer::new(db_dir.as_ref())));
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
        todo!("Not deleting stream {:?}", stream.as_ref());
    }

    pub fn check_stream_exists(&self, stream: impl AsRef<str>) -> bool {
        self.try_get_stream_id_from_matcher(stream).0.is_some()
    }

    pub fn get_all_streams(&self) -> Vec<(Uuid, String, ValueType)> {
        self.indexer.borrow().get_all_streams()
    }

    pub fn prepare_insert(&mut self, stream: impl AsRef<str>) -> Inserter {
        let stream_id = self
            .try_get_stream_id_from_matcher(stream.as_ref())
            .0
            .unwrap_or_else(|| panic!("Stream {:?} not found in db!", stream.as_ref()));

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
        let ast = parser::parse(query.as_ref()).unwrap();
        let mut planner = QueryPlanner::new(&ast, start, end);
        let plan = planner.plan(self);

        Query {
            plan,
            connection: self,
        }
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
        pub fn $function_name(&mut self, timestamp: crate::Timestamp, value: $type) {
            if self.value_type != $value_type {
                panic!("Invalid value type on insert!");
            }

            self.insert(
                timestamp,
                crate::Value {
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

pub struct Query<'a> {
    connection: &'a mut Connection,
    plan: TNode,
}

impl<'a> Query<'a> {
    pub fn value_type(&self) -> ValueType {
        self.plan.value_type()
    }

    pub fn return_type(&self) -> ReturnType {
        self.plan.return_type()
    }

    pub fn next_scalar(&mut self) -> Option<Value> {
        self.plan.next_scalar(self.connection)
    }

    pub fn next_vector(&mut self) -> Option<Vector> {
        self.plan.next_vector(self.connection)
    }
}

mod ffi;

mod query;
mod storage;
mod utils;

#[cfg(test)]
mod e2e_tests;

#[cfg(feature = "tachyon_benchmarks")]
pub mod tachyon_benchmarks {
    pub use crate::storage::file::*;
    pub use crate::storage::page_cache::PageCache;
}
