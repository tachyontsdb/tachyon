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
    pub fn try_convert_into_i64(&self, value_type: ValueType) -> Result<i64, ()> {
        match value_type {
            ValueType::Integer64 => Ok(self.get_integer64()),
            ValueType::UInteger64 => self.get_uinteger64().try_into().map_err(|_| ()),
            ValueType::Float64 => {
                let value = self.get_float64();
                if value.fract() == 0.0 {
                    Ok(value as i64)
                } else {
                    Err(())
                }
            }
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

#[cfg(feature = "tachyon_benchmarks")]
pub mod tachyon_benchmarks {
    pub use crate::storage::file::*;
    pub use crate::storage::page_cache::PageCache;
}

#[cfg(test)]
mod tests {
    use crate::{utils::test::set_up_dirs, Connection, Inserter, Timestamp, ValueType};
    use std::{borrow::Borrow, collections::HashSet, iter::zip, path::PathBuf};

    fn create_stream_helper(conn: &mut Connection, stream: &str) -> Inserter {
        if !conn.check_stream_exists(stream) {
            conn.create_stream(stream, ValueType::UInteger64);
        }

        conn.prepare_insert(stream)
    }

    fn e2e_vector_test(
        root_dir: PathBuf,
        start: u64,
        end: u64,
        first_i: usize,
        expected_count: usize,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"}"#;
        let mut stmt = conn.prepare_query(query, Some(start), Some(end));

        // Process results
        let mut i = first_i;
        let mut count = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values[i], res.value.get_uinteger64());
            i += 1;

            count += 1;
        }

        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_e2e_vector_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 23, 51, 0, 4);
    }

    #[test]
    fn test_e2e_vector_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_vector_test(root_dir, 29, 40, 1, 2);
    }

    #[test]
    fn test_e2e_multiple_streams() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        for (t, v) in zip(timestamps, values) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let timestamps_2 = [12, 15, 30, 67];
        let values_2 = [1, 5, 40, 20];

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "cool"}"#);

        for (t, v) in zip(timestamps_2, values_2) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        let mut stmt = conn.prepare_query(
            r#"http_requests_total{service = "web"}"#,
            Some(23),
            Some(51),
        );

        let mut i = 0;

        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values[i], res.value.get_uinteger64());
            i += 1;
        }

        assert_eq!(i, 4);

        let mut stmt = conn.prepare_query(
            r#"http_requests_total{service = "cool"}"#,
            Some(12),
            Some(67),
        );

        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }
            let res = res.unwrap();
            assert_eq!(timestamps_2[i], res.timestamp);
            assert_eq!(values_2[i], res.value.get_uinteger64());
            i += 1;
        }

        assert_eq!(i, 4);
    }

    fn e2e_scalar_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        start: u64,
        end: u64,
        expected_val: u64,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = format!(r#"{}(http_requests_total{{service = "web"}})"#, operation);
        let mut stmt = conn.prepare_query(&query, Some(start), Some(end));

        // Process results
        let actual_val = stmt.next_scalar().unwrap();
        assert_eq!(actual_val.get_uinteger64(), expected_val);
        assert!(stmt.next_scalar().is_none());
    }

    #[test]
    fn test_e2e_sum_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 23, 51, 163)
    }

    #[test]
    fn test_e2e_sum_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 29, 40, 70)
    }

    #[test]
    fn test_e2e_count_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 23, 51, 4)
    }

    #[test]
    fn test_e2e_count_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 29, 40, 2)
    }

    #[test]
    fn test_e2e_avg_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 23, 51, 40)
    }

    #[test]
    fn test_e2e_avg_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 29, 40, 35)
    }

    #[test]
    fn test_e2e_min_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 23, 51, 23)
    }

    #[test]
    fn test_e2e_min_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 29, 40, 23)
    }

    #[test]
    fn test_e2e_max_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 23, 51, 48)
    }

    #[test]
    fn test_e2e_max_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 29, 40, 47)
    }

    fn e2e_scalars_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        param: u64,
        start: u64,
        end: u64,
        expected_val: Vec<u64>,
    ) {
        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 25, 29, 40, 44, 51];
        let values = [27u64, 31, 47, 23, 31, 48];

        let mut inserter =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = format!(
            r#"{}({}, http_requests_total{{service = "web"}})"#,
            operation, param
        );
        let mut stmt = conn.prepare_query(&query, Some(start), Some(end));

        // Process results
        let mut actual_val: Vec<u64> = Vec::new();
        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }
            actual_val.push(res.unwrap().get_uinteger64());
        }

        assert_eq!(actual_val, expected_val);
    }

    #[test]
    fn test_e2e_bottomk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "bottomk", 2, 23, 51, [23, 27].to_vec())
    }

    #[test]
    fn test_e2e_bottomk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "bottomk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_bottomk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "bottomk",
            10000,
            23,
            51,
            [23, 27, 31, 31, 47, 48].to_vec(),
        )
    }

    #[test]
    fn test_e2e_topk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "topk", 2, 23, 51, [48, 47].to_vec())
    }

    #[test]
    fn test_e2e_topk_zero_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(root_dir, "topk", 0, 23, 51, [].to_vec())
    }

    #[test]
    fn test_e2e_topk_large_k() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "topk",
            10000,
            23,
            51,
            [48, 47, 31, 31, 27, 23].to_vec(),
        )
    }

    #[test]
    fn test_vector_to_vector_no_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} * http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values_a[i] * values_b[i], res.value.get_uinteger64());
            i += 1;
        }
    }

    fn vec_union<T: Ord + Eq + std::hash::Hash + Clone>(v1: &Vec<T>, v2: &Vec<T>) -> Vec<T> {
        let mut set = HashSet::<T>::new();

        for e in v1 {
            set.insert(e.clone());
        }

        for e in v2 {
            set.insert(e.clone());
        }

        let mut vec: Vec<T> = set.into_iter().collect();
        vec.sort();

        vec
    }

    fn e2e_vector_to_vector_test(
        root_dir: PathBuf,
        timestamps_a: Vec<Timestamp>,
        values_a: Vec<u64>,
        timestamps_b: Vec<Timestamp>,
        values_b: Vec<u64>,
        expected_timestamps: Vec<Timestamp>,
        expected_values: Vec<u64>,
    ) {
        let mut conn = Connection::new(root_dir);

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps_a, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps_b, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} + http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(
                expected_values[i],
                res.value.get_uinteger64(),
                "Comparison failed at time {} with expected {} and actual {}",
                expected_timestamps[i],
                expected_values[i],
                res.value.get_uinteger64()
            );
            assert_eq!(expected_timestamps[i], res.timestamp);
            i += 1;
        }
    }

    #[test]
    fn test_vector_to_vector_basic_interpolation_1() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![10, 20, 30, 40];
        let values_a = vec![0, 20, 0, 20];

        let timestamps_b = vec![5, 15, 25, 35, 45];
        let values_b = vec![10, 10, 10, 10, 10];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_vector_basic_interpolation_2() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![5, 15, 25, 35, 45];
        let values_a = vec![10, 10, 10, 10, 10];

        let timestamps_b = vec![10, 20, 30, 40];
        let values_b = vec![0, 20, 0, 20];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_vector_complex_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![1, 2, 4, 6, 10, 12, 13, 14, 15, 16];
        let values_a = vec![10, 20, 30, 20, 20, 10, 15, 20, 80, 100];

        let timestamps_b = vec![3, 5, 7, 8, 9, 11, 16];
        let values_b = vec![30, 30, 10, 20, 20, 10, 10];

        let expected_values = vec![
            40, 50, 55, 60, 55, 40, 30, 40, 40, 35, 25, 20, 25, 30, 90, 110,
        ];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        e2e_vector_to_vector_test(
            root_dir,
            timestamps_a,
            values_a,
            timestamps_b,
            values_b,
            expected_timestamps,
            expected_values,
        )
    }

    #[test]
    fn test_vector_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"} + sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let mut i = 0;
        let sum_values_b = values_b.iter().sum::<u64>();
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values_a[i] + sum_values_b, res.value.get_uinteger64());
            i += 1;
        }
    }

    #[test]
    fn test_scalar_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir);

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "web"}"#);

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 =
            create_stream_helper(&mut conn, r#"http_requests_total{service = "mobile"}"#);

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"sum(http_requests_total{service = "web"}) / sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100));

        // Process results
        let sum_values_a = values_a.iter().sum::<u64>();
        let sum_values_b = values_b.iter().sum::<u64>();

        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(sum_values_a / sum_values_b, res.get_uinteger64());
        }
    }
}
