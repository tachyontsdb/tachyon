#![allow(dead_code)]

use crate::execution::node::{ExecutorNode, TNode};
use crate::query::indexer::Indexer;
use crate::query::planner::QueryPlanner;
use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
use promql_parser::parser;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::fs;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::path::Path;
use std::rc::Rc;
use uuid::Uuid;

mod ffi;

mod execution;
mod query;
mod storage;
mod utils;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Version(pub u16);

/// Encoded as a 128-bit UUID
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct StreamId(pub u128);

pub const CURRENT_VERSION: Version = Version(2);

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
    (
        $function_name: ident, $function_name_same: ident, $return_type: ty,
        $same_variable_name: ident, $other_variable_name: ident,
        $expr_i64: expr, $expr_u64: expr, $expr_f64: expr,
        $not_equal_block: block
    ) => {
        pub fn $function_name(
            &self,
            value_type_self: crate::ValueType,
            other: &Self,
            value_type_other: crate::ValueType,
        ) -> $return_type {
            if value_type_self == value_type_other {
                self.$function_name_same(value_type_self, other)
            } else {
                $not_equal_block
            }
        }

        /// Safety: `value_type` must be the same between `self` and `other`.
        pub fn $function_name_same(
            &$same_variable_name,
            value_type: crate::ValueType,
            $other_variable_name: &Self,
        ) -> $return_type {
            match value_type {
                crate::ValueType::Integer64 => {
                    $expr_i64
                }
                crate::ValueType::UInteger64 => {
                    $expr_u64
                }
                crate::ValueType::Float64 => {
                    $expr_f64
                }
            }
        }
    };
}

macro_rules! create_value_primitive_fn_simplified {
    ($function_name: ident, $function_name_same: ident, $called_fn: ident) => {
        create_value_primitive_fn!(
            $function_name,
            $function_name_same,
            Self,
            self,
            other,
            self.get_integer64()
                .$called_fn(other.get_integer64())
                .into(),
            self.get_uinteger64()
                .$called_fn(other.get_uinteger64())
                .into(),
            self.get_float64().$called_fn(other.get_float64()).into(),
            {
                panic!("Invalid operation between values of different types!");
            }
        );
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
    pub fn convert_into_u64(&self, value_type: ValueType) -> u64 {
        // TODO: Handle errors
        match value_type {
            ValueType::Integer64 => self.get_integer64() as u64,
            ValueType::UInteger64 => self.get_uinteger64(),
            ValueType::Float64 => self.get_float64() as u64,
        }
    }

    #[inline]
    pub fn convert_into_i64(&self, value_type: ValueType) -> i64 {
        // TODO: Handle errors
        match value_type {
            ValueType::Integer64 => self.get_integer64(),
            ValueType::UInteger64 => self.get_uinteger64() as i64,
            ValueType::Float64 => self.get_float64() as i64,
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

    pub fn get_output(&self, value_type: ValueType) -> String {
        match value_type {
            ValueType::Integer64 => self.get_uinteger64().to_string(),
            ValueType::UInteger64 => self.get_integer64().to_string(),
            ValueType::Float64 => self.get_float64().to_string(),
        }
    }

    create_value_primitive_fn!(
        eq,
        eq_same,
        bool,
        self,
        other,
        self.get_integer64().eq(&other.get_integer64()),
        self.get_uinteger64().eq(&other.get_uinteger64()),
        self.get_float64().eq(&other.get_float64()),
        { false }
    );
    create_value_primitive_fn!(
        partial_cmp,
        partial_cmp_same,
        Option<Ordering>,
        self,
        other,
        self.get_integer64().partial_cmp(&other.get_integer64()),
        self.get_uinteger64().partial_cmp(&other.get_uinteger64()),
        self.get_float64().partial_cmp(&other.get_float64()),
        { None }
    );

    create_value_primitive_fn_simplified!(add, add_same, add);
    create_value_primitive_fn_simplified!(sub, sub_same, sub);
    create_value_primitive_fn_simplified!(mul, mul_same, mul);

    create_value_primitive_fn!(
        div,
        div_same,
        Self,
        self,
        other,
        (self.get_integer64() as f64)
            .div(other.get_integer64() as f64)
            .into(),
        (self.get_uinteger64() as f64)
            .div(other.get_uinteger64() as f64)
            .into(),
        self.get_float64().div(other.get_float64()).into(),
        {
            panic!("Invalid operation between values of different types!");
        }
    );
    create_value_primitive_fn!(
        mdl,
        mdl_same,
        Self,
        self,
        other,
        (self.get_integer64() as f64)
            .rem(other.get_integer64() as f64)
            .into(),
        (self.get_uinteger64() as f64)
            .rem(other.get_uinteger64() as f64)
            .into(),
        self.get_float64().rem(other.get_float64()).into(),
        {
            panic!("Invalid operation between values of different types!");
        }
    );

    create_value_primitive_fn_simplified!(min, min_same, min);
    create_value_primitive_fn_simplified!(max, max_same, max);
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Vector {
    pub timestamp: Timestamp,
    pub value: Value,
}

pub type StreamSummaryType = (Uuid, Vec<(String, String)>, ValueType);

/// Safety: A connection is only single-threaded
pub struct Connection {
    page_cache: Rc<RefCell<PageCache>>,
    indexer: Rc<RefCell<Indexer>>,
    writer: Rc<RefCell<Writer>>,
}

impl Connection {
    /// Recursively creates the directories to `db_dir` if they do not exist
    pub fn new(db_dir: impl AsRef<Path>) -> Self {
        fs::create_dir_all(&db_dir).unwrap();

        let indexer = Rc::new(RefCell::new(Indexer::new(db_dir.as_ref())));
        indexer.borrow_mut().create_store();
        Self {
            page_cache: Rc::new(RefCell::new(PageCache::new(10))),
            indexer: indexer.clone(),
            writer: Rc::new(RefCell::new(Writer::new(db_dir, indexer, CURRENT_VERSION))),
        }
    }

    fn parse_stream(&self, stream: impl AsRef<str>) -> parser::VectorSelector {
        let Ok(parser::Expr::VectorSelector(selector)) = parser::parse(stream.as_ref()) else {
            panic!("Expected a vector selector!");
        };

        if selector.at.is_some() || selector.offset.is_some() {
            panic!("Cannot include at / offset for insert query!");
        }

        selector
    }

    fn get_stream_ids_for_selector(&self, selector: &parser::VectorSelector) -> HashSet<Uuid> {
        self.indexer
            .borrow()
            .get_stream_ids(selector.name.as_ref().unwrap(), &selector.matchers)
    }

    pub fn create_stream(&mut self, stream: impl AsRef<str>, value_type: ValueType) {
        let selector = self.parse_stream(stream);

        if !self.get_stream_ids_for_selector(&selector).is_empty() {
            panic!("Attempting to create a stream that already exists!");
        }

        let stream_id = self.indexer.borrow_mut().insert_new_id(
            selector.name.as_ref().unwrap(),
            &selector.matchers,
            value_type,
        );
        self.writer.borrow_mut().create_stream(stream_id);
    }

    pub fn delete_stream(&mut self, stream: impl AsRef<str>) {
        todo!("Not deleting stream {:?}", stream.as_ref());
    }

    pub fn check_stream_exists(&self, stream: impl AsRef<str>) -> bool {
        !self
            .get_stream_ids_for_selector(&self.parse_stream(stream))
            .is_empty()
    }

    pub fn get_all_streams(&self) -> Vec<StreamSummaryType> {
        self.indexer.borrow().get_all_streams()
    }

    pub fn prepare_insert(&mut self, stream: impl AsRef<str>) -> Inserter {
        let stream_ids = self.get_stream_ids_for_selector(&self.parse_stream(stream.as_ref()));

        if stream_ids.len() != 1 {
            panic!("Invalid number of streams found in the database!");
        }

        let stream_id = stream_ids.into_iter().next().unwrap();

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

impl Query<'_> {
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

#[cfg(feature = "tachyon_benchmarks")]
pub mod tachyon_benchmarks {
    pub use crate::storage::file::*;
    pub use crate::storage::page_cache::PageCache;
}

#[cfg(test)]
mod tests {
    use crate::{utils::test::set_up_dirs, Connection, Inserter, Timestamp, Value, ValueType};
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
        expected_val: Value,
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

        assert!(actual_val.eq_same(stmt.value_type(), &expected_val));
        assert!(stmt.next_scalar().is_none());
    }

    #[test]
    fn test_e2e_sum_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 23, 51, 163u64.into())
    }

    #[test]
    fn test_e2e_sum_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "sum", 29, 40, 70u64.into())
    }

    #[test]
    fn test_e2e_count_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 23, 51, 4u64.into())
    }

    #[test]
    fn test_e2e_count_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "count", 29, 40, 2u64.into())
    }

    #[test]
    fn test_e2e_avg_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 23, 51, 40.75f64.into())
    }

    #[test]
    fn test_e2e_avg_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "avg", 29, 40, 35.0f64.into())
    }

    #[test]
    fn test_e2e_min_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 23, 51, 23u64.into())
    }

    #[test]
    fn test_e2e_min_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "min", 29, 40, 23u64.into())
    }

    #[test]
    fn test_e2e_max_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 23, 51, 48u64.into())
    }

    #[test]
    fn test_e2e_max_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalar_aggregate_test(root_dir, "max", 29, 40, 47u64.into())
    }

    fn e2e_scalars_aggregate_test(
        root_dir: PathBuf,
        operation: &str,
        param: u64,
        start: u64,
        end: u64,
        expected_val: Vec<Value>,
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
        let mut i = 0;
        loop {
            let res = stmt.next_scalar();
            if res.is_none() {
                break;
            }
            println!(
                "Cool: {:#?} {:#?}",
                res.unwrap().get_uinteger64(),
                expected_val[i].get_uinteger64()
            );
            assert!(res.unwrap().eq_same(stmt.value_type(), &expected_val[i]));
            i += 1;
        }
    }

    #[test]
    fn test_e2e_bottomk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "bottomk",
            2,
            23,
            51,
            [23u64.into(), 27u64.into()].to_vec(),
        )
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
            [
                23u64.into(),
                27u64.into(),
                31u64.into(),
                31u64.into(),
                47u64.into(),
                48u64.into(),
            ]
            .to_vec(),
        )
    }

    #[test]
    fn test_e2e_topk() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_scalars_aggregate_test(
            root_dir,
            "topk",
            2,
            23,
            51,
            [48u64.into(), 47u64.into()].to_vec(),
        )
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
            [
                48u64.into(),
                47u64.into(),
                31u64.into(),
                31u64.into(),
                27u64.into(),
                23u64.into(),
            ]
            .to_vec(),
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

        let values_a = [45, 47, 24, 48];
        let values_b = [9, 18, 0, 55];

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
            assert_eq!(sum_values_a as f64 / sum_values_b as f64, res.get_float64());
        }
    }
}
