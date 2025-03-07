#![allow(dead_code)]

use crate::execution::node::{ExecutorNode, TNode};
use crate::query::indexer::Indexer;
use crate::query::planner::QueryPlanner;
use crate::storage::page_cache::PageCache;
use crate::storage::writer::Writer;
use error::{ConnectionErr, QueryErr, TachyonErr};
use promql_parser::parser;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::fs;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::path::Path;
use std::rc::Rc;
use storage::writer::persistent_writer::PersistentWriter;
use uuid::Uuid;

pub mod error;

mod ffi;

mod execution;
mod query;
mod storage;
mod utils;

pub const FILE_EXTENSION: &str = "ty";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
pub struct Version(pub u16);

pub const CURRENT_VERSION: Version = Version(2);

/// Encoded as a 128-bit UUID
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(transparent)]
pub struct StreamId(pub u128);

impl From<StreamId> for Uuid {
    fn from(value: StreamId) -> Self {
        Uuid::from_u128(value.0)
    }
}

pub type Timestamp = u64;

/// Contains the Stream ID, a list of matchers and the value type.
pub type StreamSummaryType = (Uuid, Vec<(String, String)>, ValueType);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ValueType {
    Integer64,
    UInteger64,
    Float64,
}

impl ValueType {
    /// Gets the resulting type from applying operations between two different value types.
    pub fn get_applied_value_type(lhs_value_type: Self, rhs_value_type: Self) -> Self {
        if lhs_value_type == Self::Float64 || rhs_value_type == Self::Float64 {
            Self::Float64
        } else if lhs_value_type == Self::Integer64 || rhs_value_type == Self::Integer64 {
            Self::Integer64
        } else {
            Self::UInteger64
        }
    }
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

impl TryFrom<u8> for ReturnType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Scalar),
            1 => Ok(Self::Vector),
            _ => Err(()),
        }
    }
}

impl Display for ReturnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scalar => f.write_str("Scalar"),
            Self::Vector => f.write_str("Vector"),
        }
    }
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
        $same_variable_name: ident, $same_variable_value_type: ident,
        $other_variable_name: ident, $other_variable_value_type: ident,
        $expr_i64: expr, $expr_u64: expr, $expr_f64: expr,
        $expr_f64_same: expr, $expr_f64_other: expr,
        $expr_i64_same: expr, $expr_i64_other: expr,
    ) => {
        pub fn $function_name(
            &$same_variable_name,
            $same_variable_value_type: crate::ValueType,
            $other_variable_name: &Self,
            $other_variable_value_type: crate::ValueType,
        ) -> $return_type {
            if $same_variable_value_type == crate::ValueType::Float64 || $other_variable_value_type == crate::ValueType::Float64 {
                if $same_variable_value_type == crate::ValueType::Float64 && $other_variable_value_type == crate::ValueType::Float64 {
                    $expr_f64
                } else if $same_variable_value_type == crate::ValueType::Float64 {
                    $expr_f64_same
                } else {
                    $expr_f64_other
                }
            } else if $same_variable_value_type == crate::ValueType::Integer64 || $other_variable_value_type == crate::ValueType::Integer64 {
                if $same_variable_value_type == crate::ValueType::Integer64 && $other_variable_value_type == crate::ValueType::Integer64 {
                    $expr_i64
                } else if $same_variable_value_type == crate::ValueType::Integer64 {
                    $expr_i64_same
                } else {
                    $expr_i64_other
                }
            } else {
                $expr_u64
            }
        }

        /// SAFETY: The value type must be the same between `self` and `other`.
        pub fn $function_name_same(
            &$same_variable_name,
            $same_variable_value_type: crate::ValueType,
            $other_variable_name: &Self,
        ) -> $return_type {
            match $same_variable_value_type {
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
    (
        $function_name: ident, $function_name_same: ident, $return_type: ty,
        $called_fn: ident $(, $t: tt)?
    ) => {
        create_value_primitive_fn!(
            $function_name,
            $function_name_same,
            $return_type,
            self,
            value_type_self,
            other,
            value_type_other,
            self.get_integer64()
                .$called_fn($($t)?other.get_integer64())
                .into(),
            self.get_uinteger64()
                .$called_fn($($t)?other.get_uinteger64())
                .into(),
            self.get_float64().$called_fn($($t)?other.get_float64()).into(),
            self.get_float64().$called_fn($($t)?other.convert_into_f64(value_type_other)).into(),
            self.convert_into_f64(value_type_self).$called_fn($($t)?other.get_float64()).into(),
            self.get_integer64().$called_fn($($t)?other.convert_into_i64(value_type_other)).into(),
            self.convert_into_i64(value_type_self).$called_fn($($t)?other.get_integer64()).into(),
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

    create_value_primitive_fn_simplified!(eq, eq_same, bool, eq, &);
    create_value_primitive_fn_simplified!(partial_cmp, partial_cmp_same, Option<Ordering>, partial_cmp, &);

    create_value_primitive_fn_simplified!(add, add_same, Self, add);
    create_value_primitive_fn_simplified!(sub, sub_same, Self, sub);
    create_value_primitive_fn_simplified!(mul, mul_same, Self, mul);

    create_value_primitive_fn!(
        div,
        div_same,
        Self,
        self,
        value_type_same,
        other,
        value_type_other,
        (self.get_integer64() as f64)
            .div(other.get_integer64() as f64)
            .into(),
        (self.get_uinteger64() as f64)
            .div(other.get_uinteger64() as f64)
            .into(),
        self.get_float64().div(other.get_float64()).into(),
        self.get_float64()
            .div(other.convert_into_f64(value_type_other))
            .into(),
        self.convert_into_f64(value_type_same)
            .div(other.get_float64())
            .into(),
        (self.get_integer64() as f64)
            .div(other.convert_into_f64(value_type_other))
            .into(),
        self.convert_into_f64(value_type_same)
            .div(other.get_integer64() as f64)
            .into(),
    );

    create_value_primitive_fn!(
        mdl,
        mdl_same,
        Self,
        self,
        value_type_same,
        other,
        value_type_other,
        (self.get_integer64() as f64)
            .rem(other.get_integer64() as f64)
            .into(),
        (self.get_uinteger64() as f64)
            .rem(other.get_uinteger64() as f64)
            .into(),
        self.get_float64().rem(other.get_float64()).into(),
        self.get_float64()
            .rem(other.convert_into_f64(value_type_other))
            .into(),
        self.convert_into_f64(value_type_same)
            .rem(other.get_float64())
            .into(),
        (self.get_integer64() as f64)
            .rem(other.convert_into_f64(value_type_other))
            .into(),
        self.convert_into_f64(value_type_same)
            .rem(other.get_integer64() as f64)
            .into(),
    );

    create_value_primitive_fn_simplified!(min, min_same, Self, min);
    create_value_primitive_fn_simplified!(max, max_same, Self, max);
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Vector {
    pub timestamp: Timestamp,
    pub value: Value,
}

/// SAFETY: A connection is only single-threaded
pub struct Connection {
    page_cache: Rc<RefCell<PageCache>>,
    indexer: Rc<RefCell<Indexer>>,
    writer: Rc<RefCell<PersistentWriter>>,
}

impl Connection {
    /// Recursively creates the directories to `db_dir` if they do not exist
    pub fn new(db_dir: impl AsRef<Path>) -> Result<Self, TachyonErr> {
        fs::create_dir_all(&db_dir).map_err(|_| {
            TachyonErr::ConnectionErr(ConnectionErr::DatabaseCreationErr {
                db_dir: db_dir.as_ref().to_path_buf(),
            })
        })?;

        let indexer = Rc::new(RefCell::new(
            Indexer::new(db_dir.as_ref())
                .map_err(|err| TachyonErr::ConnectionErr(ConnectionErr::IndexerErr(err)))?,
        ));
        indexer
            .borrow_mut()
            .create_store()
            .map_err(|err| TachyonErr::ConnectionErr(ConnectionErr::IndexerErr(err)))?;

        Ok(Self {
            page_cache: Rc::new(RefCell::new(PageCache::new(10))),
            indexer: indexer.clone(),
            writer: Rc::new(RefCell::new(PersistentWriter::new(
                db_dir,
                indexer,
                CURRENT_VERSION,
            ))),
        })
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

    pub fn create_stream(
        &mut self,
        stream: impl AsRef<str>,
        value_type: ValueType,
    ) -> Result<(), TachyonErr> {
        let selector = self.parse_stream(&stream);

        if !self.get_stream_ids_for_selector(&selector).is_empty() {
            panic!("Attempting to create a stream that already exists!");
        }

        let stream_id = self
            .indexer
            .borrow_mut()
            .insert_new_id(
                selector.name.as_ref().unwrap(),
                &selector.matchers,
                value_type,
            )
            .map_err(|_| {
                TachyonErr::ConnectionErr(ConnectionErr::StreamCreationErr {
                    stream: stream.as_ref().to_string(),
                })
            })?;
        self.writer.borrow_mut().create_stream(stream_id);

        Ok(())
    }

    pub fn delete_stream(&mut self, stream: impl AsRef<str>) {
        todo!("Not deleting stream {:?}", stream.as_ref());
    }

    pub fn check_stream_exists(&self, stream: impl AsRef<str>) -> bool {
        !self
            .get_stream_ids_for_selector(&self.parse_stream(stream))
            .is_empty()
    }

    pub fn get_all_streams(&self) -> Result<Vec<StreamSummaryType>, TachyonErr> {
        self.indexer
            .borrow()
            .get_all_streams()
            .map_err(|_| TachyonErr::ConnectionErr(ConnectionErr::GetStreamsErr))
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
    ) -> Result<Query, TachyonErr> {
        let ast = parser::parse(query.as_ref())
            .map_err(|_| TachyonErr::QueryErr(QueryErr::QuerySyntaxErr))?;
        let mut planner = QueryPlanner::new(&ast, start, end);
        let plan = planner.plan(self)?;

        Ok(Query {
            plan,
            connection: self,
        })
    }
}

pub struct Inserter {
    value_type: ValueType,
    stream_id: Uuid,
    writer: Rc<RefCell<PersistentWriter>>,
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
    use crate::{
        utils::test::set_up_dirs, Connection, Inserter, Query, ReturnType, Timestamp, Value,
        ValueType,
    };
    use std::{borrow::Borrow, collections::HashSet, iter::zip, path::PathBuf};

    fn create_stream_helper(
        conn: &mut Connection,
        stream: impl AsRef<str>,
        value_type: ValueType,
    ) -> Inserter {
        if !conn.check_stream_exists(stream.as_ref()) {
            conn.create_stream(stream.as_ref(), value_type).unwrap();
        }

        conn.prepare_insert(stream.as_ref())
    }

    fn vector_test_helper(
        root_dir: PathBuf,
        start: u64,
        end: u64,
        first_i: usize,
        expected_count: usize,
    ) {
        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"}"#;
        let mut stmt = conn.prepare_query(query, Some(start), Some(end)).unwrap();

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

    fn e2e_large_vector_test(root_dir: PathBuf) {
        let mut conn = Connection::new(root_dir).unwrap();

        let mut inserter = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        let mut timestamps = Vec::<Timestamp>::new();
        let mut values = Vec::<Value>::new();

        for i in 0..100000u64 {
            timestamps.push(i);
            values.push(i.into());
            inserter.insert(timestamps[i as usize], values[i as usize]);
        }

        inserter.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"}"#;
        let mut stmt = conn
            .prepare_query(query, Some(timestamps[0]), timestamps.last().copied())
            .unwrap();

        // Process results
        let mut i = 0;
        let mut count = 0;
        let expected_count = timestamps.len();
        loop {
            let res = stmt.next_vector();
            if res.is_none() {
                break;
            }

            let res = res.unwrap();
            assert_eq!(timestamps[i], res.timestamp);
            assert_eq!(values[i].get_uinteger64(), res.value.get_uinteger64());
            i += 1;

            count += 1;
        }

        assert_eq!(count, expected_count);
    }

    #[test]
    fn test_e2e_large_vector() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        e2e_large_vector_test(root_dir)
    }

    #[test]
    fn test_e2e_vector_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        vector_test_helper(root_dir, 23, 51, 0, 4);
    }

    #[test]
    fn test_e2e_vector_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        vector_test_helper(root_dir, 29, 40, 1, 2);
    }

    #[test]
    fn test_e2e_multiple_streams() {
        set_up_dirs!(dirs, "db");

        let root_dir = dirs[0].clone();
        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];
        let values = [45, 47, 23, 48];

        let mut inserter1 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps, values) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let timestamps_2 = [12, 15, 30, 67];
        let values_2 = [1, 5, 40, 20];

        let mut inserter2 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "cool"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps_2, values_2) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        let mut stmt = conn
            .prepare_query(
                r#"http_requests_total{service = "web"}"#,
                Some(23),
                Some(51),
            )
            .unwrap();

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

        let mut stmt = conn
            .prepare_query(
                r#"http_requests_total{service = "cool"}"#,
                Some(12),
                Some(67),
            )
            .unwrap();

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

    fn execution_test_helper(root_dir: PathBuf, query: &str, expected: &[Value]) {
        let mut conn = Connection::new(root_dir).unwrap();

        // Insert dummy values
        let timestamps = [10, 20, 30, 40];
        let values = [2i64, 4, 6, 8];
        let mut inserter = create_stream_helper(&mut conn, r#"ints"#, ValueType::Integer64);
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }
        inserter.flush();

        let timestamps = [10, 20, 30, 40];
        let values = [1u64, 2, 3, 4];
        let mut inserter = create_stream_helper(&mut conn, r#"uints"#, ValueType::UInteger64);
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }
        inserter.flush();

        let values = [4.1, 3.2, 2.3, 1.4];
        let mut inserter = create_stream_helper(&mut conn, r#"floats"#, ValueType::Float64);
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }
        inserter.flush();

        let mut stmt = conn.prepare_query(query, Some(0), Some(100)).unwrap();

        // Process results
        let mut i: usize = 0;
        loop {
            let res = match stmt.return_type() {
                ReturnType::Vector => stmt.next_vector().map(|next| next.value),
                ReturnType::Scalar => stmt.next_scalar(),
            };
            match res {
                Some(res) => match stmt.value_type() {
                    ValueType::Integer64 => {
                        assert!(expected[i].eq_same(stmt.value_type(), &res))
                    }
                    ValueType::UInteger64 => {
                        assert!(expected[i].eq_same(stmt.value_type(), &res))
                    }
                    ValueType::Float64 => {
                        assert!((expected[i].get_float64() - res.get_float64()).abs() < 0.001)
                    }
                },
                None => {
                    assert!(i == expected.len());
                    break;
                }
            }
            i += 1;
        }
    }

    #[test]
    fn test_e2e_add_vectors() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints + floats"#,
            &[6.1, 7.2, 8.3, 9.4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_subtract_vectors() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints - uints"#,
            &[1i64, 2, 3, 4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_multiply_vectors() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints * floats"#,
            &[8.2, 12.8, 13.8, 11.2].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_divide_vectors() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints / uints"#,
            &[2.0, 2.0, 2.0, 2.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_modulo_vectors() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints % floats"#,
            &[2.0, 0.8, 1.4, 1.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_add_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints + 2"#,
            &[4.0, 6.0, 8.0, 10.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_subtract_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"uints - 2.5"#,
            &[-1.5, -0.5, 0.5, 1.5].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_multiply_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"floats * 11"#,
            &[45.1, 35.2, 25.3, 15.4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_divide_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"ints / -4"#,
            &[-0.5, -1.0, -1.5, -2.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_modulo_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"uints % 2"#,
            &[1.0, 0.0, 1.0, 0.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_equal_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"(ints % 4) == 2"#,
            &[2.0, 2.0].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_notequal_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"uints != 2"#,
            &[1i64, 3, 4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_greater_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"floats > 2.3"#,
            &[4.1, 3.2].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_less_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"floats < 3.2"#,
            &[2.3, 1.4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_greaterequal_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"floats >= 2.3"#,
            &[4.1, 3.2, 2.3].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_lessequal_vector_scalar() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"floats <= 3.2"#,
            &[3.2, 2.3, 1.4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_operate_scalars() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"(2 + 3 - 4.5) * (5 / 2.3) % 1"#,
            &[0.0870].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_sum_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"sum(ints)"#, &[20i64].map(|x| x.into()));
    }

    #[test]
    fn test_e2e_sum_no_values() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"sum(ints < 0)"#, &[]);
    }

    #[test]
    fn test_e2e_count_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"count(uints)"#,
            &[4i64].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_count_no_values() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"count(ints < 0)"#,
            &[0i64].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_average_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"avg(floats)"#, &[2.75].map(|x| x.into()));
    }

    #[test]
    fn test_e2e_average_no_values() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"avg(ints < 0)"#, &[]);
    }

    #[test]
    fn test_e2e_min_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"min(ints)"#, &[2i64].map(|x| x.into()));
    }

    #[test]
    fn test_e2e_min_no_values() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"min(ints < 0)"#, &[]);
    }

    #[test]
    fn test_e2e_max_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"max(uints)"#, &[4u64].map(|x| x.into()));
    }

    #[test]
    fn test_e2e_max_no_values() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"max(ints < 0)"#, &[]);
    }

    #[test]
    fn test_e2e_topk_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"topk(2, ints)"#,
            &[8i64, 6].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_topk_vector_largek() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"topk(100, uints)"#,
            &[4u64, 3, 2, 1].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_topk_vector_zerok() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"topk(0, floats)"#, &[]);
    }

    #[test]
    fn test_e2e_bottomk_vector() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"bottomk(2, ints)"#,
            &[2i64, 4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_bottomk_vector_largek() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(
            dirs[0].clone(),
            r#"bottomk(100, uints)"#,
            &[1u64, 2, 3, 4].map(|x| x.into()),
        );
    }

    #[test]
    fn test_e2e_bottomk_vector_zerok() {
        set_up_dirs!(dirs, "db");
        execution_test_helper(dirs[0].clone(), r#"bottomk(0, floats)"#, &[]);
    }

    fn aggregate_test_helper(
        root_dir: PathBuf,
        operation: &str,
        start: u64,
        end: u64,
        expected: Value,
    ) {
        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];
        let values = [45u64, 47, 23, 48];

        let mut inserter = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps, values) {
            inserter.insert(t, v.into());
        }

        inserter.flush();

        // Prepare test query
        let query = format!(r#"{}(http_requests_total{{service = "web"}})"#, operation);
        let mut stmt = conn.prepare_query(&query, Some(start), Some(end)).unwrap();

        // Process results
        let actual_val = stmt.next_scalar().unwrap();

        assert!(actual_val.eq_same(stmt.value_type(), &expected));
        assert!(stmt.next_scalar().is_none());
    }

    #[test]
    fn test_e2e_sum_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "sum", 23, 51, 163u64.into())
    }

    #[test]
    fn test_e2e_sum_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "sum", 29, 40, 70u64.into())
    }

    #[test]
    fn test_e2e_count_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "count", 23, 51, 4u64.into())
    }

    #[test]
    fn test_e2e_count_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "count", 29, 40, 2u64.into())
    }

    #[test]
    fn test_e2e_avg_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "avg", 23, 51, 40.75f64.into())
    }

    #[test]
    fn test_e2e_avg_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "avg", 29, 40, 35.0f64.into())
    }

    #[test]
    fn test_e2e_min_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "min", 23, 51, 23u64.into())
    }

    #[test]
    fn test_e2e_min_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "min", 29, 40, 23u64.into())
    }

    #[test]
    fn test_e2e_max_full_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "max", 23, 51, 48u64.into())
    }

    #[test]
    fn test_e2e_max_partial_file() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();
        aggregate_test_helper(root_dir, "max", 29, 40, 47u64.into())
    }

    #[test]
    fn test_e2e_vector_to_vector_no_interpolation() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "mobile"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} * http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100)).unwrap();

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

    fn vector_to_vector_interpolation_test_helper(
        root_dir: PathBuf,
        timestamps_a: Vec<Timestamp>,
        values_a: Vec<u64>,
        timestamps_b: Vec<Timestamp>,
        values_b: Vec<u64>,
        expected_timestamps: Vec<Timestamp>,
        expected_values: Vec<u64>,
    ) {
        let mut conn = Connection::new(root_dir).unwrap();

        let mut inserter1 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps_a, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "mobile"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps_b, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query =
            r#"http_requests_total{service = "web"} + http_requests_total{service = "mobile"}"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100)).unwrap();

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
    fn test_e2e_vector_to_vector_basic_interpolation_1() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![10, 20, 30, 40];
        let values_a = vec![0, 20, 0, 20];

        let timestamps_b = vec![5, 15, 25, 35, 45];
        let values_b = vec![10, 10, 10, 10, 10];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        vector_to_vector_interpolation_test_helper(
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
    fn test_e2e_vector_to_vector_basic_interpolation_2() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let timestamps_a = vec![5, 15, 25, 35, 45];
        let values_a = vec![10, 10, 10, 10, 10];

        let timestamps_b = vec![10, 20, 30, 40];
        let values_b = vec![0, 20, 0, 20];

        let expected_values = vec![10, 10, 20, 30, 20, 10, 20, 30, 30];
        let expected_timestamps = vec_union(timestamps_a.borrow(), timestamps_b.borrow());

        vector_to_vector_interpolation_test_helper(
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
    fn test_e2e_vector_to_vector_complex_interpolation() {
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

        vector_to_vector_interpolation_test_helper(
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
    fn test_e2e_vector_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 23, 48];
        let values_b = [9, 18, 0, 100];

        let mut inserter1 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "mobile"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"http_requests_total{service = "web"} + sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100)).unwrap();

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
    fn test_e2e_scalar_to_scalar() {
        set_up_dirs!(dirs, "db");
        let root_dir = dirs[0].clone();

        let mut conn = Connection::new(root_dir).unwrap();

        let timestamps = [23, 29, 40, 51];

        let values_a = [45, 47, 24, 48];
        let values_b = [9, 18, 0, 55];

        let mut inserter1 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "web"}"#,
            ValueType::UInteger64,
        );

        // Insert dummy data
        for (t, v) in zip(timestamps, values_a) {
            inserter1.insert(t, v.into());
        }

        inserter1.flush();

        let mut inserter2 = create_stream_helper(
            &mut conn,
            r#"http_requests_total{service = "mobile"}"#,
            ValueType::UInteger64,
        );

        for (t, v) in zip(timestamps, values_b) {
            inserter2.insert(t, v.into());
        }

        inserter2.flush();

        // Prepare test query
        let query = r#"sum(http_requests_total{service = "web"}) / sum(http_requests_total{service = "mobile"})"#;
        let mut stmt = conn.prepare_query(query, Some(0), Some(100)).unwrap();

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

    trait CreateAndInsertHelper {
        type InsertType: Into<Value>;
        const VALUE_TYPE: ValueType;

        fn help(
            conn: &mut Connection,
            stream: impl AsRef<str>,
            timestamps: Vec<Timestamp>,
            values: Vec<Self::InsertType>,
        ) -> Inserter {
            let mut inserter = create_stream_helper(conn, stream, Self::VALUE_TYPE);
            for (t, v) in zip(timestamps, values) {
                inserter.insert(t, v.into());
            }
            inserter.flush();

            inserter
        }
    }

    struct CreateAndInsertU64Helper;
    impl CreateAndInsertHelper for CreateAndInsertU64Helper {
        type InsertType = u64;
        const VALUE_TYPE: ValueType = ValueType::UInteger64;
    }

    struct CreateAndInsertI64Helper;
    impl CreateAndInsertHelper for CreateAndInsertI64Helper {
        type InsertType = i64;
        const VALUE_TYPE: ValueType = ValueType::Integer64;
    }

    struct CreateAndInsertF64Helper;
    impl CreateAndInsertHelper for CreateAndInsertF64Helper {
        type InsertType = f64;
        const VALUE_TYPE: ValueType = ValueType::Float64;
    }

    fn query_values_assert<T: Into<Value>>(
        mut query: Query,
        value_type: ValueType,
        mut values: Vec<T>,
    ) {
        assert_eq!(query.value_type(), value_type);

        values.reverse();

        loop {
            let result = query.next_vector();

            if result.is_none() {
                assert_eq!(values.len(), 0);
                break;
            }

            let v = result.unwrap().value;
            let value = values.pop().unwrap().into();

            assert!(v.eq_same(value_type, &value));
        }
    }

    #[test]
    fn test_e2e_all_value_types_read() {
        set_up_dirs!(db_dirs, "db");
        let db_dir = db_dirs[0].clone();

        let mut connection = Connection::new(db_dir).unwrap();

        let timestamps = vec![1, 2, 3, 4, 5];

        let u64s = vec![0u64, 5u64, 10u64, 15u64, 20u64];
        let i64s = vec![-10i64, -5i64, 0i64, 5i64, 10i64];
        let f64s = vec![
            -2.5e2,
            -std::f64::consts::PI,
            0.0f64,
            f64::MIN_POSITIVE,
            2395353.2352,
        ];

        const USTREAM: &str = r#"mystream{t="u"}"#;
        const ISTREAM: &str = r#"mystream{t="i"}"#;
        const FSTREAM: &str = r#"mystream{t="f"}"#;

        let _ = CreateAndInsertU64Helper::help(
            &mut connection,
            USTREAM,
            timestamps.clone(),
            u64s.clone(),
        );

        let uquery = connection
            .prepare_query(USTREAM, Some(0), Some(1000))
            .unwrap();
        query_values_assert(uquery, ValueType::UInteger64, u64s);

        let _ = CreateAndInsertI64Helper::help(
            &mut connection,
            ISTREAM,
            timestamps.clone(),
            i64s.clone(),
        );

        let iquery = connection
            .prepare_query(ISTREAM, Some(0), Some(1000))
            .unwrap();
        query_values_assert(iquery, ValueType::Integer64, i64s);

        let _ = CreateAndInsertF64Helper::help(
            &mut connection,
            FSTREAM,
            timestamps.clone(),
            f64s.clone(),
        );

        let fquery = connection
            .prepare_query(FSTREAM, Some(0), Some(1000))
            .unwrap();
        query_values_assert(fquery, ValueType::Float64, f64s);
    }

    #[test]
    fn test_e2e_i64_negative_values_misc_tests() {
        set_up_dirs!(db_dirs, "db");
        let db_dir = db_dirs[0].clone();

        let mut connection = Connection::new(db_dir).unwrap();

        let timestamps = vec![1, 2, 3, 4];
        let values = vec![-5i64, -7i64, -1i64, -1000i64];

        let _ = CreateAndInsertI64Helper::help(
            &mut connection,
            r#"mystream{t="i"}"#,
            timestamps,
            values.clone(),
        );

        {
            let iquery = connection
                .prepare_query(r#"mystream{t="i"}"#, Some(0), Some(1000))
                .unwrap();
            query_values_assert(iquery, ValueType::Integer64, values.clone());
        }

        {
            let mut topquery = connection
                .prepare_query(r#"topk(2, mystream{t="i"})"#, Some(0), Some(1000))
                .unwrap();
            assert_eq!(topquery.value_type(), ValueType::Integer64);
            assert_eq!(topquery.next_scalar().unwrap().get_integer64(), -1i64);
            assert_eq!(topquery.next_scalar().unwrap().get_integer64(), -5i64);
            assert!(topquery.next_scalar().is_none());
        }

        {
            let mut sumquery = connection
                .prepare_query(r#"sum(mystream{t="i"})"#, Some(0), Some(1000))
                .unwrap();
            assert_eq!(sumquery.value_type(), ValueType::Integer64);
            assert_eq!(
                sumquery.next_scalar().unwrap().get_integer64(),
                values.iter().sum::<i64>()
            );
            assert!(sumquery.next_scalar().is_none());
        }

        {
            let mut minquery = connection
                .prepare_query(r#"min(mystream{t="i"})"#, Some(0), Some(1000))
                .unwrap();
            assert_eq!(minquery.value_type(), ValueType::Integer64);
            assert_eq!(minquery.next_scalar().unwrap().get_integer64(), -1000i64);
            assert!(minquery.next_scalar().is_none());
        }
    }

    #[test]
    fn test_e2e_f64_values_misc_tests() {
        set_up_dirs!(db_dirs, "db");
        let db_dir = db_dirs[0].clone();

        let mut connection = Connection::new(db_dir).unwrap();

        let timestamps = vec![1, 2];
        let values = vec![3.8f64, -23.1f64];

        let _ = CreateAndInsertF64Helper::help(
            &mut connection,
            r#"mystream{t="f"}"#,
            timestamps,
            values.clone(),
        );

        {
            let fquery = connection
                .prepare_query(r#"mystream{t="f"}"#, Some(0), Some(1000))
                .unwrap();
            query_values_assert(fquery, ValueType::Float64, values.clone());
        }

        {
            let mut topquery = connection
                .prepare_query(r#"bottomk(1, mystream{t="f"})"#, Some(0), Some(1000))
                .unwrap();
            assert_eq!(topquery.value_type(), ValueType::Float64);
            assert_eq!(topquery.next_scalar().unwrap().get_float64(), -23.1f64);
            assert!(topquery.next_scalar().is_none());
        }

        {
            let mut avgquery = connection
                .prepare_query(r#"avg(mystream{t="f"})"#, Some(0), Some(1000))
                .unwrap();
            assert_eq!(avgquery.value_type(), ValueType::Float64);
            assert_eq!(
                avgquery.next_scalar().unwrap().get_float64(),
                (3.8f64 + -23.1f64) / 2.0f64
            );
            assert!(avgquery.next_scalar().is_none());
        }
    }
}
