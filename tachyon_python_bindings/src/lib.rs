#![allow(clippy::arc_with_non_send_sync)]

use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::{Arc, Mutex};
use tachyon_core::{
    error::TachyonErr, Connection, Inserter, QueryUtils, QueryUtilsResultData, ReturnType,
    Timestamp, ValueType,
};

macro_rules! dummy_lock {
    ($self: ident) => {
        let _dummy_lock = $self.dummy_lock.lock().map_err(|_| {
            crate::WrappedTachyonErr::MiscErr(::std::string::String::from(
                "Failed to acquire lock!",
            ))
        })?;
    };
}

macro_rules! ptr_lock {
    ($self: ident) => {
        $self.ptr.lock().map_err(|_| {
            crate::WrappedTachyonErr::MiscErr(::std::string::String::from(
                "Failed to acquire lock!",
            ))
        })?
    };
}

#[derive(Debug)]
pub enum WrappedTachyonErr {
    MiscErr(String),
    TachyonErr(TachyonErr),
}

impl From<TachyonErr> for WrappedTachyonErr {
    fn from(err: TachyonErr) -> Self {
        Self::TachyonErr(err)
    }
}

impl From<WrappedTachyonErr> for PyErr {
    fn from(err: WrappedTachyonErr) -> Self {
        match err {
            WrappedTachyonErr::MiscErr(msg) => Self::new::<PyRuntimeError, _>(msg),
            WrappedTachyonErr::TachyonErr(err) => Self::new::<PyRuntimeError, _>(err.to_string()),
        }
    }
}

#[pyclass(name = "ValueType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum WrappedValueType {
    Integer64,
    UInteger64,
    Float64,
}

impl From<ValueType> for WrappedValueType {
    fn from(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Integer64 => Self::Integer64,
            ValueType::UInteger64 => Self::UInteger64,
            ValueType::Float64 => Self::Float64,
        }
    }
}

impl From<WrappedValueType> for ValueType {
    fn from(wrapped_value_type: WrappedValueType) -> Self {
        match wrapped_value_type {
            WrappedValueType::Integer64 => Self::Integer64,
            WrappedValueType::UInteger64 => Self::UInteger64,
            WrappedValueType::Float64 => Self::Float64,
        }
    }
}

#[pyclass(name = "ReturnType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum WrappedReturnType {
    Scalar,
    Vector,
}

impl From<ReturnType> for WrappedReturnType {
    fn from(return_type: ReturnType) -> Self {
        match return_type {
            ReturnType::Scalar => Self::Scalar,
            ReturnType::Vector => Self::Vector,
        }
    }
}

impl From<WrappedReturnType> for ReturnType {
    fn from(wrapped_return_type: WrappedReturnType) -> Self {
        match wrapped_return_type {
            WrappedReturnType::Scalar => Self::Scalar,
            WrappedReturnType::Vector => Self::Vector,
        }
    }
}

#[pyclass(name = "Inserter")]
pub struct WrappedInserter {
    dummy_lock: Arc<Mutex<()>>,
    ptr: Arc<Mutex<Inserter>>,
}

// SAFETY: The `dummy_lock` is used to ensure access is thread-safe
unsafe impl Send for WrappedInserter {}

// SAFETY: The `dummy_lock` is used to ensure access is thread-safe
unsafe impl Sync for WrappedInserter {}

#[pymethods]
impl WrappedInserter {
    pub fn value_type(&self) -> PyResult<WrappedValueType> {
        dummy_lock!(self);
        let inserter = ptr_lock!(self);
        Ok(inserter.value_type().into())
    }

    pub fn flush(&self) -> PyResult<()> {
        dummy_lock!(self);
        let mut inserter = ptr_lock!(self);
        inserter.flush().map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_integer64(&self, timestamp: Timestamp, value: i64) -> PyResult<()> {
        dummy_lock!(self);
        let mut inserter = ptr_lock!(self);
        inserter
            .insert_integer64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_uinteger64(&self, timestamp: Timestamp, value: u64) -> PyResult<()> {
        dummy_lock!(self);
        let mut inserter = ptr_lock!(self);
        inserter
            .insert_uinteger64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_float64(&self, timestamp: Timestamp, value: f64) -> PyResult<()> {
        dummy_lock!(self);
        let mut inserter = ptr_lock!(self);
        inserter
            .insert_float64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }
}

#[pyclass(name = "QueryResultData")]
#[derive(Clone)]
pub enum WrappedQueryResultData {
    Integer64(Vec<i64>),
    UInteger64(Vec<u64>),
    Float64(Vec<f64>),
}

impl From<QueryUtilsResultData> for WrappedQueryResultData {
    fn from(data: QueryUtilsResultData) -> Self {
        match data {
            QueryUtilsResultData::Integer64(values) => Self::Integer64(values),
            QueryUtilsResultData::UInteger64(values) => Self::UInteger64(values),
            QueryUtilsResultData::Float64(values) => Self::Float64(values),
        }
    }
}

#[pyclass(name = "QueryResult")]
pub struct WrappedQueryResult {
    #[pyo3(get)]
    pub return_type: WrappedReturnType,
    #[pyo3(get)]
    pub value_type: WrappedValueType,
    timestamps: Option<Vec<Timestamp>>,
    data: WrappedQueryResultData,
}

#[pymethods]
impl WrappedQueryResult {
    pub fn get(&self) -> PyResult<(Option<Vec<Timestamp>>, WrappedQueryResultData)> {
        // TODO: Make this more efficient
        Ok((self.timestamps.clone(), self.data.clone()))
    }
}

/// SAFETY: A connection is only single-threaded
#[pyclass(name = "Connection")]
pub struct WrappedConnection {
    dummy_lock: Arc<Mutex<()>>,
    ptr: Arc<Mutex<Connection>>,
}

// SAFETY: The `dummy_lock` is used to ensure access is thread-safe
unsafe impl Send for WrappedConnection {}

// SAFETY: The `dummy_lock` is used to ensure access is thread-safe
unsafe impl Sync for WrappedConnection {}

#[pymethods]
impl WrappedConnection {
    #[new]
    pub fn new(path: String) -> PyResult<Self> {
        Ok(Self {
            dummy_lock: Arc::new(Mutex::new(())),
            ptr: Arc::new(Mutex::new(
                Connection::new(path).map_err(WrappedTachyonErr::TachyonErr)?,
            )),
        })
    }

    pub fn create_stream(&self, name: String, value_type: WrappedValueType) -> PyResult<()> {
        dummy_lock!(self);
        let mut conn = ptr_lock!(self);
        conn.create_stream(name, value_type.into())
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn delete_stream(&self, name: String) -> PyResult<()> {
        dummy_lock!(self);
        let mut conn = ptr_lock!(self);
        conn.delete_stream(name);
        Ok(())
    }

    pub fn check_stream_exists(&self, name: String) -> PyResult<bool> {
        dummy_lock!(self);
        let conn = ptr_lock!(self);
        Ok(conn
            .check_stream_exists(name)
            .map_err(WrappedTachyonErr::TachyonErr)?)
    }

    pub fn prepare_insert(&self, stream: String) -> PyResult<WrappedInserter> {
        dummy_lock!(self);
        let mut conn = ptr_lock!(self);
        let inserter = conn
            .prepare_insert(stream)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(WrappedInserter {
            dummy_lock: Arc::new(Mutex::new(())),
            ptr: Arc::new(Mutex::new(inserter)),
        })
    }

    pub fn query(
        &self,
        query: String,
        start: Option<Timestamp>,
        end: Option<Timestamp>,
    ) -> PyResult<WrappedQueryResult> {
        dummy_lock!(self);
        let mut conn = ptr_lock!(self);
        let (return_type, value_type, timestamps, values) =
            QueryUtils::perform(&mut conn, query, start, end)
                .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(WrappedQueryResult {
            return_type: return_type.into(),
            value_type: value_type.into(),
            timestamps,
            data: WrappedQueryResultData::from(values),
        })
    }
}

/// The TachyonDB Python bindings.
/// This module provides a Python interface to a TachyonDB database.
#[pymodule]
fn tachyondb(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<WrappedValueType>()?;
    m.add_class::<WrappedReturnType>()?;

    m.add_class::<WrappedInserter>()?;
    m.add_class::<WrappedQueryResultData>()?;
    m.add_class::<WrappedQueryResult>()?;
    m.add_class::<WrappedConnection>()?;

    Ok(())
}
