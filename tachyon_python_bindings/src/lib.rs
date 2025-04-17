#![allow(clippy::arc_with_non_send_sync)]

use pyo3::{exceptions::PyRuntimeError, prelude::*};
use std::sync::{Arc, Mutex};
use tachyon_core::{
    error::TachyonErr, Connection, Inserter, QueryUtils, QueryUtilsResultData, ReturnType,
    Timestamp, ValueType,
};

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
            WrappedTachyonErr::MiscErr(msg) => PyErr::new::<PyRuntimeError, _>(msg),
            WrappedTachyonErr::TachyonErr(err) => PyErr::new::<PyRuntimeError, _>(err.to_string()),
        }
    }
}

#[pyclass(name = "ValueType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum WrappedValueType {
    SignedInteger,
    UnsignedInteger,
    Float,
}

impl From<ValueType> for WrappedValueType {
    fn from(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Integer64 => Self::SignedInteger,
            ValueType::UInteger64 => Self::UnsignedInteger,
            ValueType::Float64 => Self::Float,
        }
    }
}

impl From<WrappedValueType> for ValueType {
    fn from(wrapped_value_type: WrappedValueType) -> Self {
        match wrapped_value_type {
            WrappedValueType::SignedInteger => Self::Integer64,
            WrappedValueType::UnsignedInteger => Self::UInteger64,
            WrappedValueType::Float => Self::Float64,
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

// SAFETY: TODO
unsafe impl Send for WrappedInserter {}

// SAFETY: TODO
unsafe impl Sync for WrappedInserter {}

#[pymethods]
impl WrappedInserter {
    pub fn get_value_type(&self) -> PyResult<WrappedValueType> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let inserter = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        Ok(inserter.value_type().into())
    }

    pub fn flush(&self) -> PyResult<()> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut inserter = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        inserter.flush().map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_signed_integer(&self, timestamp: Timestamp, value: i64) -> PyResult<()> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut inserter = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        inserter
            .insert_integer64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_unsigned_integer(&self, timestamp: Timestamp, value: u64) -> PyResult<()> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut inserter = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        inserter
            .insert_uinteger64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn insert_float(&self, timestamp: Timestamp, value: f64) -> PyResult<()> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut inserter = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        inserter
            .insert_float64(timestamp, value)
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }
}

#[pyclass(name = "QueryResultData")]
#[derive(Clone)]
pub enum WrappedQueryResultData {
    SignedInteger(Vec<i64>),
    UnsignedInteger(Vec<u64>),
    Float(Vec<f64>),
}

impl From<QueryUtilsResultData> for WrappedQueryResultData {
    fn from(data: QueryUtilsResultData) -> Self {
        match data {
            QueryUtilsResultData::Integer64(values) => Self::SignedInteger(values),
            QueryUtilsResultData::UInteger64(values) => Self::UnsignedInteger(values),
            QueryUtilsResultData::Float64(values) => Self::Float(values),
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

// SAFETY: TODO
unsafe impl Send for WrappedConnection {}

// SAFETY: TODO
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
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut conn = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        conn.create_stream(name, value_type.into())
            .map_err(WrappedTachyonErr::TachyonErr)?;
        Ok(())
    }

    pub fn delete_stream(&self, name: String) -> PyResult<()> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut conn = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        conn.delete_stream(name);
        Ok(())
    }

    pub fn check_stream_exists(&self, name: String) -> PyResult<bool> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let conn = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        Ok(conn
            .check_stream_exists(name)
            .map_err(WrappedTachyonErr::TachyonErr)?)
    }

    pub fn create_inserter(&self, stream: String) -> PyResult<WrappedInserter> {
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut conn = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
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
        let _dummy_lock = self
            .dummy_lock
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
        let mut conn = self
            .ptr
            .lock()
            .map_err(|_| WrappedTachyonErr::MiscErr(String::from("Failed to acquire lock!")))?;
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
fn tachyon_python_bindings(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<WrappedValueType>()?;
    m.add_class::<WrappedReturnType>()?;

    m.add_class::<WrappedInserter>()?;
    m.add_class::<WrappedQueryResultData>()?;
    m.add_class::<WrappedQueryResult>()?;
    m.add_class::<WrappedConnection>()?;

    Ok(())
}
