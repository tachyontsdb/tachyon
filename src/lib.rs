#![allow(unused)]

pub mod common;

use api::{Connection, Stmt, TachyonResultType};
use common::{TachyonValue, TachyonValueType, TachyonVector, Timestamp, Value};
use std::{path::PathBuf, ptr};

mod executor;
mod query;
pub mod storage;

mod utils;

pub mod api;

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_open(db_dir: *const core::ffi::c_char) -> *mut Connection {
    let ffi_str = core::ffi::CStr::from_ptr(db_dir);
    let root_dir = PathBuf::from(ffi_str.to_str().unwrap());

    let connection = Connection::new(root_dir);
    Box::into_raw(Box::new(connection))
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_close(connection: *mut Connection) {
    let connection = Box::from_raw(connection);
    drop(connection);
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_delete_stream(
    connection: *mut Connection,
    stream: *const core::ffi::c_char,
) {
    todo!();
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_insert(
    connection: *mut Connection,
    stream: *const core::ffi::c_char,
    timestamp: Timestamp,
    value_type: TachyonValueType,
    value: TachyonValue,
) {
    let ffi_str = core::ffi::CStr::from_ptr(stream);
    (*connection).insert(ffi_str.to_str().unwrap(), timestamp, value.unsigned_integer);
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_insert_flush(connection: *mut Connection) {
    (*connection).writer.flush_all();
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_statement_prepare(
    connection: *mut Connection,
    query: *const core::ffi::c_char,
    start: *const Timestamp,
    end: *const Timestamp,
    value_type: TachyonValueType,
) -> *mut Stmt {
    let ffi_str = core::ffi::CStr::from_ptr(query);
    let stmt = (*connection).prepare(
        ffi_str.to_str().unwrap(),
        if start.is_null() { None } else { Some(*start) },
        if end.is_null() { None } else { Some(*end) },
    );
    Box::into_raw(Box::new(stmt))
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_statement_close(statement: *mut Stmt) {
    let stmt = Box::from_raw(statement);
    drop(stmt);
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_get_scalar(
    statement: *mut Stmt,
    scalar: *mut TachyonValue,
) -> bool {
    let result = (*statement).next_scalar();
    match result {
        None => false,
        Some(value) => {
            *scalar = TachyonValue {
                unsigned_integer: value,
            };
            true
        }
    }
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_next_vector(
    statement: *mut Stmt,
    vector: *mut TachyonVector,
) -> bool {
    let result = (*statement).next_vector();
    match result {
        None => false,
        Some((timestamp, value)) => {
            *vector = TachyonVector {
                timestamp,
                value: TachyonValue {
                    unsigned_integer: value,
                },
            };
            true
        }
    }
}
