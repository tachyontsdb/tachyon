#![allow(unused)]

pub mod common;

use api::{Connection, Stmt, TachyonResult, TachyonResultType, TachyonResultUnion, VectorResult};
use common::{Timestamp, Value};
use std::path::PathBuf;

mod executor;
mod query;
pub mod storage;

mod utils;

pub mod api;

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_open(root_dir: *const core::ffi::c_char) -> *mut Connection {
    let ffi_str = core::ffi::CStr::from_ptr(root_dir);
    let root_dir = PathBuf::from(ffi_str.to_str().unwrap());

    todo!("Need to create dir for db if not exists");

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
pub unsafe extern "C" fn tachyon_prepare(
    connection: *mut Connection,
    str_ptr: *const core::ffi::c_char,
    start: *const Timestamp,
    end: *const Timestamp,
) -> *mut Stmt {
    let ffi_str = core::ffi::CStr::from_ptr(str_ptr);
    let stmt = (*connection).prepare(
        ffi_str.to_str().unwrap(),
        if start.is_null() { None } else { Some(*start) },
        if end.is_null() { None } else { Some(*end) },
    );
    Box::into_raw(Box::new(stmt))
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_next_vector(stmt: *mut Stmt) -> TachyonResult {
    let result = unsafe { (*stmt).next_vector() };
    match result {
        None => TachyonResult {
            t: TachyonResultType::Done,
            r: TachyonResultUnion { scalar: 0 },
        },
        Some((timestamp, value)) => TachyonResult {
            t: TachyonResultType::Vector,
            r: TachyonResultUnion {
                vector: VectorResult { timestamp, value },
            },
        },
    }
}

/// # Safety
#[no_mangle]
pub unsafe extern "C" fn tachyon_insert(
    connection: *mut Connection,
    str_ptr: *const core::ffi::c_char,
    timestamp: Timestamp,
    value: Value,
) {
    let ffi_str = core::ffi::CStr::from_ptr(str_ptr);
    (*connection).insert(ffi_str.to_str().unwrap(), timestamp, value);
}
