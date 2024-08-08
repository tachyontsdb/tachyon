use crate::{Connection, Inserter, Query, ReturnType, Timestamp, Value, ValueType, Vector};
use std::ffi::{c_char, CStr};

#[no_mangle]
pub unsafe extern "C" fn tachyon_open(db_dir: *const c_char) -> *mut Connection {
    let db_dir = CStr::from_ptr(db_dir).to_str().unwrap();
    let connection = Connection::new(db_dir);
    Box::into_raw(Box::new(connection))
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_close(connection: *mut Connection) {
    let connection = Box::from_raw(connection);
    drop(connection);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_stream_create(
    connection: *mut Connection,
    stream: *const c_char,
    value_type: ValueType,
) {
    let stream = CStr::from_ptr(stream).to_str().unwrap();
    (*connection).create_stream(stream, value_type);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_stream_delete(connection: *mut Connection, stream: *const c_char) {
    let stream = CStr::from_ptr(stream).to_str().unwrap();
    (*connection).delete_stream(stream);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_stream_check_exists(
    connection: *const Connection,
    stream: *const c_char,
) -> bool {
    let stream = CStr::from_ptr(stream).to_str().unwrap();
    (*connection).check_stream_exists(stream)
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_create(
    connection: *mut Connection,
    stream: *const c_char,
) -> *mut Inserter {
    let stream = CStr::from_ptr(stream).to_str().unwrap();
    let inserter = (*connection).prepare_insert(stream);
    Box::into_raw(Box::new(inserter))
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_close(inserter: *mut Inserter) {
    let inserter = Box::from_raw(inserter);
    drop(inserter);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_value_type(inserter: *const Inserter) -> ValueType {
    (*inserter).value_type()
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_insert_integer64(
    inserter: *mut Inserter,
    timestamp: Timestamp,
    value: i64,
) {
    (*inserter).insert_integer64(timestamp, value);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_insert_uinteger64(
    inserter: *mut Inserter,
    timestamp: Timestamp,
    value: u64,
) {
    (*inserter).insert_uinteger64(timestamp, value);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_insert_float64(
    inserter: *mut Inserter,
    timestamp: Timestamp,
    value: f64,
) {
    (*inserter).insert_float64(timestamp, value);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_inserter_flush(inserter: *mut Inserter) {
    (*inserter).flush();
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_create(
    connection: *mut Connection,
    query: *const c_char,
    start: *const Timestamp,
    end: *const Timestamp,
) -> *mut Query {
    let query = CStr::from_ptr(query).to_str().unwrap();
    let query = (*connection).prepare_query(
        query,
        if start.is_null() { None } else { Some(*start) },
        if end.is_null() { None } else { Some(*end) },
    );
    Box::into_raw(Box::new(query))
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_close(query: *mut Query) {
    let query = Box::from_raw(query);
    drop(query);
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_value_type(query: *const Query) -> ValueType {
    (*query).value_type()
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_return_type(query: *const Query) -> ReturnType {
    (*query).return_type()
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_next_scalar(query: *mut Query, scalar: *mut Value) -> bool {
    let result = (*query).next_scalar();
    match result {
        None => false,
        Some(result) => {
            *scalar = result;
            true
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_query_next_vector(query: *mut Query, vector: *mut Vector) -> bool {
    let result = (*query).next_vector();
    match result {
        None => false,
        Some(result) => {
            *vector = result;
            true
        }
    }
}
