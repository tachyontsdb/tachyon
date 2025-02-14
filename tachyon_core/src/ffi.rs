use crate::{
    print_error, Connection, Inserter, Query, ReturnType, TachyonErr, Timestamp, Value, ValueType,
    Vector,
};
use std::ffi::{c_char, c_void, CStr};

fn get_error_code(error: &TachyonErr) -> u8 {
    match error {
        TachyonErr::TyErr => 1,
        TachyonErr::InputErr { .. } => 2,
        TachyonErr::DatabaseCreationErr { .. } => 3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_error_print(code: u8, ptr: *const c_void) {
    if let 1..=3 = code {
        let error_ptr = ptr as *const TachyonErr;
        print_error(&error_ptr.read());
    }
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_error_free(code: u8, ptr: *mut c_void) {
    if let 1..=3 = code {
        drop(Box::from_raw(ptr as *mut TachyonErr));
    }
}

/// SAFETY: On success (code 0), this returns a `Connection *` in the `out` parameter. Otherwise, it returns an error.
/// The caller is responsible for freeing the returned pointer in `out`.
/// Success data can be freed by using the function `tachyon_close`.
/// Error data can be freed by using the function `tachyon_error_free`.
#[no_mangle]
pub unsafe extern "C" fn tachyon_open(db_dir: *const c_char, out: *mut *mut c_void) -> u8 {
    let db_dir_res = CStr::from_ptr(db_dir)
        .to_str()
        .map_err(|err| TachyonErr::InputErr {
            reason: Box::new(err),
        });

    match db_dir_res {
        Ok(db_dir) => match Connection::new(db_dir) {
            Ok(connection) => {
                *out = Box::into_raw(Box::new(connection)) as *mut c_void;
                0u8
            }
            Err(err) => {
                let tachyon_err = TachyonErr::from(err);
                let return_value = get_error_code(&tachyon_err);
                *out = Box::into_raw(Box::new(tachyon_err)) as *mut c_void;
                return_value
            }
        },
        Err(tachyon_err) => {
            let return_value = get_error_code(&tachyon_err);
            *out = Box::into_raw(Box::new(tachyon_err)) as *mut c_void;
            return_value
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tachyon_close(connection: *mut Connection) {
    let connection = Box::from_raw(connection);
    drop(connection);
}

/// SAFETY: On error (not code 0), this returns an error in the `out` parameter.
/// The caller is responsible for freeing the returned pointer in `out`.
/// Error data can be freed by using the function `tachyon_error_free`.
#[no_mangle]
pub unsafe extern "C" fn tachyon_stream_create(
    connection: *mut Connection,
    stream: *const c_char,
    value_type: ValueType,
    out: *mut *mut c_void,
) -> u8 {
    let stream_res = CStr::from_ptr(stream)
        .to_str()
        .map_err(|err| TachyonErr::InputErr {
            reason: Box::new(err),
        });

    match stream_res {
        Ok(stream) => match (*connection).create_stream(stream, value_type) {
            Ok(()) => 0u8,
            Err(err) => {
                let tachyon_err = TachyonErr::from(err);
                let return_value = get_error_code(&tachyon_err);
                *out = Box::into_raw(Box::new(tachyon_err)) as *mut c_void;
                return_value
            }
        },
        Err(tachyon_err) => {
            let return_value = get_error_code(&tachyon_err);
            *out = Box::into_raw(Box::new(tachyon_err)) as *mut c_void;
            return_value
        }
    }
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

/// SAFETY: The caller is responsible for freeing the returned pointer by using the function `tachyon_inserter_close`.
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

/// SAFETY: The caller is responsible for freeing the returned pointer by using the function `tachyon_query_close`.
#[no_mangle]
pub unsafe extern "C" fn tachyon_query_create<'a>(
    connection: *mut Connection,
    query: *const c_char,
    start: *const Timestamp,
    end: *const Timestamp,
) -> *mut Query<'a> {
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
