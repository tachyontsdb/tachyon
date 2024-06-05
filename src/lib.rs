#[cfg(target_endian = "big")]
const _: () = assert!(false, "Big endian not supported!");

mod executor;
mod storage;

mod common;
