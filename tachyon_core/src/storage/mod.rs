mod compression;
mod hash_map;

pub mod file;
pub mod page_cache;
pub mod writer;

/// Varint decoding
pub struct FileReaderUtils;

impl FileReaderUtils {
    #[inline]
    pub fn read_u64_1(buf: &[u8]) -> u64 {
        buf[0] as u64
    }

    #[inline]
    pub fn read_u64_2(buf: &[u8]) -> u64 {
        ((buf[1] as u64) << 8) | (buf[0] as u64)
    }

    #[inline]
    pub fn read_u64_3(buf: &[u8]) -> u64 {
        ((buf[2] as u64) << 16) | ((buf[1] as u64) << 8) | (buf[0] as u64)
    }

    #[inline]
    pub fn read_u64_4(buf: &[u8]) -> u64 {
        ((buf[3] as u64) << 24) | ((buf[2] as u64) << 16) | ((buf[1] as u64) << 8) | (buf[0] as u64)
    }

    #[inline]
    pub fn read_u64_8(buf: &[u8]) -> u64 {
        u64::from_le_bytes(buf.try_into().unwrap())
    }

    #[inline]
    pub fn read_i64_8(buf: &[u8]) -> i64 {
        i64::from_le_bytes(buf.try_into().unwrap())
    }

    #[inline]
    pub fn read_f64_8(buf: &[u8]) -> f64 {
        f64::from_le_bytes(buf.try_into().unwrap())
    }
}
