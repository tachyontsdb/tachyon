pub struct FileReaderUtil;

impl FileReaderUtil {
    // Varint decoding
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
}
