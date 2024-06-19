use std::mem::size_of;

pub struct FileReaderUtil;

// TODO: Check this, changed
impl FileReaderUtil {
    pub fn read_u16(buffer: [u8; size_of::<u16>()]) -> u16 {
        u16::from_le_bytes(buffer)
    }

    pub fn read_u32(buffer: [u8; size_of::<u32>()]) -> u32 {
        u32::from_le_bytes(buffer)
    }

    pub fn read_u64(buffer: [u8; size_of::<u64>()]) -> u64 {
        u64::from_le_bytes(buffer)
    }

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
    pub fn read_u64_4(buf: &[u8]) -> u64 {
        ((buf[3] as u64) << 24) | ((buf[2] as u64) << 16) | ((buf[1] as u64) << 8) | (buf[0] as u64)
    }

    #[inline]
    pub fn read_u64_8(buf: &[u8]) -> u64 {
        let ret = u64::from_le_bytes(buf.try_into().unwrap())
            .try_into()
            .unwrap();
        ret
    }
}
