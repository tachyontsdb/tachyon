use std::io::{Read, Write};

use crate::{storage::file::Header, Timestamp};

use super::{CompressionEngine, DecompressionEngine};

mod google;
#[deprecated]
mod v1;
mod v2;

pub(super) struct IntCompressionUtils;
impl IntCompressionUtils {
    #[inline]
    pub fn zig_zag_decode(n: u64) -> i64 {
        ((n >> 1) as i64) ^ -((n & 1) as i64)
    }

    #[inline]
    pub fn zig_zag_encode(n: i64) -> u64 {
        ((n >> (i64::BITS as usize - 1)) ^ (n << 1)) as u64
    }
}

// TODO: Make macro to generate the enum + implementation
#[allow(clippy::large_enum_variant)]
#[allow(deprecated)]
pub enum IntDecompressor<R: Read> {
    V1(v1::DecompressionEngineV1<R>),
    V2(v2::DecompressionEngineV2<R>),
}

impl<R: Read> DecompressionEngine<R> for IntDecompressor<R> {
    type PhysicalType = u64;

    fn new(reader: R, header: &Header) -> Self {
        Self::V2(v2::DecompressionEngineV2::new(reader, header))
    }

    fn next(&mut self) -> (Timestamp, u64) {
        match self {
            Self::V1(engine) => engine.next(),
            Self::V2(engine) => engine.next(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[allow(deprecated)]
pub enum IntCompressor<W: Write> {
    V1(v1::CompressionEngineV1<W>),
    V2(v2::CompressionEngineV2<W>),
}

impl<W: Write> CompressionEngine<W> for IntCompressor<W> {
    type PhysicalType = u64;

    fn new(writer: W, header: &Header) -> Self {
        Self::V2(v2::CompressionEngineV2::new(writer, header))
    }

    fn consume(&mut self, timestamp: Timestamp, value: Self::PhysicalType) -> usize {
        match self {
            Self::V1(engine) => engine.consume(timestamp, value),
            Self::V2(engine) => engine.consume(timestamp, value),
        }
    }

    fn flush_all(&mut self) -> usize {
        match self {
            Self::V1(engine) => engine.flush_all(),
            Self::V2(engine) => engine.flush_all(),
        }
    }
}

#[test]
fn test_zig_zag() {
    assert_eq!(0, IntCompressionUtils::zig_zag_encode(0));
    assert_eq!(1, IntCompressionUtils::zig_zag_encode(-1));
    assert_eq!(2, IntCompressionUtils::zig_zag_encode(1));
    assert_eq!(3, IntCompressionUtils::zig_zag_encode(-2));
    assert_eq!(4, IntCompressionUtils::zig_zag_encode(2));
    assert_eq!(379, IntCompressionUtils::zig_zag_encode(-190));
    assert_eq!(80, IntCompressionUtils::zig_zag_encode(40));
    assert_eq!(254, IntCompressionUtils::zig_zag_encode(127));
    assert_eq!(256, IntCompressionUtils::zig_zag_encode(128));

    assert_eq!(0, IntCompressionUtils::zig_zag_decode(0));
    assert_eq!(-1, IntCompressionUtils::zig_zag_decode(1));
    assert_eq!(1, IntCompressionUtils::zig_zag_decode(2));
    assert_eq!(-2, IntCompressionUtils::zig_zag_decode(3));
    assert_eq!(2, IntCompressionUtils::zig_zag_decode(4));
    assert_eq!(-5, IntCompressionUtils::zig_zag_decode(9));
    assert_eq!(-18, IntCompressionUtils::zig_zag_decode(35));

    assert_eq!(
        64,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(64))
    );

    assert_eq!(
        0,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(0))
    );

    assert_eq!(
        -17,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(-17))
    );

    assert_eq!(
        -12,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(-12))
    );

    assert_eq!(
        130,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(130))
    );

    assert_eq!(
        (i32::MAX) as i64,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode((i32::MAX) as i64))
    );

    assert_eq!(
        i32::MAX as i64 + 3,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(
            (i32::MAX as i64) + 3
        ))
    );

    assert_eq!(
        i64::MAX,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(i64::MAX))
    );

    assert_eq!(
        i64::MIN,
        IntCompressionUtils::zig_zag_decode(IntCompressionUtils::zig_zag_encode(i64::MIN))
    );
}
