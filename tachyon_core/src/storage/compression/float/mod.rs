use std::io::Write;

mod v1;

#[allow(clippy::large_enum_variant)]
pub enum FloatCompressor<W: Write> {
    V1(v1::CompressionEngineV1<W>),
}
