pub type Timestamp = u64;
pub type Value = u64;

#[repr(u8)]
pub enum TachyonValueType {
    UnsignedInteger,
    SignedInteger,
    Float,
}

#[repr(C)]
pub union TachyonValue {
    pub unsigned_integer: u64,
    pub signed_integer: i64,
    pub floating: f64,
}

#[repr(C)]
pub struct TachyonVector {
    pub timestamp: u64,
    pub value: TachyonValue,
}
