use crate::{common::Timestamp, storage::file::Cursor};
use std::{path::PathBuf, sync::Arc};

#[non_exhaustive]
#[repr(u8)]
pub enum OperationCode {
    NoOperation,

    Init,
    Halt,

    OpenRead, // { register_index: u64, stream_id: u64, from_timestamp: u64, to_timestamp: u64 } returns cursor,
    CloseRead, // { cursor: u64 },

    Next, // { cursor: u64 },

    // GoPrevEntry, // { cursor: u64 },
    // GoGreaterTimestampEntry, // Seek { cursor: u64, timestamp: u64 }, (might not need)
    FetchScalar, // { cursor: u64, to_register: u64 }
    FetchVector, // { cursor: u64, to_register_timestamp: u64, to_register_value: u64 },

    Goto,    // { address: u64 }
    GotoEq,  // { address: u64, register_1: u64, register_2: u64 }
    GotoNeq, // { address: u64, register_1: u64, register_2: u64 }

    OutputScalar, // { from_register: u64 }
    OutputVector, // { from_register_timestamp: u64, from_register_value: u64 }

    LogicalNot,        // { to_register: u64, from_register: u64 }
    LogicalAnd,        // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    LogicalOr,         // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    LogicalXor,        // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    LogicalShiftLeft,  // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    LogicalShiftRight, // { to_register: u64, from_register_1: u64, from_register_2: u64 }

    ArithmeticAdd, // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    ArithmeticAddImmediate, // { to_register: u64, from_register: u64, immediate_value: u64 }
    ArithmeticSubtract, // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    ArithmeticSubtractImmediate, // { to_register: u64, from_register: u64, immediate_value: u64 }
    ArithmeticMultiply, // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    ArithmeticMultiplyImmediate, // { to_register: u64, from_register: u64, immediate_value: u64 }
    ArithmeticDivide, // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    ArithmeticDivideImmediate, // { to_register: u64, from_register: u64, immediate_value: u64 }
    ArithmeticRemainder, // { to_register: u64, from_register_1: u64, from_register_2: u64 }
    ArithmeticRemainderImmediate, // { to_register: u64, from_register: u64, immediate_value: u64 }
}

const NUM_REGS: usize = 10;

#[repr(C, packed)]
pub struct Context {
    pc: u64,
    regs: [u64; NUM_REGS],
    cursor: Cursor,
    file_paths: Arc<[PathBuf]>,
}

impl Context {
    pub fn new(file_paths: Arc<[PathBuf]>, start: Timestamp, end: Timestamp) -> Self {
        Self {
            pc: 0,
            regs: [0x00u64; NUM_REGS],
            cursor: Cursor::new(file_paths.clone(), start, end).unwrap(),
            file_paths,
        }
    }
}
