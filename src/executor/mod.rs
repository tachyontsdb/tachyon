#[non_exhaustive]
#[repr(u8)]
pub enum OperationCode {
    NoOperation,

    Init,
    Halt,

    OpenReadCursor, // { register_index: u64, stream_id: u64, from_timestamp: u64, to_timestamp: u64 } returns cursor,
    CloseReadCursor, // { cursor: u64 },

    GoNextEntry, // { cursor: u64 },
    GoPrevEntry, // { cursor: u64 },

    GoGreaterTimestampEntry, // Seek { cursor: u64, timestamp: u64 }, (might not need)

    FetchVectorDiscardTimestamp, // { cursor: u64, to_register: u64 }
    FetchVector, // { cursor: u64, to_register_timestamp: u64, to_register_value: u64 },

    Goto,    // { address: u64 }
    GotoEq,  // { address: u64, register_1: u64, register_2: u64 }
    GotoNeq, // { address: u64, register_1: u64, register_2: u64 }

    AddToOutputScalar, // { from_register: u64 }
    AddToOutputVector, // { from_register_timestamp: u64, from_register_value: u64 }

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

#[repr(C, packed)]
pub struct Context {
    pub pc: u64,
    pub regs: [u64; 8],
}
