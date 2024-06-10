use crate::{
    common::{Timestamp, Value},
    storage::{file::Cursor, page_cache::PageCache},
};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
};

#[non_exhaustive]
#[repr(u8)]
pub enum OperationCode {
    NoOperation,

    Init,
    Halt,

    OpenRead, // { cursor: u64, file_paths_array_idx: u64, start: u64, end: u64 } (returns cursor),
    CloseRead, // { cursor: u64 },

    Next, // { cursor: u64 },

    // GoPrevEntry, // { cursor: u64 },
    // GoGreaterTimestampEntry, // Seek { cursor: u64, timestamp: u64 }, (might not need)
    // FetchScalar, // { cursor: u64, to_register: u64 }
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

pub enum OutputValue {
    Scalar(Value),
    Vector((Timestamp, Value)),
}

pub struct Context<'a> {
    pc: usize,
    regs: [u64; NUM_REGS],
    file_paths_array: Arc<[Arc<[PathBuf]>]>,
    cursors: HashMap<usize, Cursor<'a>>,
    outputs: VecDeque<OutputValue>,
    page_cache: &'a mut PageCache,
}

impl<'a> Context<'a> {
    pub fn new(file_paths_array: Arc<[Arc<[PathBuf]>]>, page_cache: &'a mut PageCache) -> Self {
        Self {
            pc: 0,
            regs: [0x00u64; NUM_REGS],
            file_paths_array,
            cursors: HashMap::new(),
            outputs: VecDeque::new(),
            page_cache: page_cache,
        }
    }

    fn open_read(&mut self, cursor_idx: u64, file_paths_array_idx: u64, start: u64, end: u64) {
        if self.cursors.contains_key(&(cursor_idx as usize)) {
            panic!("Cursor key already used!");
        }

        // let cursor = Cursor::<'a>::new(
        //     self.file_paths_array[file_paths_array_idx as usize].clone(),
        //     start,
        //     end,
        //     self.page_cache,
        // )
        // .unwrap();
        // self.cursors.insert(cursor_idx as usize, cursor);
        todo!()
    }

    fn close_read(&mut self, cursor_idx: u64) {
        self.cursors.remove(&(cursor_idx as usize));
    }

    fn next(&mut self, cursor_idx: u64) {
        self.cursors.get_mut(&(cursor_idx as usize)).unwrap().next();
    }

    fn fetch_vector(&mut self, cursor_idx: u64) -> (Timestamp, Value) {
        self.cursors[&(cursor_idx as usize)].fetch()
    }
}

pub fn execute(mut context: Context, buffer: &[u8]) {
    while context.pc < buffer.len() {
        let opcode: OperationCode = unsafe { std::mem::transmute(buffer[context.pc]) };
        context.pc += 1;

        match opcode {
            OperationCode::NoOperation => {}

            OperationCode::Init => {}
            OperationCode::Halt => {
                return;
            }

            OperationCode::OpenRead => {
                let cursor_idx =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let file_paths_array_idx =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let start = Timestamp::from_le_bytes(
                    buffer[context.pc..(context.pc + 8)].try_into().unwrap(),
                );
                context.pc += 8;
                let end = Timestamp::from_le_bytes(
                    buffer[context.pc..(context.pc + 8)].try_into().unwrap(),
                );
                context.pc += 8;

                context.open_read(cursor_idx, file_paths_array_idx, start, end);
            }
            OperationCode::CloseRead => {
                let cursor_idx =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                context.close_read(cursor_idx);
            }

            OperationCode::Next => {
                let cursor_idx =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                context.next(cursor_idx);
            }
            OperationCode::FetchVector => {
                let cursor_idx =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let to_register_timestamp =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let to_register_value = Timestamp::from_le_bytes(
                    buffer[context.pc..(context.pc + 8)].try_into().unwrap(),
                );
                context.pc += 8;

                let (timestamp, value) = context.fetch_vector(cursor_idx);
                context.regs[to_register_timestamp as usize] = timestamp;
                context.regs[to_register_value as usize] = value;
            }

            OperationCode::Goto => {
                let address =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                context.pc = address as usize;
            }
            OperationCode::GotoEq => {
                let address =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let register1 =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let register2 =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                let value1 = context.regs[register1 as usize];
                let value2 = context.regs[register2 as usize];
                if value1 == value2 {
                    context.pc = address as usize;
                }
            }
            OperationCode::GotoNeq => {
                let address =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let register1 =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let register2 =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                let value1 = context.regs[register1 as usize];
                let value2 = context.regs[register2 as usize];
                if value1 != value2 {
                    context.pc = address as usize;
                }
            }

            OperationCode::OutputScalar => {
                let from_register =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                context
                    .outputs
                    .push_back(OutputValue::Scalar(context.regs[from_register as usize]));
            }
            OperationCode::OutputVector => {
                let from_register_timestamp =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;
                let from_register_value =
                    u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
                context.pc += 8;

                context.outputs.push_back(OutputValue::Vector((
                    context.regs[from_register_timestamp as usize],
                    context.regs[from_register_value as usize],
                )));
            }

            OperationCode::LogicalNot => todo!(),
            OperationCode::LogicalAnd => todo!(),
            OperationCode::LogicalOr => todo!(),
            OperationCode::LogicalXor => todo!(),
            OperationCode::LogicalShiftLeft => todo!(),
            OperationCode::LogicalShiftRight => todo!(),
            OperationCode::ArithmeticAdd => todo!(),
            OperationCode::ArithmeticAddImmediate => todo!(),
            OperationCode::ArithmeticSubtract => todo!(),
            OperationCode::ArithmeticSubtractImmediate => todo!(),
            OperationCode::ArithmeticMultiply => todo!(),
            OperationCode::ArithmeticMultiplyImmediate => todo!(),
            OperationCode::ArithmeticDivide => todo!(),
            OperationCode::ArithmeticDivideImmediate => todo!(),
            OperationCode::ArithmeticRemainder => todo!(),
            OperationCode::ArithmeticRemainderImmediate => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {}
