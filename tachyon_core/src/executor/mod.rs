use crate::{
    common::{Timestamp, Value},
    storage::{
        file::{Cursor, ScanHint},
        page_cache::PageCache,
    },
};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    path::PathBuf,
    rc::Rc,
};

pub mod node;

#[non_exhaustive]
#[repr(u8)]
pub enum OperationCode {
    NoOperation,

    Init,
    Halt,

    OpenRead,
    CloseRead,

    Next,
    FetchVector,

    Goto,
    GotoEq,
    GotoNeq,

    OutputScalar,
    OutputVector,

    LogicalNot,
    LogicalAnd,
    LogicalOr,
    LogicalXor,
    LogicalShiftLeft,
    LogicalShiftRight,

    ArithmeticAdd,
    ArithmeticAddImmediate,
    ArithmeticSubtract,
    ArithmeticSubtractImmediate,
    ArithmeticMultiply,
    ArithmeticMultiplyImmediate,
    ArithmeticDivide,
    ArithmeticDivideImmediate,
    ArithmeticRemainder,
    ArithmeticRemainderImmediate,
}

const NUM_EXPECTED_INITIAL_CURSORS: usize = 10;
const NUM_REGS: usize = 10;

pub enum OutputValue {
    Scalar(Value),
    Vector((Timestamp, Value)),
}

pub struct Context {
    pc: usize,
    regs: [u64; NUM_REGS],

    file_paths_array: Rc<[Vec<PathBuf>]>,
    cursors: Vec<Option<Cursor>>,
    page_cache: Rc<RefCell<PageCache>>,

    outputs: VecDeque<OutputValue>,
}

impl Context {
    pub fn new(file_paths_array: Rc<[Vec<PathBuf>]>, page_cache: Rc<RefCell<PageCache>>) -> Self {
        Self {
            pc: 0,
            regs: [0x00u64; NUM_REGS],
            file_paths_array,
            cursors: Vec::with_capacity(NUM_EXPECTED_INITIAL_CURSORS),
            outputs: VecDeque::new(),
            page_cache,
        }
    }

    fn open_read(
        &mut self,
        cursor_idx: u64,
        file_paths_array_idx: u64,
        start: Timestamp,
        end: Timestamp,
    ) {
        if cursor_idx as usize > self.cursors.len() {
            self.cursors
                .reserve(cursor_idx as usize - self.cursors.len());
            for i in self.cursors.len()..cursor_idx as usize {
                self.cursors.push(None);
            }
        }

        if self.cursors[cursor_idx as usize].is_some() {
            panic!("Cursor key already used!");
        }

        self.cursors.push(Some(
            Cursor::new(
                self.file_paths_array[file_paths_array_idx as usize].clone(),
                start,
                end,
                self.page_cache.clone(),
                ScanHint::None,
            )
            .unwrap(),
        ));
    }

    fn close_read(&mut self, cursor_idx: u64) {
        self.cursors[cursor_idx as usize] = None;
    }

    fn next(&mut self, cursor_idx: u64) -> bool {
        self.cursors[cursor_idx as usize]
            .as_mut()
            .unwrap()
            .next()
            .is_some()
    }

    fn fetch_vector(&mut self, cursor_idx: u64) -> (Timestamp, Value) {
        self.cursors[cursor_idx as usize].as_ref().unwrap().fetch()
    }

    pub fn get_output(&mut self) -> Option<OutputValue> {
        self.outputs.pop_front()
    }
}

fn read_u64_pc(context: &mut Context, buffer: &[u8]) -> u64 {
    let ret = u64::from_le_bytes(buffer[context.pc..(context.pc + 8)].try_into().unwrap());
    context.pc += 8;
    ret
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
                let cursor_idx = read_u64_pc(&mut context, buffer);
                let file_paths_array_idx = read_u64_pc(&mut context, buffer);
                let start = read_u64_pc(&mut context, buffer);
                let end = read_u64_pc(&mut context, buffer);

                context.open_read(cursor_idx, file_paths_array_idx, start, end);
            }
            OperationCode::CloseRead => {
                let cursor_idx = read_u64_pc(&mut context, buffer);
                context.close_read(cursor_idx);
            }

            OperationCode::Next => {
                let cursor_idx = read_u64_pc(&mut context, buffer);
                let goto_success = read_u64_pc(&mut context, buffer);

                if context.next(cursor_idx) {
                    context.pc = goto_success as usize;
                }
            }
            OperationCode::FetchVector => {
                let cursor_idx = read_u64_pc(&mut context, buffer);
                let to_register_timestamp = read_u64_pc(&mut context, buffer);
                let to_register_value = read_u64_pc(&mut context, buffer);

                let (timestamp, value) = context.fetch_vector(cursor_idx);
                context.regs[to_register_timestamp as usize] = timestamp;
                context.regs[to_register_value as usize] = value;
            }

            OperationCode::Goto => {
                let address = read_u64_pc(&mut context, buffer);
                context.pc = address as usize;
            }
            OperationCode::GotoEq => {
                let address = read_u64_pc(&mut context, buffer);
                let register1 = read_u64_pc(&mut context, buffer);
                let register2 = read_u64_pc(&mut context, buffer);

                let value1 = context.regs[register1 as usize];
                let value2 = context.regs[register2 as usize];
                if value1 == value2 {
                    context.pc = address as usize;
                }
            }
            OperationCode::GotoNeq => {
                let address = read_u64_pc(&mut context, buffer);
                let register1 = read_u64_pc(&mut context, buffer);
                let register2 = read_u64_pc(&mut context, buffer);

                let value1 = context.regs[register1 as usize];
                let value2 = context.regs[register2 as usize];
                if value1 != value2 {
                    context.pc = address as usize;
                }
            }

            OperationCode::OutputScalar => {
                let from_register = read_u64_pc(&mut context, buffer);
                context
                    .outputs
                    .push_back(OutputValue::Scalar(context.regs[from_register as usize]));
            }
            OperationCode::OutputVector => {
                let from_register_timestamp = read_u64_pc(&mut context, buffer);
                let from_register_value = read_u64_pc(&mut context, buffer);

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
