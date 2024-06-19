use crate::{
    common::{Timestamp, Value},
    storage::{file::Cursor, page_cache::PageCache},
};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    mem::size_of,
    path::PathBuf,
    rc::Rc,
};

#[non_exhaustive]
#[repr(u8)]
enum OperationCode {
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

const NUM_REGS: usize = 10;
const NUM_EXPECTED_INITIAL_CURSORS: usize = 10;

#[derive(Debug)]
pub struct Buffer(Vec<u8>);

impl Buffer {
    pub fn new() -> Self {
        Self(vec![OperationCode::Init as u8])
    }

    pub fn len(&self) -> u64 {
        self.0.len() as u64
    }

    pub fn add_halt(&mut self) {
        self.0.push(OperationCode::Halt as u8);
    }

    pub fn add_open_read(
        &mut self,
        cursor_idx: &[u8; size_of::<u16>()],
        file_paths_array_idx: &[u8; size_of::<u32>()],
        start: &[u8; size_of::<Timestamp>()],
        end: &[u8; size_of::<Timestamp>()],
    ) {
        self.0.push(OperationCode::OpenRead as u8);
        self.0.extend_from_slice(cursor_idx);
        self.0.extend_from_slice(file_paths_array_idx);
        self.0.extend_from_slice(start);
        self.0.extend_from_slice(end);
    }

    pub fn add_close_read(&mut self, cursor_idx: &[u8; size_of::<u16>()]) {
        self.0.push(OperationCode::CloseRead as u8);
        self.0.extend_from_slice(cursor_idx);
    }

    pub fn add_fetch_vector(
        &mut self,
        cursor_idx: &[u8; size_of::<u16>()],
        to_register_timestamp: u8,
        to_register_value: u8,
    ) {
        self.0.push(OperationCode::FetchVector as u8);
        self.0.extend_from_slice(cursor_idx);
        self.0.push(to_register_timestamp);
        self.0.push(to_register_value);
    }

    pub fn add_output_vector(&mut self, from_register_timestamp: u8, from_register_value: u8) {
        self.0.push(OperationCode::OutputVector as u8);
        self.0.push(from_register_timestamp);
        self.0.push(from_register_value);
    }

    pub fn add_next(
        &mut self,
        cursor_idx: &[u8; size_of::<u16>()],
        goto_success: &[u8; size_of::<u64>()],
    ) {
        self.0.push(OperationCode::Next as u8);
        self.0.extend_from_slice(cursor_idx);
        self.0.extend_from_slice(goto_success);
    }
}

pub enum OutputValue {
    Scalar(Value),
    Vector((Timestamp, Value)),
    Halted,
}

pub struct VirtualMachine {
    pc: usize,
    regs: [u64; NUM_REGS],

    file_paths_array: Rc<[Rc<[PathBuf]>]>,
    page_cache: Rc<RefCell<PageCache>>,
    buffer: Rc<Buffer>,

    cursors: Vec<Option<Cursor>>,
}

impl VirtualMachine {
    pub fn new(
        file_paths_array: Rc<[Rc<[PathBuf]>]>,
        page_cache: Rc<RefCell<PageCache>>,
        buffer: Rc<Buffer>,
    ) -> Self {
        Self {
            pc: 0,
            regs: [0x00u64; NUM_REGS],
            file_paths_array,
            page_cache,
            buffer,
            cursors: Vec::with_capacity(NUM_EXPECTED_INITIAL_CURSORS),
        }
    }

    fn open_read(
        &mut self,
        cursor_idx: u16,
        file_paths_array_idx: u32,
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

        if self.cursors.len() > cursor_idx as usize && self.cursors[cursor_idx as usize].is_some() {
            panic!("Cursor key already used!");
        }

        self.cursors.push(Some(
            Cursor::new(
                self.file_paths_array[file_paths_array_idx as usize].clone(),
                start,
                end,
                self.page_cache.clone(),
            )
            .unwrap(),
        ));
    }

    fn close_read(&mut self, cursor_idx: u16) {
        self.cursors[cursor_idx as usize] = None;
    }

    fn next(&mut self, cursor_idx: u16) -> bool {
        self.cursors[cursor_idx as usize]
            .as_mut()
            .unwrap()
            .next()
            .is_some()
    }

    fn fetch_vector(&mut self, cursor_idx: u16) -> (Timestamp, Value) {
        self.cursors[cursor_idx as usize].as_ref().unwrap().fetch()
    }

    fn read_u8_pc(&mut self) -> u8 {
        let ret = self.buffer.0[self.pc];
        self.pc += 1;
        ret
    }

    fn read_u16_pc(&mut self) -> u16 {
        let ret = ((self.buffer.0[self.pc + 1] as u16) << 8) | (self.buffer.0[self.pc] as u16);
        self.pc += 2;
        ret
    }

    fn read_u32_pc(&mut self) -> u32 {
        let ret = ((self.buffer.0[self.pc + 3] as u32) << 24)
            | ((self.buffer.0[self.pc + 2] as u32) << 16)
            | ((self.buffer.0[self.pc + 1] as u32) << 8)
            | (self.buffer.0[self.pc] as u32);
        self.pc += 4;
        ret
    }

    fn read_u64_pc(&mut self) -> u64 {
        let ret = u64::from_le_bytes(self.buffer.0[self.pc..(self.pc + 8)].try_into().unwrap());
        self.pc += 8;
        ret
    }

    pub fn execute_step(&mut self) -> OutputValue {
        while self.pc < self.buffer.0.len() {
            let opcode: OperationCode = unsafe { std::mem::transmute(self.buffer.0[self.pc]) };
            self.pc += 1;

            match opcode {
                OperationCode::NoOperation => {}

                OperationCode::Init => {}
                OperationCode::Halt => {
                    break;
                }

                OperationCode::OpenRead => {
                    let cursor_idx = self.read_u16_pc();
                    let file_paths_array_idx = self.read_u32_pc();
                    let start = self.read_u64_pc();
                    let end = self.read_u64_pc();

                    self.open_read(cursor_idx, file_paths_array_idx, start, end);
                }
                OperationCode::CloseRead => {
                    let cursor_idx = self.read_u16_pc();
                    self.close_read(cursor_idx);
                }

                OperationCode::Next => {
                    let cursor_idx = self.read_u16_pc();
                    let goto_success = self.read_u64_pc();

                    if self.next(cursor_idx) {
                        self.pc = goto_success as usize;
                    }
                }
                OperationCode::FetchVector => {
                    let cursor_idx = self.read_u16_pc();
                    let to_register_timestamp = self.read_u8_pc();
                    let to_register_value = self.read_u8_pc();

                    let (timestamp, value) = self.fetch_vector(cursor_idx);
                    self.regs[to_register_timestamp as usize] = timestamp;
                    self.regs[to_register_value as usize] = value;
                }

                OperationCode::Goto => {
                    let address = self.read_u64_pc();
                    self.pc = address as usize;
                }
                OperationCode::GotoEq => {
                    let address = self.read_u64_pc();
                    let register1 = self.read_u8_pc();
                    let register2 = self.read_u8_pc();

                    let value1 = self.regs[register1 as usize];
                    let value2 = self.regs[register2 as usize];
                    if value1 == value2 {
                        self.pc = address as usize;
                    }
                }
                OperationCode::GotoNeq => {
                    let address = self.read_u64_pc();
                    let register1 = self.read_u8_pc();
                    let register2 = self.read_u8_pc();

                    let value1 = self.regs[register1 as usize];
                    let value2 = self.regs[register2 as usize];
                    if value1 != value2 {
                        self.pc = address as usize;
                    }
                }

                OperationCode::OutputScalar => {
                    let from_register = self.read_u8_pc();
                    return OutputValue::Scalar(self.regs[from_register as usize]);
                }
                OperationCode::OutputVector => {
                    let from_register_timestamp = self.read_u8_pc();
                    let from_register_value = self.read_u8_pc();

                    return OutputValue::Vector((
                        self.regs[from_register_timestamp as usize],
                        self.regs[from_register_value as usize],
                    ));
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

        OutputValue::Halted
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
