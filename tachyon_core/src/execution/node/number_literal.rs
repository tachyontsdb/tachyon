use crate::{Connection, ReturnType, Value, ValueType};

use super::ExecutorNode;

pub struct NumberLiteralNode {
    val_type: ValueType,
    val: Option<Value>,
}

impl NumberLiteralNode {
    pub fn new(val_type: ValueType, val: Value) -> Self {
        Self {
            val_type,
            val: Some(val),
        }
    }
}

impl ExecutorNode for NumberLiteralNode {
    fn value_type(&self) -> ValueType {
        self.val_type
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, _: &mut Connection) -> Option<Value> {
        self.val.take()
    }
}
