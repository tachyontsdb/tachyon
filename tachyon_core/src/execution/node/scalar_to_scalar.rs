use crate::{Connection, ReturnType, Value, ValueType};

use super::{BinaryOp, BinaryOpType, ExecutorNode, TNode};

pub struct ScalarToScalarNode {
    op: BinaryOp,
    lhs: Box<TNode>,
    rhs: Box<TNode>,
}

impl ScalarToScalarNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        Self { op, lhs, rhs }
    }
}

impl ExecutorNode for ScalarToScalarNode {
    fn value_type(&self) -> ValueType {
        let lhs_value_type = self.lhs.value_type();

        if lhs_value_type != self.rhs.value_type() {
            todo!("Implement operations between different types!");
        }

        lhs_value_type
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self.op.op_type() {
            BinaryOpType::Arithmetic => {
                let lhs_opt = self.lhs.next_scalar(conn);
                let rhs_opt = self.rhs.next_scalar(conn);

                match (lhs_opt, rhs_opt) {
                    (Some(lhs_value), Some(rhs_value)) => Some(self.op.apply(
                        lhs_value,
                        self.lhs.value_type(),
                        rhs_value,
                        self.rhs.value_type(),
                    )),
                    _ => None,
                }
            }
            BinaryOpType::Comparison => {
                panic!("Comparison operator not allowed between scalar and scalar!")
            }
        }
    }
}
