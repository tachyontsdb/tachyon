use std::cmp::Ordering;

use crate::{Connection, ReturnType, Value, ValueType, Vector};

use super::{ExecutorNode, ScalarToScalarNode, TNode, VectorToScalarNode, VectorToVectorNode};

#[derive(Debug)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
}

#[derive(Debug)]
pub enum BinaryOpType {
    Arithmetic,
    Comparison,
}

impl BinaryOp {
    pub fn op_type(&self) -> BinaryOpType {
        match self {
            BinaryOp::Add
            | BinaryOp::Subtract
            | BinaryOp::Multiply
            | BinaryOp::Divide
            | BinaryOp::Modulo => BinaryOpType::Arithmetic,
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::Greater
            | BinaryOp::Less
            | BinaryOp::GreaterEqual
            | BinaryOp::LessEqual => BinaryOpType::Comparison,
        }
    }

    pub fn apply(
        &self,
        lhs: Value,
        lhs_value_type: ValueType,
        rhs: Value,
        rhs_value_type: ValueType,
    ) -> Value {
        match self {
            BinaryOp::Add => lhs.add(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Subtract => lhs.sub(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Multiply => lhs.mul(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Divide => lhs.div(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Modulo => lhs.mdl(lhs_value_type, &rhs, rhs_value_type),
            _ => panic!("apply not implemented for this binary operator!"),
        }
    }

    pub fn compare(
        &self,
        lhs: Value,
        lhs_value_type: ValueType,
        rhs: Value,
        rhs_value_type: ValueType,
    ) -> bool {
        match self {
            BinaryOp::Equal => lhs.eq(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::NotEqual => !lhs.eq(lhs_value_type, &rhs, rhs_value_type),
            BinaryOp::Greater => {
                lhs.partial_cmp(lhs_value_type, &rhs, rhs_value_type) == Some(Ordering::Greater)
            }
            BinaryOp::Less => {
                lhs.partial_cmp(lhs_value_type, &rhs, rhs_value_type) == Some(Ordering::Less)
            }
            BinaryOp::GreaterEqual => {
                let ordering = lhs.partial_cmp(lhs_value_type, &rhs, rhs_value_type);
                ordering == Some(Ordering::Greater) || ordering == Some(Ordering::Equal)
            }
            BinaryOp::LessEqual => {
                let ordering = lhs.partial_cmp(lhs_value_type, &rhs, rhs_value_type);
                ordering == Some(Ordering::Less) || ordering == Some(Ordering::Equal)
            }
            _ => panic!("compare not implemented for this binary operator!"),
        }
    }
}

pub struct BinaryOpNode {
    child: Box<TNode>,
}

impl BinaryOpNode {
    pub fn new(op: BinaryOp, lhs: Box<TNode>, rhs: Box<TNode>) -> Self {
        match (lhs.return_type(), rhs.return_type()) {
            (ReturnType::Scalar, ReturnType::Scalar) => Self {
                child: Box::new(TNode::ScalarToScalar(ScalarToScalarNode::new(op, lhs, rhs))),
            },
            (ReturnType::Vector, ReturnType::Scalar) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, lhs, rhs))),
            },
            (ReturnType::Scalar, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToScalar(VectorToScalarNode::new(op, rhs, lhs))),
            },
            (ReturnType::Vector, ReturnType::Vector) => Self {
                child: Box::new(TNode::VectorToVector(VectorToVectorNode::new(op, lhs, rhs))),
            },
        }
    }
}

impl ExecutorNode for BinaryOpNode {
    fn value_type(&self) -> ValueType {
        self.child.value_type()
    }

    fn return_type(&self) -> ReturnType {
        self.child.return_type()
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        self.child.next_vector(conn)
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        self.child.next_scalar(conn)
    }
}
