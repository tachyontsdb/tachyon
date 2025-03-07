use crate::{Connection, ReturnType, Value, ValueType, Vector};

use super::{BinaryOp, ExecutorNode, TNode};

pub struct VectorToScalarNode {
    op: BinaryOp,
    vector_node: Box<TNode>,
    scalar_node: Box<TNode>,
    scalar: Option<Value>,
}

impl VectorToScalarNode {
    pub fn new(op: BinaryOp, vector_node: Box<TNode>, scalar_node: Box<TNode>) -> Self {
        Self {
            op,
            vector_node,
            scalar_node,
            scalar: None,
        }
    }
}

impl ExecutorNode for VectorToScalarNode {
    fn value_type(&self) -> ValueType {
        ValueType::get_applied_value_type(
            self.vector_node.value_type(),
            self.scalar_node.value_type(),
        )
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Vector
    }

    fn next_vector(&mut self, conn: &mut Connection) -> Option<Vector> {
        let scalar = match self.scalar {
            Some(s) => s,
            None => {
                self.scalar = self.scalar_node.next_scalar(conn);
                self.scalar.unwrap()
            }
        };

        match self.op {
            BinaryOp::Arithmetic(_) => {
                let vector_opt = self.vector_node.next_vector(conn);

                if let Some(Vector { timestamp, value }) = vector_opt {
                    Some(Vector {
                        timestamp,
                        value: self.op.apply(
                            value,
                            self.vector_node.value_type(),
                            scalar,
                            self.scalar_node.value_type(),
                        ),
                    })
                } else {
                    None
                }
            }
            BinaryOp::Comparison(_) => loop {
                let vector_opt = self.vector_node.next_vector(conn);

                if let Some(Vector { timestamp, value }) = vector_opt {
                    if self.op.compare(
                        value,
                        self.vector_node.value_type(),
                        scalar,
                        self.scalar_node.value_type(),
                    ) {
                        break Some(Vector { timestamp, value });
                    }
                } else {
                    break None;
                }
            },
        }
    }
}
