use crate::{Connection, ReturnType, Value, ValueType, Vector};

use super::{ExecutorNode, TNode};

#[derive(PartialEq)]
pub enum AggregateType {
    Sum,
    Count,
    Min,
    Max,
}

pub struct AggregateNode {
    pub aggregate_type: AggregateType,
    child: Box<TNode>,
}

impl AggregateNode {
    pub fn new(aggregate_type: AggregateType, child: Box<TNode>) -> Self {
        Self {
            aggregate_type,
            child,
        }
    }
}

impl ExecutorNode for AggregateNode {
    fn value_type(&self) -> ValueType {
        let child_value_type = self.child.value_type();

        match self.aggregate_type {
            AggregateType::Count => match *self.child {
                TNode::VectorSelect(_) => child_value_type,
                _ => ValueType::UInteger64,
            },
            _ => child_value_type,
        }
    }

    fn return_type(&self) -> ReturnType {
        ReturnType::Scalar
    }

    fn next_scalar(&mut self, conn: &mut Connection) -> Option<Value> {
        match self.aggregate_type {
            AggregateType::Sum => {
                let mut sum = self.child.next_vector(conn)?.value;
                let value_type = self.value_type();

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    sum = sum.add_same(value_type, &value);
                }

                Some(sum)
            }
            AggregateType::Count => {
                let first_vector = self.child.next_vector(conn)?;

                if let TNode::VectorSelect(_) = *self.child {
                    let mut count = first_vector.value;
                    let value_type = self.value_type();
                    while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                        count = count.add_same(value_type, &value);
                    }
                    Some(count)
                } else {
                    let mut count = 1u64;
                    while self.child.next_vector(conn).is_some() {
                        count += 1;
                    }
                    Some(count.into())
                }
            }
            AggregateType::Min | AggregateType::Max => {
                let value_type = self.value_type();
                let mut val = self.child.next_vector(conn)?.value;

                while let Some(Vector { value, .. }) = self.child.next_vector(conn) {
                    if self.aggregate_type == AggregateType::Min {
                        val = val.min_same(value_type, &value);
                    } else if self.aggregate_type == AggregateType::Max {
                        val = val.max_same(value_type, &value);
                    }
                }

                Some(val)
            }
        }
    }
}
